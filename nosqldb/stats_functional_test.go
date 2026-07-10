//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

//go:build cloud || onprem
// +build cloud onprem

package nosqldb_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type StatsFunctionalTestSuite struct {
	*test.NoSQLTestSuite
}

var (
	statsPeriodicDurationSec = flag.Int("statsPeriodicDurationSec", 3, "Duration in seconds for the periodic stats workload.")
	statsPeriodicIntervalSec = flag.Int("statsPeriodicIntervalSec", 1, "Stats interval in seconds for the periodic stats workload.")
	statsPeriodicCyclesSec   = flag.Int("statsPeriodicCyclesSec", 4, "Put/Get/Delete cycles per second for the periodic stats workload.")
	statsPeriodicWorkers     = flag.Int("statsPeriodicWorkers", 4, "Concurrent workers for the periodic stats workload.")
	statsPeriodicProfile     = flag.String("statsPeriodicProfile", "MORE", "Stats profile for the periodic stats workload: REGULAR, MORE, or ALL.")
)

func boolPtr(value bool) *bool {
	return &value
}

func periodicIntArg(name string, defaultValue int) int {
	value := periodicStringArg(name, "")
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func periodicStringArg(name string, defaultValue string) string {
	for _, arg := range flag.Args() {
		arg = strings.TrimPrefix(arg, "-")
		key, value, ok := strings.Cut(arg, "=")
		if ok && key == name {
			return value
		}
	}
	return defaultValue
}

func (suite *StatsFunctionalTestSuite) TestStatsHandlerReceivesRealRequestStats() {
	t := suite.T()
	snapshots := make(chan *nosqldb.StatsSnapshot, 2)

	cfg := suite.Config.Config
	cfg.StatsProfile = nosqldb.StatsProfileAll
	cfg.StatsPrettyPrint = true
	cfg.StatsEnableLog = boolPtr(false)
	cfg.StatsHandler = nosqldb.StatsHandlerFunc(func(snapshot *nosqldb.StatsSnapshot) {
		snapshots <- snapshot
	})

	client, err := nosqldb.NewClient(cfg)
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			client.Close()
		}
	}()

	table := suite.GetTableName("StatsFunctional")
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"name STRING, "+
		"PRIMARY KEY(id))", table)
	createReq := &nosqldb.TableRequest{Statement: stmt}
	if suite.IsCloud() {
		createReq.TableLimits = &nosqldb.TableLimits{
			ReadUnits:  50,
			WriteUnits: 50,
			StorageGB:  1,
		}
	}
	_, err = client.DoTableRequestAndWait(createReq, 30*time.Second, time.Second)
	require.NoError(t, err)

	value := &types.MapValue{}
	value.Put("id", 1).Put("name", "stats-row")
	_, err = client.Put(&nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	})
	require.NoError(t, err)

	key := &types.MapValue{}
	key.Put("id", 1)
	_, err = client.Get(&nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	})
	require.NoError(t, err)

	queryReq := &nosqldb.QueryRequest{
		Statement: "SELECT * FROM " + table + " WHERE id = 1",
		TableName: table,
	}
	for {
		_, err = client.Query(queryReq)
		require.NoError(t, err)
		if queryReq.IsDone() {
			break
		}
	}

	_, err = client.Delete(&nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	})
	require.NoError(t, err)

	dropReq := &nosqldb.TableRequest{Statement: "DROP TABLE IF EXISTS " + table}
	_, err = client.DoTableRequestAndWait(dropReq, 30*time.Second, time.Second)
	require.NoError(t, err)
	require.NoError(t, client.Close())
	closed = true

	var snapshot *nosqldb.StatsSnapshot
	select {
	case snapshot = <-snapshots:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for functional stats snapshot")
	}

	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(snapshot.JSON()), &payload))
	requireStatsRequest(t, payload, "Put")
	requireStatsRequest(t, payload, "Get")
	requireStatsRequest(t, payload, "Query")
	requireStatsRequest(t, payload, "Delete")
	requireStatsRequest(t, payload, "Table")

	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok, "ALL profile should include query stats")
	require.NotEmpty(t, queries)
}

func (suite *StatsFunctionalTestSuite) TestPeriodicStatsHandlerReceivesRealRequestStats() {
	t := suite.T()
	var snapshotsMu sync.Mutex
	snapshots := make([]map[string]interface{}, 0, 8)
	handlerErrs := make([]error, 0)
	snapshotReady := make(chan struct{}, 1)

	durationSec := periodicIntArg("statsPeriodicDurationSec", *statsPeriodicDurationSec)
	intervalSec := periodicIntArg("statsPeriodicIntervalSec", *statsPeriodicIntervalSec)
	cyclesPerSec := periodicIntArg("statsPeriodicCyclesSec", *statsPeriodicCyclesSec)
	workerCount := periodicIntArg("statsPeriodicWorkers", *statsPeriodicWorkers)
	profileName := periodicStringArg("statsPeriodicProfile", *statsPeriodicProfile)

	profile, err := nosqldb.ParseStatsProfile(profileName)
	require.NoError(t, err)
	require.NotEqual(t, nosqldb.StatsProfileNone, profile, "periodic stats workload requires REGULAR, MORE, or ALL")
	require.Greater(t, durationSec, 0)
	require.Greater(t, intervalSec, 0)
	require.Greater(t, cyclesPerSec, 0)
	require.Greater(t, workerCount, 0)

	cfg := suite.Config.Config
	cfg.StatsProfile = nosqldb.StatsProfileNone
	cfg.StatsInterval = time.Duration(intervalSec) * time.Second
	cfg.StatsPrettyPrint = true
	cfg.StatsEnableLog = boolPtr(false)
	cfg.StatsHandler = nosqldb.StatsHandlerFunc(func(snapshot *nosqldb.StatsSnapshot) {
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(snapshot.JSON()), &payload); err != nil {
			snapshotsMu.Lock()
			handlerErrs = append(handlerErrs, err)
			snapshotsMu.Unlock()
			return
		}
		printPeriodicSnapshotSummary(t, payload)
		snapshotsMu.Lock()
		snapshots = append(snapshots, payload)
		snapshotsMu.Unlock()
		select {
		case snapshotReady <- struct{}{}:
		default:
		}
	})

	client, err := nosqldb.NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()

	table := suite.GetTableName("StatsPeriodic")
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"name STRING, "+
		"PRIMARY KEY(id))", table)
	createReq := &nosqldb.TableRequest{Statement: stmt}
	if suite.IsCloud() {
		createReq.TableLimits = &nosqldb.TableLimits{
			ReadUnits:  50,
			WriteUnits: 50,
			StorageGB:  1,
		}
	}
	_, err = client.DoTableRequestAndWait(createReq, 30*time.Second, time.Second)
	require.NoError(t, err)

	defer func() {
		dropReq := &nosqldb.TableRequest{Statement: "DROP TABLE IF EXISTS " + table}
		_, dropErr := client.DoTableRequestAndWait(dropReq, 30*time.Second, time.Second)
		require.NoError(t, dropErr)
	}()

	stats := client.GetStatsControl()
	require.NotNil(t, stats)
	require.NoError(t, stats.SetProfile(profile))
	stats.Start()

	t.Log("Starting deterministic concurrent periodic stats workload...")
	t.Logf("Stats profile: %s", profile)
	t.Logf("Stats interval: %ds", intervalSec)
	t.Logf("Workload duration: %ds", durationSec)
	t.Logf("Cycles per second: %d", cyclesPerSec)
	t.Logf("Concurrent workers: %d", workerCount)
	t.Logf("Target request rate: %d requests/sec", cyclesPerSec*3)
	t.Log("Each cycle: 1 Put + 1 Get + 1 Delete")

	expectedPerRequest := runPeriodicDeterministicWorkload(
		t,
		client,
		table,
		durationSec,
		cyclesPerSec,
		workerCount,
	)

	t.Log("Workload finished. Waiting for periodic stats to report all workload requests...")
	waitForPeriodicRequestCounts(
		t,
		snapshotReady,
		&snapshotsMu,
		&snapshots,
		expectedPerRequest,
		time.Duration(intervalSec*2+5)*time.Second,
	)
	stats.Stop()

	snapshotsMu.Lock()
	captured := append([]map[string]interface{}(nil), snapshots...)
	errs := append([]error(nil), handlerErrs...)
	snapshotsMu.Unlock()
	for _, err := range errs {
		require.NoError(t, err)
	}
	validatePeriodicSnapshots(
		t,
		captured,
		expectedPerRequest,
		durationSec,
		profile,
	)
}

func waitForPeriodicRequestCounts(
	t *testing.T,
	snapshotReady <-chan struct{},
	snapshotsMu *sync.Mutex,
	snapshots *[]map[string]interface{},
	expectedPerRequest int,
	timeout time.Duration,
) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		snapshotsMu.Lock()
		put, get, deleteCount := periodicRequestTotals(*snapshots)
		snapshotsMu.Unlock()
		if put == float64(expectedPerRequest) &&
			get == float64(expectedPerRequest) &&
			deleteCount == float64(expectedPerRequest) {
			return
		}

		select {
		case <-snapshotReady:
		case <-timer.C:
			t.Fatalf(
				"timed out waiting for periodic stats counts: expected=%d Put=%.0f Get=%.0f Delete=%.0f",
				expectedPerRequest,
				put,
				get,
				deleteCount,
			)
		}
	}
}

func periodicRequestTotals(snapshots []map[string]interface{}) (put, get, deleteCount float64) {
	for _, snapshot := range snapshots {
		put += requestCount(snapshot, "Put")
		get += requestCount(snapshot, "Get")
		deleteCount += requestCount(snapshot, "Delete")
	}
	return put, get, deleteCount
}

func requireStatsRequest(t *testing.T, payload map[string]interface{}, name string) {
	t.Helper()
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	for _, request := range requests {
		entry, ok := request.(map[string]interface{})
		require.True(t, ok)
		if entry["name"] == name {
			require.Greater(t, entry["httpRequestCount"].(float64), float64(0))
			return
		}
	}
	t.Fatalf("request %s not found in stats payload: %v", name, payload)
}

func runPeriodicDeterministicWorkload(
	t *testing.T,
	client *nosqldb.Client,
	table string,
	durationSec int,
	cyclesPerSec int,
	workerCount int,
) int {
	t.Helper()
	totalCycles := durationSec * cyclesPerSec
	baseID := int(time.Now().UnixMilli() % 1_000_000_000)
	started := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, workerCount)

	t.Logf("Target cycles: %d using %d workers", totalCycles, workerCount)
	for worker := 0; worker < workerCount; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cycle := worker; cycle < totalCycles; cycle += workerCount {
				scheduled := started.Add(time.Duration(cycle) * time.Second / time.Duration(cyclesPerSec))
				time.Sleep(time.Until(scheduled))
				id := baseID + cycle + 1
				if err := putGetDeleteCycle(client, table, id); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	t.Logf("Workload issued %d Put, %d Get, and %d Delete requests in %.2fs.",
		totalCycles, totalCycles, totalCycles, time.Since(started).Seconds())
	return totalCycles
}

func putGetDeleteCycle(client *nosqldb.Client, table string, id int) error {
	value := &types.MapValue{}
	value.Put("id", id).Put("name", fmt.Sprintf("periodic-stats-row-%d", id))
	if _, err := client.Put(&nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}); err != nil {
		return err
	}

	key := &types.MapValue{}
	key.Put("id", id)
	if _, err := client.Get(&nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	}); err != nil {
		return err
	}

	_, err := client.Delete(&nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	})
	return err
}

func printPeriodicSnapshotSummary(t *testing.T, payload map[string]interface{}) {
	t.Helper()
	startTime, _ := payload["startTime"].(string)
	endTime, _ := payload["endTime"].(string)
	t.Logf(
		"Stats interval %s -> %s | Put=%.0f %s | Get=%.0f %s | Delete=%.0f %s | requests=%d",
		startTime,
		endTime,
		requestCount(payload, "Put"),
		latencySummary(t, payload, "Put"),
		requestCount(payload, "Get"),
		latencySummary(t, payload, "Get"),
		requestCount(payload, "Delete"),
		latencySummary(t, payload, "Delete"),
		requestEntryCount(payload),
	)
}

func validatePeriodicSnapshots(
	t *testing.T,
	snapshots []map[string]interface{},
	expectedPerRequest int,
	durationSec int,
	profile nosqldb.StatsProfile,
) {
	t.Helper()
	totals := make(map[string]float64)
	latencyEntries := make(map[string]int)
	emptyIntervals := 0

	for _, snapshot := range snapshots {
		requests, ok := snapshot["requests"].([]interface{})
		require.True(t, ok)
		if len(requests) == 0 {
			emptyIntervals++
		}
		for _, request := range requests {
			entry, ok := request.(map[string]interface{})
			require.True(t, ok)
			name, _ := entry["name"].(string)
			count, _ := entry["httpRequestCount"].(float64)
			totals[name] += count
			if name == "Put" || name == "Get" || name == "Delete" {
				assertLatencyIsNumeric(t, entry, profile)
				latencyEntries[name]++
			}
		}
	}

	put := totals["Put"]
	get := totals["Get"]
	deleteCount := totals["Delete"]
	total := put + get + deleteCount
	expectedTotal := float64(expectedPerRequest * 3)

	t.Log("Periodic stats validation summary")
	t.Logf("Snapshots observed: %d", len(snapshots))
	t.Logf("Empty intervals observed: %d", emptyIntervals)
	t.Logf("Put count:    expected=%d actual=%.0f", expectedPerRequest, put)
	t.Logf("Get count:    expected=%d actual=%.0f", expectedPerRequest, get)
	t.Logf("Delete count: expected=%d actual=%.0f", expectedPerRequest, deleteCount)
	t.Logf("Latency entries checked: Put=%d Get=%d Delete=%d",
		latencyEntries["Put"], latencyEntries["Get"], latencyEntries["Delete"])
	t.Logf("Rough throughput: %.3f requests/sec", total/float64(durationSec))

	require.NotEmpty(t, snapshots)
	require.Equal(t, float64(expectedPerRequest), put)
	require.Equal(t, float64(expectedPerRequest), get)
	require.Equal(t, float64(expectedPerRequest), deleteCount)
	require.Equal(t, expectedTotal, total)
	require.Greater(t, latencyEntries["Put"], 0)
	require.Greater(t, latencyEntries["Get"], 0)
	require.Greater(t, latencyEntries["Delete"], 0)
}

func requestCount(payload map[string]interface{}, name string) float64 {
	requests, ok := payload["requests"].([]interface{})
	if !ok {
		return 0
	}
	for _, request := range requests {
		entry, ok := request.(map[string]interface{})
		if ok && entry["name"] == name {
			count, _ := entry["httpRequestCount"].(float64)
			return count
		}
	}
	return 0
}

func requestEntryCount(payload map[string]interface{}) int {
	requests, ok := payload["requests"].([]interface{})
	if !ok {
		return 0
	}
	return len(requests)
}

func latencySummary(t *testing.T, payload map[string]interface{}, name string) string {
	t.Helper()
	entry := requestEntry(payload, name)
	if entry == nil {
		return "latency=n/a"
	}
	latency, ok := entry["httpRequestLatencyMs"].(map[string]interface{})
	if !ok {
		return "latency=n/a"
	}
	summary := fmt.Sprintf("latency_avg=%.3fms", latency["avg"].(float64))
	if p95, ok := latency["95th"].(float64); ok {
		summary += fmt.Sprintf(" p95=%.0fms", p95)
	}
	if p99, ok := latency["99th"].(float64); ok {
		summary += fmt.Sprintf(" p99=%.0fms", p99)
	}
	return summary
}

func requestEntry(payload map[string]interface{}, name string) map[string]interface{} {
	requests, ok := payload["requests"].([]interface{})
	if !ok {
		return nil
	}
	for _, request := range requests {
		entry, ok := request.(map[string]interface{})
		if ok && entry["name"] == name {
			return entry
		}
	}
	return nil
}

func assertLatencyIsNumeric(t *testing.T, request map[string]interface{}, profile nosqldb.StatsProfile) {
	t.Helper()
	name, _ := request["name"].(string)
	latency, ok := request["httpRequestLatencyMs"].(map[string]interface{})
	require.True(t, ok, "%s is missing httpRequestLatencyMs", name)
	fields := []string{"min", "avg", "max"}
	if profile == nosqldb.StatsProfileMore || profile == nosqldb.StatsProfileAll {
		fields = append(fields, "95th", "99th")
	}
	for _, field := range fields {
		_, ok := latency[field].(float64)
		require.True(t, ok, "%s.httpRequestLatencyMs.%s is not numeric", name, field)
	}
}

func TestStatsFunctional(t *testing.T) {
	suite.Run(t, &StatsFunctionalTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	})
}
