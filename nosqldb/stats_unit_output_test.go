//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func statsRequestNames(t *testing.T, payload map[string]interface{}) []string {
	t.Helper()
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)

	names := make([]string, 0, len(requests))
	for _, request := range requests {
		entry, ok := request.(map[string]interface{})
		require.True(t, ok)
		name, ok := entry["name"].(string)
		require.True(t, ok)
		names = append(names, name)
	}
	return names
}

func statsRequests(t *testing.T, payload map[string]interface{}) []interface{} {
	t.Helper()
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	return requests
}

func assertSizeSummary(t *testing.T, summary interface{}, min int, avg float64, max int) {
	t.Helper()
	values, ok := summary.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(min), values["min"])
	assert.Equal(t, avg, values["avg"])
	assert.Equal(t, float64(max), values["max"])
}

func assertLatencySummary(t *testing.T, summary interface{}, min int, avg float64, max int, hasPercentiles bool) {
	t.Helper()
	values, ok := summary.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(min), values["min"])
	assert.Equal(t, avg, values["avg"])
	assert.Equal(t, float64(max), values["max"])
	if hasPercentiles {
		assert.Equal(t, float64(max), values["95th"])
		assert.Equal(t, float64(max), values["99th"])
	} else {
		assert.NotContains(t, values, "95th")
		assert.NotContains(t, values, "99th")
	}
}

func assertRetrySummary(
	t *testing.T,
	summary interface{},
	delayMs int,
	authCount int,
	throttleCount int,
	count int,
) {
	t.Helper()
	values, ok := summary.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(delayMs), values["delayMs"])
	assert.Equal(t, float64(authCount), values["authCount"])
	assert.Equal(t, float64(throttleCount), values["throttleCount"])
	assert.Equal(t, float64(count), values["count"])
}

func statsProfileTestRequests() []statsRequestMetadata {
	return []statsRequestMetadata{
		{requestName: "Put"},
		{requestName: "Get"},
		{requestName: "Delete"},
		{requestName: "Table"},
		{requestName: "ListTables"},
		{requestName: "GetIndexes"},
		{
			requestName: "Query",
			query: &queryStatsMetadata{
				query:      "SELECT * FROM stats_table WHERE id = 1",
				unprepared: true,
				simple:     false,
				doesWrites: false,
				plan:       "query-plan",
			},
		},
	}
}

func statsProfileRequestNames() []string {
	return []string{"Put", "Get", "Delete", "Table", "ListTables", "GetIndexes", "Query"}
}

func statsProfilePayload(t *testing.T, profile StatsProfile) map[string]interface{} {
	t.Helper()
	return decodeStatsSnapshot(t, statsProfileSnapshot(t, profile))
}

func statsProfileSnapshot(t *testing.T, profile StatsProfile) *StatsSnapshot {
	t.Helper()
	control := statsProfileControl(t, profile)
	return control.snapshotAndReset()
}

func statsProfileControl(t *testing.T, profile StatsProfile) *StatsControl {
	t.Helper()
	control := newStatsControl(Config{
		StatsProfile:     profile,
		StatsPrettyPrint: true,
	})
	for index, request := range statsProfileTestRequests() {
		control.observe(newStatsSuccess(
			request,
			100+index,
			200+index,
			time.Duration(index+1)*time.Millisecond,
			index,
			time.Duration(index)*time.Millisecond,
			0,
			0,
			0,
		))
		if request.query != nil {
			control.observeQuery(*request.query)
		}
	}
	return control
}

func statsAllQuery(t *testing.T) map[string]interface{} {
	t.Helper()
	payload := statsProfilePayload(t, StatsProfileAll)
	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok)
	require.Len(t, queries, 1)
	query, ok := queries[0].(map[string]interface{})
	require.True(t, ok)
	return query
}

func TestStatsUnitProfileNoneDisablesSnapshots(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:     StatsProfileNone,
		StatsPrettyPrint: true,
	})
	for _, request := range statsProfileTestRequests() {
		control.observe(newStatsSuccess(request, 10, 20, time.Millisecond, 0, 0, 0, 0, 0))
	}

	assert.Nil(t, control.snapshotAndReset())
}

func TestStatsUnitRegularIncludesExpectedRequestNames(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileRegular)
	assert.Equal(t, statsProfileRequestNames(), statsRequestNames(t, payload))
}

func TestStatsUnitMoreIncludesExpectedRequestNames(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	assert.Equal(t, statsProfileRequestNames(), statsRequestNames(t, payload))
}

func TestStatsUnitAllIncludesExpectedRequestNames(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileAll)
	assert.Equal(t, statsProfileRequestNames(), statsRequestNames(t, payload))
}

func TestStatsUnitSnapshotIncludesIdentityAndTimeFields(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileRegular)
	assert.NotEmpty(t, payload["clientId"])
	assert.NotEmpty(t, payload["startTime"])
	assert.NotEmpty(t, payload["endTime"])
}

func TestStatsUnitSnapshotHasExpectedRequestEntryCount(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileRegular)
	require.Len(t, statsRequests(t, payload), len(statsProfileRequestNames()))
}

func TestStatsUnitRegularOmitsLatencyPercentiles(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileRegular)
	for index, name := range statsProfileRequestNames() {
		request := findStatsRequest(t, payload, name)
		assertLatencySummary(t, request["httpRequestLatencyMs"], index+1, float64(index+1), index+1, false)
	}
}

func TestStatsUnitMoreIncludesLatencyPercentiles(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for index, name := range statsProfileRequestNames() {
		request := findStatsRequest(t, payload, name)
		assertLatencySummary(t, request["httpRequestLatencyMs"], index+1, float64(index+1), index+1, true)
	}
}

func TestStatsUnitAllIncludesLatencyPercentiles(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileAll)
	for index, name := range statsProfileRequestNames() {
		request := findStatsRequest(t, payload, name)
		assertLatencySummary(t, request["httpRequestLatencyMs"], index+1, float64(index+1), index+1, true)
	}
}

func TestStatsUnitRequestCountsAreRecorded(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for _, name := range statsProfileRequestNames() {
		assert.Equal(t, float64(1), findStatsRequest(t, payload, name)["httpRequestCount"])
	}
}

func TestStatsUnitRequestErrorsAreZeroForSuccesses(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for _, name := range statsProfileRequestNames() {
		assert.Equal(t, float64(0), findStatsRequest(t, payload, name)["errors"])
	}
}

func TestStatsUnitRateLimitDelayIsRecorded(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for _, name := range statsProfileRequestNames() {
		assert.Equal(t, float64(0), findStatsRequest(t, payload, name)["rateLimitDelayMs"])
	}
}

func TestStatsUnitRequestSizeSummariesAreRecorded(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for index, name := range statsProfileRequestNames() {
		request := findStatsRequest(t, payload, name)
		assertSizeSummary(t, request["requestSize"], 100+index, float64(100+index), 100+index)
	}
}

func TestStatsUnitResultSizeSummariesAreRecorded(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for index, name := range statsProfileRequestNames() {
		request := findStatsRequest(t, payload, name)
		assertSizeSummary(t, request["resultSize"], 200+index, float64(200+index), 200+index)
	}
}

func TestStatsUnitRetrySummariesAreRecorded(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	for index, name := range statsProfileRequestNames() {
		request := findStatsRequest(t, payload, name)
		assertRetrySummary(t, request["retry"], index, 0, 0, index)
	}
}

func TestStatsUnitDoesNotEmitSyntheticConnections(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	assert.NotContains(t, payload, "connections")
}

func TestStatsUnitRegularOmitsQueryStats(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileRegular)
	assert.NotContains(t, payload, "queries")
}

func TestStatsUnitMoreOmitsQueryStats(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileMore)
	assert.NotContains(t, payload, "queries")
}

func TestStatsUnitAllIncludesQueryStats(t *testing.T) {
	payload := statsProfilePayload(t, StatsProfileAll)
	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok)
	require.Len(t, queries, 1)
}

func TestStatsUnitAllQueryTextIsRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assert.Equal(t, "SELECT * FROM stats_table WHERE id = 1", query["query"])
}

func TestStatsUnitAllQueryFlagsAreRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assert.Equal(t, false, query["doesWrites"])
	assert.Equal(t, false, query["simple"])
	assert.Equal(t, float64(1), query["unprepared"])
}

func TestStatsUnitAllQueryPlanIsRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assert.Equal(t, "query-plan", query["plan"])
}

func TestStatsUnitAllQueryCountsAreRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assert.Equal(t, float64(1), query["httpRequestCount"])
	assert.Equal(t, float64(1), query["count"])
}

func TestStatsUnitAllQueryErrorsAndRateLimitAreRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assert.Equal(t, float64(0), query["errors"])
	assert.Equal(t, float64(0), query["rateLimitDelayMs"])
}

func TestStatsUnitAllQuerySizesAreRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assertSizeSummary(t, query["requestSize"], 106, 106, 106)
	assertSizeSummary(t, query["resultSize"], 206, 206, 206)
}

func TestStatsUnitAllQueryLatencyIsRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assertLatencySummary(t, query["httpRequestLatencyMs"], 7, 7, 7, true)
}

func TestStatsUnitAllQueryRetryIsRecorded(t *testing.T) {
	query := statsAllQuery(t)
	assertRetrySummary(t, query["retry"], 6, 0, 0, 6)
}

func TestStatsUnitSnapshotResetClearsRequests(t *testing.T) {
	control := statsProfileControl(t, StatsProfileMore)
	_ = control.snapshotAndReset()
	resetPayload := decodeStatsSnapshot(t, control.snapshotAndReset())
	assert.Empty(t, statsRequests(t, resetPayload))
}

func TestStatsUnitSnapshotResetClearsConnections(t *testing.T) {
	control := statsProfileControl(t, StatsProfileMore)
	_ = control.snapshotAndReset()
	resetPayload := decodeStatsSnapshot(t, control.snapshotAndReset())
	assert.NotContains(t, resetPayload, "connections")
}

func TestStatsUnitSnapshotResetClearsQueries(t *testing.T) {
	control := statsProfileControl(t, StatsProfileAll)
	_ = control.snapshotAndReset()
	resetPayload := decodeStatsSnapshot(t, control.snapshotAndReset())
	assert.NotContains(t, resetPayload, "queries")
}
