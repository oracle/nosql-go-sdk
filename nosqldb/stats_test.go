//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type statsTestExecutor struct {
	body       string
	statusCode int
}

func (e statsTestExecutor) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: e.statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(e.body)),
	}, nil
}

type statsSequenceExecutor struct {
	steps []statsExecutorStep
}

type statsExecutorStep struct {
	body       string
	statusCode int
	err        error
}

func (e *statsSequenceExecutor) Do(req *http.Request) (*http.Response, error) {
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	default:
	}

	if len(e.steps) == 0 {
		return nil, io.EOF
	}

	step := e.steps[0]
	e.steps = e.steps[1:]
	if step.err != nil {
		return nil, step.err
	}

	return &http.Response{
		StatusCode: step.statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(step.body)),
	}, nil
}

type statsBlockingExecutor struct{}

func (statsBlockingExecutor) Do(req *http.Request) (*http.Response, error) {
	<-req.Context().Done()
	return nil, req.Context().Err()
}

type statsBlockingRetryHandler struct {
	entered chan struct{}
	once    sync.Once
}

func (*statsBlockingRetryHandler) MaxNumRetries() uint {
	return 1
}

func (*statsBlockingRetryHandler) ShouldRetry(Request, uint, error) bool {
	return true
}

func (*statsBlockingRetryHandler) Delay(Request, uint, error) {}

func (h *statsBlockingRetryHandler) DelayWithContext(ctx context.Context, _ Request, _ uint, _ error) error {
	h.once.Do(func() {
		close(h.entered)
	})
	<-ctx.Done()
	return ctx.Err()
}

type statsBlockingRateLimiter struct {
	common.RateLimiter
	entered chan struct{}
	once    sync.Once
}

func (l *statsBlockingRateLimiter) ConsumeUnitsWithContext(ctx context.Context, _ int64, _ time.Duration, _ bool) (time.Duration, error) {
	l.once.Do(func() {
		close(l.entered)
	})
	<-ctx.Done()
	return 0, ctx.Err()
}

type statsCloseCountingProvider struct {
	closeCalls int32
}

func (*statsCloseCountingProvider) AuthorizationScheme() string {
	return auth.BearerToken
}

func (*statsCloseCountingProvider) AuthorizationString(auth.Request) (string, error) {
	return "", nil
}

func (*statsCloseCountingProvider) SignHTTPRequest(*http.Request) error {
	return nil
}

func (p *statsCloseCountingProvider) Close() error {
	atomic.AddInt32(&p.closeCalls, 1)
	return nil
}

func (*statsCloseCountingProvider) GetLogger() *logger.Logger {
	return nil
}

type statsTrackingReadCloser struct {
	reader     io.Reader
	closeCount int32
}

func (r *statsTrackingReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *statsTrackingReadCloser) Close() error {
	atomic.AddInt32(&r.closeCount, 1)
	return nil
}

type statsFailingReader struct{}

func (statsFailingReader) Read(p []byte) (int, error) {
	return copy(p, "partial"), io.ErrUnexpectedEOF
}

type statsCountingPlan struct {
	planIter
	plan  string
	calls int32
}

func (p *statsCountingPlan) getPlan() string {
	atomic.AddInt32(&p.calls, 1)
	return p.plan
}

func decodeStatsSnapshot(t *testing.T, snapshot *StatsSnapshot) map[string]interface{} {
	t.Helper()
	require.NotNil(t, snapshot)
	var payload map[string]interface{}
	err := json.Unmarshal([]byte(snapshot.JSON()), &payload)
	require.NoError(t, err)
	return payload
}

func decodeStatsIntervalLogs(t *testing.T, output string) []map[string]interface{} {
	t.Helper()
	var intervals []map[string]interface{}
	for _, line := range strings.Split(output, "\n") {
		prefix := strings.Index(line, StatsLogPrefix)
		if prefix < 0 {
			continue
		}
		var payload map[string]interface{}
		err := json.Unmarshal([]byte(line[prefix+len(StatsLogPrefix):]), &payload)
		require.NoError(t, err)
		if _, ok := payload["startTime"]; ok {
			intervals = append(intervals, payload)
		}
	}
	return intervals
}

func findStatsRequest(t *testing.T, payload map[string]interface{}, name string) map[string]interface{} {
	t.Helper()
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	for _, request := range requests {
		entry, ok := request.(map[string]interface{})
		require.True(t, ok)
		if entry["name"] == name {
			return entry
		}
	}
	t.Fatalf("request %s not found in stats payload: %v", name, payload)
	return nil
}

func boolPtr(value bool) *bool {
	return &value
}

func emitStatsIntervalForTest(t *testing.T, control *StatsControl, snapshots <-chan *StatsSnapshot) *StatsSnapshot {
	t.Helper()
	snapshot := control.emitSnapshot()
	require.NotNil(t, snapshot)
	select {
	case handled := <-snapshots:
		require.Same(t, snapshot, handled)
		return handled
	default:
		t.Fatal("expected stats handler to receive emitted snapshot")
		return nil
	}
}

func statsRetryPayload(t *testing.T, request map[string]interface{}) map[string]interface{} {
	t.Helper()
	retry, ok := request["retry"].(map[string]interface{})
	require.True(t, ok)
	return retry
}

func TestStatsProfile(t *testing.T) {
	tests := []struct {
		input string
		want  StatsProfile
	}{
		{input: "", want: StatsProfileNone},
		{input: "none", want: StatsProfileNone},
		{input: "REGULAR", want: StatsProfileRegular},
		{input: " more ", want: StatsProfileMore},
		{input: "ALL", want: StatsProfileAll},
	}

	for _, test := range tests {
		got, err := ParseStatsProfile(test.input)
		require.NoError(t, err)
		assert.Equal(t, test.want, got)
		assert.Equal(t, string(test.want), got.String())
	}

	_, err := ParseStatsProfile("verbose")
	assert.Error(t, err)

	assert.False(t, StatsProfileRegular.includesPercentiles())
	assert.True(t, StatsProfileMore.includesPercentiles())
	assert.True(t, StatsProfileAll.includesPercentiles())
}

func TestStatsPercentileMode(t *testing.T) {
	tests := []struct {
		input string
		want  StatsPercentileMode
	}{
		{input: "", want: StatsPercentileExact},
		{input: "exact", want: StatsPercentileExact},
		{input: "SAMPLES", want: StatsPercentileExact},
		{input: "hdr", want: StatsPercentileHDR},
		{input: " histogram ", want: StatsPercentileHDR},
	}

	for _, test := range tests {
		got, err := ParseStatsPercentileMode(test.input)
		require.NoError(t, err)
		assert.Equal(t, test.want, got)
		assert.Equal(t, string(test.want), got.String())
	}

	_, err := ParseStatsPercentileMode("approx")
	assert.Error(t, err)
}

func TestStatsSnapshotAndHandler(t *testing.T) {
	snapshot := newStatsSnapshot(`{"requests":[]}`)
	assert.Equal(t, `{"requests":[]}`, snapshot.JSON())
	assert.Equal(t, `{"requests":[]}`, snapshot.String())

	var got string
	handler := StatsHandlerFunc(func(stats *StatsSnapshot) {
		got = stats.JSON()
	})
	handler.HandleStats(snapshot)
	assert.Equal(t, snapshot.JSON(), got)

	var nilHandler StatsHandlerFunc
	nilHandler.HandleStats(snapshot)
}

func TestStatsRequestNameSeparatesTableUsageAndReplicaStats(t *testing.T) {
	assert.Equal(t, "TableUsage", statsRequestName(&TableUsageRequest{}))
	assert.Equal(t, "ReplicaStats", statsRequestName(&ReplicaStatsRequest{}))
}

func TestStatsOutputSeparatesTableUsageAndReplicaStats(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile: StatsProfileRegular,
	})
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: statsRequestName(&TableUsageRequest{})},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: statsRequestName(&ReplicaStatsRequest{})},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	tableUsage := findStatsRequest(t, payload, "TableUsage")
	replicaStats := findStatsRequest(t, payload, "ReplicaStats")
	assert.Equal(t, float64(1), tableUsage["httpRequestCount"])
	assert.Equal(t, float64(1), replicaStats["httpRequestCount"])
}

func TestStatsConfigDefaults(t *testing.T) {
	var cfg *Config
	assert.Equal(t, StatsProfileNone, cfg.DefaultStatsProfile())
	assert.Equal(t, 600*time.Second, cfg.DefaultStatsInterval())
	assert.False(t, cfg.DefaultStatsPrettyPrint())
	assert.True(t, cfg.DefaultStatsEnableLog())
	assert.Equal(t, StatsPercentileExact, cfg.DefaultStatsPercentileMode())

	enableLog := false
	cfg = &Config{
		StatsProfile:        "more",
		StatsInterval:       5 * time.Second,
		StatsPrettyPrint:    true,
		StatsEnableLog:      &enableLog,
		StatsPercentileMode: "hdr",
	}

	assert.Equal(t, StatsProfileMore, cfg.DefaultStatsProfile())
	assert.Equal(t, 5*time.Second, cfg.DefaultStatsInterval())
	assert.True(t, cfg.DefaultStatsPrettyPrint())
	assert.False(t, cfg.DefaultStatsEnableLog())
	assert.Equal(t, StatsPercentileHDR, cfg.DefaultStatsPercentileMode())
}

func TestStatsConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{
			name: "defaults are valid",
			cfg:  Config{},
			ok:   true,
		},
		{
			name: "lowercase values are valid",
			cfg: Config{
				StatsProfile:        "all",
				StatsPercentileMode: "hdr",
			},
			ok: true,
		},
		{
			name: "custom interval is valid",
			cfg: Config{
				StatsInterval: time.Second,
			},
			ok: true,
		},
		{
			name: "interval below one second is invalid",
			cfg: Config{
				StatsInterval: time.Millisecond,
			},
			ok: false,
		},
		{
			name: "fractional seconds interval is invalid",
			cfg: Config{
				StatsInterval: 1500 * time.Millisecond,
			},
			ok: false,
		},
		{
			name: "invalid profile",
			cfg: Config{
				StatsProfile: "verbose",
			},
			ok: false,
		},
		{
			name: "invalid percentile mode",
			cfg: Config{
				StatsPercentileMode: "approx",
			},
			ok: false,
		},
	}

	for _, test := range tests {
		err := test.cfg.validateStatsConfig()
		if test.ok {
			assert.NoError(t, err, test.name)
		} else {
			assert.Error(t, err, test.name)
		}
	}
}

func TestStatsConfigSetDefaultsNormalizesStatsValues(t *testing.T) {
	enableLog := false
	cfg := Config{
		Mode:                "cloudsim",
		Endpoint:            "localhost:8080",
		StatsProfile:        "more",
		StatsInterval:       5 * time.Second,
		StatsPrettyPrint:    true,
		StatsEnableLog:      &enableLog,
		StatsPercentileMode: "hdr",
	}

	err := cfg.setDefaults()
	require.NoError(t, err)

	assert.Equal(t, StatsProfileMore, cfg.StatsProfile)
	assert.Equal(t, 5*time.Second, cfg.DefaultStatsInterval())
	assert.True(t, cfg.DefaultStatsPrettyPrint())
	assert.False(t, cfg.DefaultStatsEnableLog())
	assert.Equal(t, StatsPercentileHDR, cfg.StatsPercentileMode)
}

func TestStatsControlDefaults(t *testing.T) {
	control := newStatsControl(Config{})

	assert.Equal(t, StatsProfileNone, control.GetProfile())
	assert.Equal(t, 600*time.Second, control.GetInterval())
	assert.False(t, control.GetPrettyPrint())
	assert.Nil(t, control.GetStatsHandler())
	assert.False(t, control.IsStarted())
}

func TestStatsControlsHaveDistinctRandomClientIDs(t *testing.T) {
	first := newStatsControl(Config{})
	second := newStatsControl(Config{})

	require.NotEqual(t, first.inner.clientID, second.inner.clientID)
	for _, clientID := range []string{first.inner.clientID, second.inner.clientID} {
		decoded, err := hex.DecodeString(clientID)
		require.NoError(t, err)
		assert.Len(t, decoded, 16)
	}
}

func TestStatsControlConfigPropagation(t *testing.T) {
	var handled bool
	handler := StatsHandlerFunc(func(stats *StatsSnapshot) {
		handled = stats.JSON() == `{"requests":[]}`
	})
	enableLog := false
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileRegular,
		StatsInterval:       5 * time.Second,
		StatsPrettyPrint:    true,
		StatsEnableLog:      &enableLog,
		StatsPercentileMode: StatsPercentileHDR,
		StatsHandler:        handler,
	})

	assert.Equal(t, StatsProfileRegular, control.GetProfile())
	assert.Equal(t, 5*time.Second, control.GetInterval())
	assert.True(t, control.GetPrettyPrint())
	assert.True(t, control.IsStarted())
	assert.Equal(t, StatsPercentileHDR, control.inner.percentileMode)

	gotHandler := control.GetStatsHandler()
	require.NotNil(t, gotHandler)
	gotHandler.HandleStats(newStatsSnapshot(`{"requests":[]}`))
	assert.True(t, handled)
}

func TestStatsControlSetters(t *testing.T) {
	control := newStatsControl(Config{})

	err := control.SetProfile(StatsProfileAll)
	require.NoError(t, err)
	control.SetPrettyPrint(true)

	var handled bool
	control.SetStatsHandler(StatsHandlerFunc(func(stats *StatsSnapshot) {
		handled = stats.JSON() == "snapshot"
	}))

	assert.Equal(t, StatsProfileAll, control.GetProfile())
	assert.True(t, control.GetPrettyPrint())
	assert.False(t, control.IsStarted(), "SetProfile must not start collection")

	gotHandler := control.GetStatsHandler()
	require.NotNil(t, gotHandler)
	gotHandler.HandleStats(newStatsSnapshot("snapshot"))
	assert.True(t, handled)

	err = control.SetProfile("verbose")
	assert.Error(t, err)
	assert.Equal(t, StatsProfileAll, control.GetProfile())
}

func TestStatsControlSetProfileFlushesCollectedStats(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 1)
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})

	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Get"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	require.NoError(t, control.SetProfile(StatsProfileMore))

	select {
	case snapshot := <-snapshots:
		payload := decodeStatsSnapshot(t, snapshot)
		request := findStatsRequest(t, payload, "Get")
		assert.Equal(t, float64(1), request["httpRequestCount"])
		latency := request["httpRequestLatencyMs"].(map[string]interface{})
		assert.NotContains(t, latency, "95th")
		assert.NotContains(t, latency, "99th")
	default:
		t.Fatal("expected the old profile interval to be flushed")
	}

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, requests)
	assert.NotContains(t, payload, "connections")
}

func TestStatsControlSetProfileNoneFlushesBufferedStats(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 1)
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileAll,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})
	queryMetadata := queryStatsMetadata{
		query:      "SELECT * FROM stats_table",
		unprepared: true,
	}
	control.observeQuery(queryMetadata)
	control.observe(newStatsSuccess(
		statsRequestMetadata{
			requestName: "Query",
			query:       &queryMetadata,
		},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	require.NoError(t, control.SetProfile(StatsProfileNone))
	assert.Nil(t, control.snapshotAndReset())
	select {
	case snapshot := <-snapshots:
		payload := decodeStatsSnapshot(t, snapshot)
		request := findStatsRequest(t, payload, "Query")
		assert.Equal(t, float64(1), request["httpRequestCount"])
		queries, ok := payload["queries"].([]interface{})
		require.True(t, ok)
		require.Len(t, queries, 1)
		assert.Equal(t, queryMetadata.query, queries[0].(map[string]interface{})["query"])
	default:
		t.Fatal("expected buffered ALL-profile stats to be flushed")
	}

	require.NoError(t, control.SetProfile(StatsProfileAll))
	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, requests)
	assert.NotContains(t, payload, "queries")
	assert.NotContains(t, payload, "connections")
}

func TestStatsControlSetProfileResumesWhenAlreadyStarted(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsEnableLog: boolPtr(false),
	})
	assert.True(t, control.IsStarted())

	require.NoError(t, control.SetProfile(StatsProfileNone))
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Get"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))
	assert.Nil(t, control.snapshotAndReset())

	require.NoError(t, control.SetProfile(StatsProfileAll))
	assert.True(t, control.IsStarted())
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Put"},
		11,
		21,
		2*time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Put")
	assert.Equal(t, float64(1), request["httpRequestCount"])
}

func TestStatsControlSetProfileStartsEmitterWhenAlreadyStarted(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 2)
	control := newStatsControl(Config{
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})

	control.Start()
	assert.True(t, control.IsStarted())
	assert.False(t, control.inner.emitterStarted)

	require.NoError(t, control.SetProfile(StatsProfileRegular))
	assert.True(t, control.inner.emitterStarted)
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Put"},
		11,
		21,
		2*time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))
	control.shutdown()

	select {
	case snapshot := <-snapshots:
		payload := decodeStatsSnapshot(t, snapshot)
		request := findStatsRequest(t, payload, "Put")
		assert.Equal(t, float64(1), request["httpRequestCount"])
	default:
		t.Fatal("expected shutdown to flush stats after runtime profile enable")
	}
}

func TestStatsControlEnabledFastPathTracksGateAndProfile(t *testing.T) {
	control := newStatsControl(Config{
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
	})
	defer control.shutdown()

	assert.False(t, control.isEnabled())
	control.Start()
	assert.False(t, control.isEnabled())

	require.NoError(t, control.SetProfile(StatsProfileRegular))
	assert.True(t, control.isEnabled())

	control.Stop()
	assert.False(t, control.isEnabled())

	require.NoError(t, control.SetProfile(StatsProfileAll))
	assert.False(t, control.isEnabled())

	control.Start()
	assert.True(t, control.isEnabled())

	require.NoError(t, control.SetProfile(StatsProfileNone))
	assert.False(t, control.isEnabled())
}

func TestStatsControlRuntimeEnableUsesInfoLogger(t *testing.T) {
	client, err := NewClient(Config{
		Mode:     "cloudsim",
		Endpoint: "localhost:8080",
	})
	require.NoError(t, err)
	defer client.Close()

	control := client.GetStatsControl()
	require.NotNil(t, control)
	require.NoError(t, control.SetProfile(StatsProfileRegular))

	require.NotNil(t, control.inner.logger)
	assert.NotEqual(t, logger.DefaultLogger, control.inner.logger)
}

func TestStatsControlStopPreventsObservationsAndStartResumes(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileMore,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
	})

	control.Stop()
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Get"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	control.Start()
	defer control.shutdown()
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Put"},
		11,
		21,
		2*time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	findStatsRequest(t, payload, "Put")
	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	require.Len(t, requests, 1)
}

func TestStatsControlStartStopAreIdempotent(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
	})

	control.Start()
	control.Start()
	assert.True(t, control.IsStarted())
	control.Stop()
	control.Stop()
	assert.False(t, control.IsStarted())
	control.shutdown()
	control.shutdown()
}

func TestStatsControlStartStop(t *testing.T) {
	control := newStatsControl(Config{})

	assert.False(t, control.IsStarted())
	control.Start()
	assert.True(t, control.IsStarted())
	control.Stop()
	assert.False(t, control.IsStarted())
}

func TestStatsControlStartAfterProfileChangeStartsEmitter(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 2)
	control := newStatsControl(Config{
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})

	control.Start()
	assert.False(t, control.inner.emitterStarted)

	require.NoError(t, control.SetProfile(StatsProfileRegular))
	control.Start()
	defer control.shutdown()
	assert.True(t, control.inner.emitterStarted)

	emitStatsIntervalForTest(t, control, snapshots)
}

func TestClientGetStatsControlReturnsSharedState(t *testing.T) {
	client, err := NewClient(Config{
		Mode:             "cloudsim",
		Endpoint:         "localhost:8080",
		StatsProfile:     StatsProfileRegular,
		StatsInterval:    5 * time.Second,
		StatsPrettyPrint: true,
		StatsEnableLog:   boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	control := client.GetStatsControl()
	sameControl := client.GetStatsControl()

	require.NotNil(t, control)
	require.NotNil(t, sameControl)
	assert.Equal(t, StatsProfileRegular, control.GetProfile())
	assert.Equal(t, 5*time.Second, control.GetInterval())
	assert.True(t, control.GetPrettyPrint())
	assert.True(t, control.IsStarted())

	err = control.SetProfile(StatsProfileMore)
	require.NoError(t, err)
	sameControl.SetPrettyPrint(false)
	sameControl.Stop()

	assert.Equal(t, StatsProfileMore, sameControl.GetProfile())
	assert.False(t, control.GetPrettyPrint())
	assert.False(t, control.IsStarted())
}

func TestStatsControlObservesRequestLifecycleStats(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile: StatsProfileMore,
	})

	metadata := statsRequestMetadata{
		requestName: "Put",
	}
	control.observe(newStatsSuccess(
		metadata,
		120,
		240,
		12*time.Millisecond,
		2,
		30*time.Millisecond,
		1,
		1,
		5*time.Millisecond,
	))
	control.observe(newStatsError(
		metadata,
		120,
		80,
		2,
		30*time.Millisecond,
		1,
		1,
		5*time.Millisecond,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Put")

	assert.Equal(t, float64(2), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
	assert.Equal(t, float64(10), request["rateLimitDelayMs"])

	resultSize := request["resultSize"].(map[string]interface{})
	assert.Equal(t, float64(240), resultSize["min"])
	assert.Equal(t, float64(240), resultSize["avg"])
	assert.Equal(t, float64(240), resultSize["max"])

	requestSize := request["requestSize"].(map[string]interface{})
	assert.Equal(t, float64(120), requestSize["min"])
	assert.Equal(t, float64(120), requestSize["avg"])
	assert.Equal(t, float64(120), requestSize["max"])

	latency := request["httpRequestLatencyMs"].(map[string]interface{})
	assert.Equal(t, float64(12), latency["min"])
	assert.Equal(t, float64(12), latency["avg"])
	assert.Equal(t, float64(12), latency["max"])
	assert.Equal(t, float64(12), latency["95th"])
	assert.Equal(t, float64(12), latency["99th"])

	retry := request["retry"].(map[string]interface{})
	assert.Equal(t, float64(60), retry["delayMs"])
	assert.Equal(t, float64(2), retry["authCount"])
	assert.Equal(t, float64(2), retry["throttleCount"])
	assert.Equal(t, float64(4), retry["count"])

	assert.NotContains(t, payload, "connections")
}

func TestStatsPercentileExactKeepsLatencySamples(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileMore,
		StatsPercentileMode: StatsPercentileExact,
	})

	for i := 1; i <= 10; i++ {
		control.observe(newStatsSuccess(
			statsRequestMetadata{requestName: "Get"},
			10,
			20,
			time.Duration(i)*time.Millisecond,
			0,
			0,
			0,
			0,
			0,
		))
	}

	summary := control.inner.requestStats["Get"].latency
	assert.Len(t, summary.samples, 10)
	assert.Nil(t, summary.histogram)
}

func TestStatsPercentileExactMatchesJavaRankSelection(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileMore,
		StatsEnableLog:      boolPtr(false),
		StatsPercentileMode: StatsPercentileExact,
	})

	for value := 11; value >= 1; value-- {
		control.observe(newStatsSuccess(
			statsRequestMetadata{requestName: "Get"},
			10,
			20,
			time.Duration(value)*time.Millisecond,
			0,
			0,
			0,
			0,
			0,
		))
	}

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	latency := request["httpRequestLatencyMs"].(map[string]interface{})
	assert.Equal(t, float64(10), latency["95th"])
	assert.Equal(t, float64(11), latency["99th"])
}

func TestStatsPercentileExactCalculatesBothPercentilesFromOneSort(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileMore,
		StatsEnableLog:      boolPtr(false),
		StatsPercentileMode: StatsPercentileExact,
	})

	for value := 100; value >= 1; value-- {
		control.observe(newStatsSuccess(
			statsRequestMetadata{requestName: "Get"},
			10,
			20,
			time.Duration(value)*time.Millisecond,
			0,
			0,
			0,
			0,
			0,
		))
	}

	summary := &control.inner.requestStats["Get"].latency
	assert.False(t, sort.SliceIsSorted(summary.samples, func(i, j int) bool {
		return summary.samples[i] < summary.samples[j]
	}))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	latency := request["httpRequestLatencyMs"].(map[string]interface{})
	assert.Equal(t, float64(95), latency["95th"])
	assert.Equal(t, float64(99), latency["99th"])
	assert.True(t, sort.SliceIsSorted(summary.samples, func(i, j int) bool {
		return summary.samples[i] < summary.samples[j]
	}))
}

func TestStatsControlConcurrentObservations(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileMore,
		StatsEnableLog:      boolPtr(false),
		StatsPercentileMode: StatsPercentileHDR,
	})

	const (
		workerCount           = 16
		observationsPerWorker = 500
	)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for observation := 0; observation < observationsPerWorker; observation++ {
				control.observe(newStatsSuccess(
					statsRequestMetadata{requestName: "Get"},
					10,
					20,
					time.Millisecond,
					0,
					0,
					0,
					0,
					0,
				))
			}
		}()
	}
	wg.Wait()

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	assert.Equal(t, float64(workerCount*observationsPerWorker), request["httpRequestCount"])
}

func TestStatsPercentileHDRUsesBoundedLatencyHistogram(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileMore,
		StatsPercentileMode: StatsPercentileHDR,
	})

	for i := 1; i <= 5000; i++ {
		control.observe(newStatsSuccess(
			statsRequestMetadata{requestName: "Get"},
			10,
			20,
			time.Duration(i)*time.Millisecond,
			0,
			0,
			0,
			0,
			0,
		))
	}

	summary := control.inner.requestStats["Get"].latency
	assert.Empty(t, summary.samples)
	require.NotNil(t, summary.histogram)
	assert.Equal(t, uint64(5000), summary.histogram.count)

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	latency := request["httpRequestLatencyMs"].(map[string]interface{})
	assert.Equal(t, float64(1), latency["min"])
	assert.Equal(t, float64(2500.5), latency["avg"])
	assert.Equal(t, float64(5000), latency["max"])
	assert.GreaterOrEqual(t, latency["95th"].(float64), float64(4500))
	assert.LessOrEqual(t, latency["95th"].(float64), float64(5000))
	assert.GreaterOrEqual(t, latency["99th"].(float64), float64(4900))
	assert.LessOrEqual(t, latency["99th"].(float64), float64(5000))
}

func TestStatsRegularSnapshotOmitsPercentilesAndQueryStats(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile: StatsProfileRegular,
	})

	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Get"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	latency := request["httpRequestLatencyMs"].(map[string]interface{})

	assert.NotContains(t, latency, "95th")
	assert.NotContains(t, latency, "99th")
	assert.NotContains(t, payload, "queries")
	assert.NotContains(t, payload, "profile")
	assert.NotContains(t, payload, "sdkName")
	assert.NotContains(t, payload, "sdkVersion")
}

func TestStatsNoneDoesNotSnapshot(t *testing.T) {
	control := newStatsControl(Config{})

	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Get"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	assert.Nil(t, control.snapshotAndReset())
}

func TestStatsEmitterProducesJavaStyleEmptyIntervals(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 4)
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})

	snapshot := emitStatsIntervalForTest(t, control, snapshots)

	payload := decodeStatsSnapshot(t, snapshot)
	assert.NotEmpty(t, payload["clientId"])
	assert.NotEmpty(t, payload["startTime"])
	assert.NotEmpty(t, payload["endTime"])

	requests, ok := payload["requests"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, requests)
	assert.NotContains(t, payload, "connections")
	assert.NotContains(t, payload, "queries")
}

func TestStatsEmitterFlushesCollectedStatsOnShutdown(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 4)
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileMore,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})
	control.startEmitter()

	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Put"},
		8,
		16,
		3*time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))
	control.shutdown()

	var snapshot *StatsSnapshot
	select {
	case snapshot = <-snapshots:
	default:
		t.Fatal("expected shutdown to flush a stats snapshot")
	}

	payload := decodeStatsSnapshot(t, snapshot)
	request := findStatsRequest(t, payload, "Put")
	assert.Equal(t, float64(1), request["httpRequestCount"])
	latency := request["httpRequestLatencyMs"].(map[string]interface{})
	assert.Contains(t, latency, "95th")
	assert.Contains(t, latency, "99th")
}

func TestStatsEmitterDoesNotEmitAfterShutdown(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 8)
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})
	control.startEmitter()
	control.shutdown()

	select {
	case snapshot := <-snapshots:
		require.NotNil(t, snapshot)
	default:
		t.Fatal("expected shutdown to flush a stats snapshot")
	}

	select {
	case snapshot := <-snapshots:
		t.Fatalf("unexpected stats snapshot after shutdown: %s", snapshot.JSON())
	default:
	}
}

func TestStatsControlShutdownDisablesLateObservations(t *testing.T) {
	snapshots := make(chan *StatsSnapshot, 2)
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			snapshots <- stats
		}),
	})
	control.startEmitter()
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Get"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	control.shutdown()
	assert.False(t, control.IsStarted())
	assert.False(t, control.isEnabled())

	select {
	case snapshot := <-snapshots:
		payload := decodeStatsSnapshot(t, snapshot)
		request := findStatsRequest(t, payload, "Get")
		assert.Equal(t, float64(1), request["httpRequestCount"])
	default:
		t.Fatal("expected shutdown to flush the final observation")
	}

	control.Start()
	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Put"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))
	assert.False(t, control.IsStarted())
	assert.False(t, control.isEnabled())
	assert.Nil(t, control.emitSnapshot())

	err := control.SetProfile(StatsProfileMore)
	require.Error(t, err)
	statsErr, ok := err.(*nosqlerr.Error)
	require.True(t, ok)
	assert.Equal(t, nosqlerr.IllegalState, statsErr.Code)

	control.inner.mu.RLock()
	assert.Empty(t, control.inner.requestStats)
	control.inner.mu.RUnlock()
}

func TestStatsControlConcurrentStartAndShutdown(t *testing.T) {
	for iteration := 0; iteration < 25; iteration++ {
		control := newStatsControl(Config{
			StatsProfile:   StatsProfileRegular,
			StatsInterval:  time.Hour,
			StatsEnableLog: boolPtr(false),
		})

		var wg sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				control.Start()
			}()
			go func() {
				defer wg.Done()
				control.shutdown()
			}()
		}
		wg.Wait()

		control.Start()
		assert.False(t, control.IsStarted())
		assert.False(t, control.isEnabled())

		control.inner.mu.RLock()
		emitterStarted := control.inner.emitterStarted
		closed := control.inner.closed
		control.inner.mu.RUnlock()
		assert.True(t, closed)
		if emitterStarted {
			select {
			case <-control.inner.doneCh:
			case <-time.After(5 * time.Second):
				t.Fatal("stats emitter did not stop after shutdown")
			}
		}
	}
}

func TestClientCloseFromFinalStatsHandlerClosesResourcesOnce(t *testing.T) {
	provider := &statsCloseCountingProvider{}
	var client *Client
	var handlerCalls int32
	var handlerCloseErr error

	var err error
	client, err = NewClient(Config{
		Mode:                  "cloudsim",
		Endpoint:              "localhost:8080",
		AuthorizationProvider: provider,
		StatsProfile:          StatsProfileRegular,
		StatsInterval:         time.Hour,
		StatsEnableLog:        boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(*StatsSnapshot) {
			atomic.AddInt32(&handlerCalls, 1)
			handlerCloseErr = client.Close()
		}),
	})
	require.NoError(t, err)

	require.NoError(t, client.Close())
	require.NoError(t, handlerCloseErr)
	assert.Equal(t, int32(1), atomic.LoadInt32(&handlerCalls))
	assert.Equal(t, int32(1), atomic.LoadInt32(&provider.closeCalls))

	require.NoError(t, client.Close())
	assert.Equal(t, int32(1), atomic.LoadInt32(&provider.closeCalls))
}

func TestStatsHandlerCanCloseClient(t *testing.T) {
	var client *Client
	var handlerCalls int32
	var handlerMu sync.Mutex
	handlerClosedClient := make(chan struct{})
	clientClosed := make(chan struct{})

	var err error
	client, err = NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileRegular,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			handlerMu.Lock()
			defer handlerMu.Unlock()
			call := atomic.AddInt32(&handlerCalls, 1)
			_ = client.Close()
			if call == 1 {
				close(handlerClosedClient)
			}
		}),
	})
	require.NoError(t, err)

	go func() {
		client.GetStatsControl().emitSnapshot()
		close(clientClosed)
	}()

	select {
	case <-handlerClosedClient:
	case <-time.After(5 * time.Second):
		t.Fatal("stats handler deadlocked while closing its client")
	}
	select {
	case <-clientClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("stats emission did not return after the handler closed its client")
	}
	select {
	case <-client.GetStatsControl().inner.doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("stats emitter did not exit after handler-triggered client close")
	}
	assert.Equal(t, int32(2), atomic.LoadInt32(&handlerCalls))
}

func TestStatsShutdownLogsFinalSnapshotBeforeBlockedHandlerReturns(t *testing.T) {
	var out bytes.Buffer
	var handlerCalls int32
	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	finalHandled := make(chan struct{})

	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsInterval:  time.Hour,
		StatsEnableLog: boolPtr(true),
		LoggingConfig: LoggingConfig{
			Logger: logger.New(&out, logger.Info, false),
		},
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			switch atomic.AddInt32(&handlerCalls, 1) {
			case 1:
				close(handlerEntered)
				<-releaseHandler
			case 2:
				close(finalHandled)
			}
		}),
	})
	control.startEmitter()
	defer func() {
		select {
		case <-releaseHandler:
		default:
			close(releaseHandler)
		}
	}()

	emitDone := make(chan struct{})
	go func() {
		control.emitSnapshot()
		close(emitDone)
	}()

	select {
	case <-handlerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("stats handler was not invoked")
	}

	control.observe(newStatsSuccess(
		statsRequestMetadata{requestName: "Put"},
		10,
		20,
		time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	shutdownDone := make(chan struct{})
	go func() {
		control.shutdown()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stats shutdown waited for a blocked handler")
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&handlerCalls))
	intervals := decodeStatsIntervalLogs(t, out.String())
	require.Len(t, intervals, 2)
	require.Empty(t, intervals[0]["requests"])
	finalRequest := findStatsRequest(t, intervals[1], "Put")
	assert.Equal(t, float64(1), finalRequest["httpRequestCount"])

	close(releaseHandler)
	select {
	case <-emitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stats emission did not return after releasing the handler")
	}
	select {
	case <-finalHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("deferred final snapshot was not delivered")
	}
	select {
	case <-control.inner.doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("stats emitter did not stop after shutdown")
	}
	assert.Equal(t, int32(2), atomic.LoadInt32(&handlerCalls))
	assert.Len(t, decodeStatsIntervalLogs(t, out.String()), 2)
}

func TestStatsHandlerPanicDoesNotEscapeEmission(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsEnableLog: boolPtr(false),
		StatsHandler: StatsHandlerFunc(func(stats *StatsSnapshot) {
			panic("handler failed")
		}),
	})

	assert.NotPanics(t, func() {
		snapshot := control.emitSnapshot()
		require.NotNil(t, snapshot)
	})
}

func TestStatsInitLogIncludesConfigurationMetadata(t *testing.T) {
	var out bytes.Buffer
	control := newStatsControl(Config{
		StatsProfile:        StatsProfileAll,
		StatsInterval:       5 * time.Second,
		StatsPrettyPrint:    true,
		StatsEnableLog:      boolPtr(true),
		StatsPercentileMode: StatsPercentileHDR,
		LoggingConfig: LoggingConfig{
			Logger: logger.New(&out, logger.Info, false),
		},
		RateLimitingEnabled: true,
	})

	control.startEmitter()
	control.shutdown()

	logOutput := out.String()
	assert.Contains(t, logOutput, StatsLogPrefix)
	assert.Contains(t, logOutput, `"sdkName":"Oracle NoSQL SDK for Go"`)
	assert.Contains(t, logOutput, `"sdkVersion":`)
	assert.Contains(t, logOutput, `"clientId":`)
	assert.Contains(t, logOutput, `"profile":"ALL"`)
	assert.Contains(t, logOutput, `"intervalSec":5`)
	assert.Contains(t, logOutput, `"prettyPrint":true`)
	assert.Contains(t, logOutput, `"percentileMode":"HDR"`)
	assert.Contains(t, logOutput, `"rateLimitingEnabled":true`)
}

func TestStatsUsesInfoLoggerWhenDefaultLoggerWouldSuppressStats(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsEnableLog: boolPtr(true),
		LoggingConfig: LoggingConfig{
			Logger: logger.DefaultLogger,
		},
	})

	require.NotNil(t, control.inner.logger)
	assert.NotEqual(t, logger.DefaultLogger, control.inner.logger)
}

func TestStatsDisableLoggingSuppressesDefaultStatsLog(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile: StatsProfileRegular,
		LoggingConfig: LoggingConfig{
			DisableLogging: true,
		},
	})

	assert.False(t, control.inner.enableLog)
	assert.Nil(t, control.inner.logger)
}

func TestStatsDisableLoggingAllowsExplicitStatsLogOptIn(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile:   StatsProfileRegular,
		StatsEnableLog: boolPtr(true),
		LoggingConfig: LoggingConfig{
			DisableLogging: true,
		},
	})

	assert.True(t, control.inner.enableLog)
	assert.NotNil(t, control.inner.logger)
}

func TestStatsDisableLoggingAllowsCustomLoggerOptIn(t *testing.T) {
	var out bytes.Buffer
	statsLogger := logger.New(&out, logger.Info, false)
	control := newStatsControl(Config{
		StatsProfile: StatsProfileRegular,
		LoggingConfig: LoggingConfig{
			Logger:         statsLogger,
			DisableLogging: true,
		},
	})

	assert.True(t, control.inner.enableLog)
	assert.Same(t, statsLogger, control.inner.logger)
}

func TestDoExecuteRecordsStatsObservation(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	client.executor = statsTestExecutor{
		body:       "response-body",
		statusCode: http.StatusOK,
	}
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		assert.Equal(t, "response-body", string(data))
		return &GetResult{}, nil
	}

	req := &GetRequest{
		TableName: "stats_table",
		Timeout:   time.Second,
	}
	_, err = client.doExecute(context.Background(), req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.NoError(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")

	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(0), request["errors"])

	requestSize := request["requestSize"].(map[string]interface{})
	assert.Equal(t, float64(3), requestSize["min"])
	assert.Equal(t, float64(3), requestSize["max"])

	resultSize := request["resultSize"].(map[string]interface{})
	assert.Equal(t, float64(len("response-body")), resultSize["min"])
	assert.Equal(t, float64(len("response-body")), resultSize["max"])

	latency := request["httpRequestLatencyMs"].(map[string]interface{})
	assert.Contains(t, latency, "95th")
	assert.Contains(t, latency, "99th")
}

func TestReadHTTPResponseBodyAlwaysClosesBody(t *testing.T) {
	tests := []struct {
		name     string
		reader   io.Reader
		wantData string
		wantErr  error
	}{
		{
			name:     "success",
			reader:   strings.NewReader("response-body"),
			wantData: "response-body",
		},
		{
			name:     "read error",
			reader:   statsFailingReader{},
			wantData: "",
			wantErr:  io.ErrUnexpectedEOF,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &statsTrackingReadCloser{reader: test.reader}
			data, err := readHTTPResponseBody(&http.Response{Body: body})
			if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, test.wantErr, err)
			}
			assert.Equal(t, test.wantData, string(data))
			assert.Equal(t, int32(1), atomic.LoadInt32(&body.closeCount))
		})
	}
}

func TestDoExecuteRecordsStatsObservationOnResponseHandlerError(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	client.executor = statsTestExecutor{
		body:       "bad-response-body",
		statusCode: http.StatusOK,
	}
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		assert.Equal(t, "bad-response-body", string(data))
		return nil, nosqlerr.New(nosqlerr.IllegalArgument, "response handler failed")
	}

	req := &GetRequest{
		TableName: "stats_table",
		Timeout:   time.Second,
	}
	_, err = client.doExecute(context.Background(), req, []byte{1, 2, 3, 4}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.Error(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
	assert.NotContains(t, request, "httpRequestLatencyMs")
}

func TestDoExecuteRecordsSingleStatsObservationForRetriedSuccess(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	retryHandler, err := NewDefaultRetryHandler(1, time.Millisecond)
	require.NoError(t, err)
	client.RetryHandler = retryHandler
	client.executor = &statsSequenceExecutor{
		steps: []statsExecutorStep{
			{
				err: &url.Error{
					Op:  http.MethodPost,
					URL: "http://localhost:8080",
					Err: mockErr{msg: "temporary transport failure", isTemp: true},
				},
			},
			{
				body:       "response-body",
				statusCode: http.StatusOK,
			},
		},
	}
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		assert.Equal(t, "response-body", string(data))
		return &GetResult{}, nil
	}

	req := &GetRequest{
		TableName: "stats_table",
		Timeout:   time.Second,
	}
	_, err = client.doExecute(context.Background(), req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.NoError(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	retry := statsRetryPayload(t, request)

	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(0), request["errors"])
	assert.Equal(t, float64(1), retry["count"])
	assert.Equal(t, float64(1), retry["delayMs"])
}

func TestDoExecuteCountsRetryAuthenticationOnRetriedSuccess(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	retryHandler, err := NewDefaultRetryHandler(1, time.Millisecond)
	require.NoError(t, err)
	client.RetryHandler = retryHandler
	client.executor = &statsSequenceExecutor{
		steps: []statsExecutorStep{
			{body: "auth-failed", statusCode: http.StatusOK},
			{body: "response-body", statusCode: http.StatusOK},
		},
	}
	responseCount := 0
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		responseCount++
		if responseCount == 1 {
			return nil, nosqlerr.New(nosqlerr.RetryAuthentication, "authentication must be retried")
		}
		return &GetResult{}, nil
	}

	req := &GetRequest{TableName: "stats_table", Timeout: time.Second}
	_, err = client.doExecute(context.Background(), req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.NoError(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	retry := statsRetryPayload(t, request)
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(0), request["errors"])
	assert.Equal(t, float64(1), retry["count"])
	assert.Equal(t, float64(1), retry["authCount"])
	assert.Equal(t, float64(0), retry["throttleCount"])
}

func TestDoExecuteCountsRetryAuthenticationOnExhaustedRetry(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	retryHandler, err := NewDefaultRetryHandler(1, time.Millisecond)
	require.NoError(t, err)
	client.RetryHandler = retryHandler
	client.executor = &statsSequenceExecutor{
		steps: []statsExecutorStep{
			{body: "auth-failed", statusCode: http.StatusOK},
			{body: "auth-failed-again", statusCode: http.StatusOK},
		},
	}
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		return nil, nosqlerr.New(nosqlerr.RetryAuthentication, "authentication must be retried")
	}

	req := &GetRequest{TableName: "stats_table", Timeout: time.Second}
	_, err = client.doExecute(context.Background(), req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.Error(t, err)
	assert.True(t, nosqlerr.Is(err, nosqlerr.RetryAuthentication))

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	retry := statsRetryPayload(t, request)
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
	assert.Equal(t, float64(1), retry["count"])
	assert.Equal(t, float64(1), retry["authCount"])
	assert.Equal(t, float64(0), retry["throttleCount"])
}

func TestDoExecuteRecordsStatsObservationOnContextCancel(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	client.executor = statsBlockingExecutor{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := &GetRequest{
		TableName: "stats_table",
		Timeout:   time.Second,
	}
	_, err = client.doExecute(ctx, req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
	assert.NotContains(t, request, "httpRequestLatencyMs")
}

func TestDoExecuteRecordsStatsWhenRetryDelayIsCanceled(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	retryHandler := &statsBlockingRetryHandler{entered: make(chan struct{})}
	client.RetryHandler = retryHandler
	client.executor = &statsSequenceExecutor{
		steps: []statsExecutorStep{{
			err: &url.Error{
				Op:  http.MethodPost,
				URL: "http://localhost:8080",
				Err: mockErr{msg: "temporary transport failure", isTemp: true},
			},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		req := &GetRequest{TableName: "stats_table", Timeout: 5 * time.Second}
		_, executeErr := client.doExecute(ctx, req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
		result <- executeErr
	}()

	select {
	case <-retryHandler.entered:
	case <-time.After(time.Second):
		t.Fatal("retry handler was not entered")
	}
	cancel()

	select {
	case err = <-result:
	case <-time.After(time.Second):
		t.Fatal("request did not stop after context cancellation")
	}
	require.Equal(t, context.Canceled, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
}

func TestDoExecuteRecordsStatsWhenRateLimiterWaitIsCanceled(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	limiter := &statsBlockingRateLimiter{
		RateLimiter: common.NewSimpleRateLimiter(1),
		entered:     make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		req := &GetRequest{TableName: "stats_table", Timeout: 5 * time.Second}
		req.SetReadRateLimiter(limiter)
		_, executeErr := client.doExecute(ctx, req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
		result <- executeErr
	}()

	select {
	case <-limiter.entered:
	case <-time.After(time.Second):
		t.Fatal("rate limiter was not entered")
	}
	cancel()

	select {
	case err = <-result:
	case <-time.After(time.Second):
		t.Fatal("request did not stop after context cancellation")
	}
	require.Equal(t, context.Canceled, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
}

func TestDoExecuteRecordsStatsObservationOnRequestTimeout(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileMore,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	client.executor = statsBlockingExecutor{}

	req := &GetRequest{
		TableName: "stats_table",
		Timeout:   time.Millisecond,
	}
	_, err = client.doExecute(context.Background(), req, []byte{1, 2, 3}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.Error(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	request := findStatsRequest(t, payload, "Get")
	assert.Equal(t, float64(1), request["httpRequestCount"])
	assert.Equal(t, float64(1), request["errors"])
	assert.NotContains(t, request, "httpRequestLatencyMs")
}

func TestStatsAllAggregatesQueryStats(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile: StatsProfileAll,
	})

	queryMetadata := queryStatsMetadata{
		query:      `SELECT * FROM stats_table WHERE grp = "g0"`,
		unprepared: true,
		simple:     false,
		doesWrites: false,
		plan:       "query-plan",
	}
	control.observeQuery(queryMetadata)
	control.observeQuery(queryMetadata)

	requestMetadata := statsRequestMetadata{
		requestName: "Query",
		query:       &queryMetadata,
	}
	control.observe(newStatsSuccess(
		requestMetadata,
		64,
		128,
		10*time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))
	control.observe(newStatsSuccess(
		requestMetadata,
		96,
		160,
		20*time.Millisecond,
		1,
		5*time.Millisecond,
		0,
		0,
		0,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	queryRequest := findStatsRequest(t, payload, "Query")
	assert.Equal(t, float64(2), queryRequest["httpRequestCount"])

	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok)
	require.Len(t, queries, 1)
	query := queries[0].(map[string]interface{})

	assert.Equal(t, false, query["doesWrites"])
	assert.Equal(t, float64(2), query["unprepared"])
	assert.Equal(t, float64(2), query["httpRequestCount"])
	assert.Equal(t, queryMetadata.query, query["query"])
	assert.Equal(t, float64(2), query["count"])
	assert.Equal(t, false, query["simple"])
	assert.Equal(t, "query-plan", query["plan"])

	latency := query["httpRequestLatencyMs"].(map[string]interface{})
	assert.Equal(t, float64(10), latency["min"])
	assert.Equal(t, float64(15), latency["avg"])
	assert.Equal(t, float64(20), latency["max"])
	assert.Equal(t, float64(20), latency["95th"])
	assert.Equal(t, float64(20), latency["99th"])

	retry := query["retry"].(map[string]interface{})
	assert.Equal(t, float64(5), retry["delayMs"])
	assert.Equal(t, float64(1), retry["count"])
}

func TestStatsAllPreparedQueryCanReportSimple(t *testing.T) {
	control := newStatsControl(Config{
		StatsProfile: StatsProfileAll,
	})

	queryMetadata := queryStatsMetadata{
		query:      "SELECT * FROM stats_table WHERE id = $id",
		unprepared: false,
		simple:     true,
		doesWrites: false,
	}
	control.observeQuery(queryMetadata)
	control.observe(newStatsSuccess(
		statsRequestMetadata{
			requestName: "Query",
			query:       &queryMetadata,
		},
		64,
		128,
		10*time.Millisecond,
		0,
		0,
		0,
		0,
		0,
	))

	payload := decodeStatsSnapshot(t, control.snapshotAndReset())
	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok)
	require.Len(t, queries, 1)

	query := queries[0].(map[string]interface{})
	assert.Equal(t, float64(1), query["count"])
	assert.Equal(t, float64(0), query["unprepared"])
	assert.Equal(t, true, query["simple"])
}

func TestStatsQueryTextDoesWritesIgnoresLeadingComments(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "line comment before update",
			query: "-- generated by app\nUPDATE stats_table SET name = \"n\" WHERE id = 1",
			want:  true,
		},
		{
			name:  "block comment before delete",
			query: "/* trace id: 123 */ DELETE FROM stats_table WHERE id = 1",
			want:  true,
		},
		{
			name:  "nested leading comments before select",
			query: "/* trace id: 123 */ -- readonly\n SELECT * FROM stats_table",
			want:  false,
		},
		{
			name:  "unterminated comment",
			query: "/* generated */ /* unfinished UPDATE stats_table SET name = \"n\"",
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, queryTextDoesWrites(test.query))
		})
	}
}

func TestPreparedStatementDoesWritesUsesOperation(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		operation int
		want      bool
	}{
		{
			name:      "known select operation overrides write-looking text",
			sql:       "UPDATE stats_table SET name = \"n\" WHERE id = 1",
			operation: queryOperationSelect,
			want:      false,
		},
		{
			name:      "known non-select operation overrides read-looking text",
			sql:       "SELECT * FROM stats_table",
			operation: 0,
			want:      true,
		},
		{
			name:      "unknown operation falls back to write text",
			sql:       "/* generated */ DELETE FROM stats_table WHERE id = 1",
			operation: queryOperationUnknown,
			want:      true,
		},
		{
			name:      "unknown operation falls back to read text",
			sql:       "/* generated */ SELECT * FROM stats_table",
			operation: queryOperationUnknown,
			want:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prep := &PreparedStatement{
				sqlText:   test.sql,
				operation: test.operation,
			}
			assert.Equal(t, test.want, prep.doesWrites())
		})
	}
}

func TestQueryStatsMetadataUsesPreparedOperation(t *testing.T) {
	req := &QueryRequest{
		PreparedStatement: &PreparedStatement{
			sqlText:   "SELECT * FROM stats_table",
			operation: 0,
		},
	}

	metadata := req.queryStatsMetadata()
	assert.True(t, metadata.doesWrites)
}

func TestQueryStatsMetadataUsesDriverPlan(t *testing.T) {
	driverPlan := &varRefIter{
		planIterDelegate: &planIterDelegate{resultReg: 3},
		name:             "row",
	}
	req := &QueryRequest{
		PreparedStatement: &PreparedStatement{
			sqlText:         "SELECT * FROM stats_table",
			queryPlan:       "server explain plan",
			operation:       queryOperationSelect,
			driverQueryPlan: driverPlan,
		},
	}

	metadata := req.queryStatsMetadata()
	assert.Equal(t, driverPlan.getPlan(), metadata.plan)
	assert.NotEqual(t, req.PreparedStatement.queryPlan, metadata.plan)
}

func TestPreparedStatementStatsDriverPlanCachesRenderedPlan(t *testing.T) {
	driverPlan := &statsCountingPlan{plan: "cached driver plan"}
	prepared := &PreparedStatement{
		driverQueryPlan: driverPlan,
		statsPlanCache:  &preparedStatementStatsCache{},
	}
	copied := *prepared
	statements := []*PreparedStatement{prepared, &copied}

	const workerCount = 32
	results := make(chan string, workerCount)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func(statement *PreparedStatement) {
			defer wg.Done()
			results <- statement.statsDriverPlan()
		}(statements[worker%len(statements)])
	}
	wg.Wait()
	close(results)

	for plan := range results {
		assert.Equal(t, driverPlan.plan, plan)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&driverPlan.calls))
}

func TestPrepareRequestDoesNotCreateQueryMetadata(t *testing.T) {
	metadata := statsRequestMetadataForRequest(&PrepareRequest{
		Statement: "SELECT * FROM stats_table",
	})

	assert.Equal(t, "Prepare", metadata.requestName)
	assert.Nil(t, metadata.query)
}

func TestDoExecuteRecordsLogicalQueryAndQueryRequestStats(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileAll,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	client.executor = statsTestExecutor{
		body:       "query-response",
		statusCode: http.StatusOK,
	}
	driverPlan := &varRefIter{
		planIterDelegate: &planIterDelegate{resultReg: 3},
		name:             "row",
	}
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		queryReq := req.(*QueryRequest)
		queryReq.PreparedStatement = &PreparedStatement{
			sqlText:         queryReq.Statement,
			queryPlan:       "server explain plan",
			operation:       queryOperationSelect,
			driverQueryPlan: driverPlan,
		}
		return newQueryResult(queryReq, true), nil
	}

	queryReq := &QueryRequest{
		Statement: "SELECT * FROM stats_table WHERE grp = \"g0\"",
		TableName: "stats_table",
		Timeout:   time.Second,
	}
	_, err = client.doExecute(context.Background(), queryReq, []byte{1, 2, 3, 4}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.NoError(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	queryRequest := findStatsRequest(t, payload, "Query")
	assert.Equal(t, float64(1), queryRequest["httpRequestCount"])

	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok)
	require.Len(t, queries, 1)

	query := queries[0].(map[string]interface{})
	assert.Equal(t, float64(1), query["count"])
	assert.Equal(t, float64(1), query["httpRequestCount"])
	assert.Equal(t, false, query["simple"])
	assert.Equal(t, driverPlan.getPlan(), query["plan"])
	assert.NotEqual(t, "server explain plan", query["plan"])
	assert.Equal(t, queryReq.Statement, query["query"])
}

func TestDoExecuteAttributesFailedInternalQueryRequest(t *testing.T) {
	client, err := NewClient(Config{
		Mode:           "cloudsim",
		Endpoint:       "localhost:8080",
		StatsProfile:   StatsProfileAll,
		StatsEnableLog: boolPtr(false),
	})
	require.NoError(t, err)
	defer client.Close()

	driverPlan := &varRefIter{
		planIterDelegate: &planIterDelegate{resultReg: 3},
		name:             "row",
	}
	prepared := &PreparedStatement{
		sqlText:         "SELECT * FROM stats_table WHERE grp = $grp",
		operation:       queryOperationSelect,
		driverQueryPlan: driverPlan,
	}
	queryReq := &QueryRequest{
		PreparedStatement: prepared,
		TableName:         "stats_table",
		Timeout:           time.Second,
		isInternal:        true,
	}
	client.GetStatsControl().observeQuery(queryReq.queryStatsMetadata())
	client.executor = statsTestExecutor{
		body:       "invalid-query-response",
		statusCode: http.StatusOK,
	}
	client.handleResponse = func(data []byte, httpResp *http.Response, req Request, serialVerUsed int16, queryVerUsed int16) (Result, error) {
		return nil, nosqlerr.New(nosqlerr.IllegalArgument, "internal query failed")
	}

	_, err = client.doExecute(context.Background(), queryReq, []byte{1, 2, 3, 4}, proto.DefaultSerialVersion, proto.DefaultQueryVersion)
	require.Error(t, err)

	payload := decodeStatsSnapshot(t, client.GetStatsControl().snapshotAndReset())
	queryRequest := findStatsRequest(t, payload, "Query")
	assert.Equal(t, float64(1), queryRequest["httpRequestCount"])
	assert.Equal(t, float64(1), queryRequest["errors"])

	queries, ok := payload["queries"].([]interface{})
	require.True(t, ok)
	require.Len(t, queries, 1)
	query := queries[0].(map[string]interface{})
	assert.Equal(t, prepared.sqlText, query["query"])
	assert.Equal(t, float64(1), query["count"])
	assert.Equal(t, float64(1), query["httpRequestCount"])
	assert.Equal(t, float64(1), query["errors"])
	assert.Equal(t, driverPlan.getPlan(), query["plan"])
}

func BenchmarkStatsSnapshotExact10000(b *testing.B) {
	benchmarkStatsSnapshot(b, StatsPercentileExact, 10_000)
}

func BenchmarkStatsSnapshotHDR10000(b *testing.B) {
	benchmarkStatsSnapshot(b, StatsPercentileHDR, 10_000)
}

func benchmarkStatsSnapshot(b *testing.B, mode StatsPercentileMode, sampleCount int) {
	b.Helper()
	base := requestLifecycleStats{successCount: uint64(sampleCount)}
	for value := sampleCount; value >= 1; value-- {
		base.latency.observe(time.Duration(value)*time.Millisecond, mode)
	}
	base.requestSize.observe(128)
	base.responseSize.observe(256)
	start := time.Unix(1_700_000_000, 0).UTC()

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		stats := base
		if mode == StatsPercentileExact {
			stats.latency.samples = append([]int64(nil), base.latency.samples...)
		}
		state := &statsSnapshotState{
			clientID:       "benchmark-client",
			profile:        StatsProfileMore,
			percentileMode: mode,
			startTime:      start,
			endTime:        start.Add(time.Minute),
			requestStats: map[string]*requestLifecycleStats{
				"Get": &stats,
			},
			queryStats: make(map[string]*queryEntryStats),
		}
		if snapshot := statsSnapshotFromState(state); snapshot == nil {
			b.Fatal("expected a stats snapshot")
		}
	}
}
