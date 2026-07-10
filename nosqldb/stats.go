//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

var nextStatsClientID uint64

// StatsLogPrefix matches the Java SDK prefix for client-side statistics logs.
const StatsLogPrefix = "Client stats|"

// Preserve Java SDK request ordering in emitted stats JSON so Go SDK output is
// easy to compare against Java logs and tests.
var javaRequestOutputOrder = []string{
	"Put",
	"WriteMultiple",
	"Get",
	"Prepare",
	"MultiDelete",
	"GetTable",
	"Delete",
	"Table",
	"ListTables",
	"GetIndexes",
	"Query",
	"System",
	"SystemStatus",
	"TableUsage",
	"ReplicaStats",
	"Write",
}

// StatsProfile controls how much client-side statistics data is collected.
//
// The profile names and behavior are intended to follow the Oracle NoSQL Java
// SDK statistics contract:
//
//   - StatsProfileNone disables statistics collection.
//   - StatsProfileRegular collects aggregate request statistics.
//   - StatsProfileMore adds latency percentile statistics.
//   - StatsProfileAll adds query-level statistics.
type StatsProfile string

const (
	// StatsProfileNone disables statistics collection.
	StatsProfileNone StatsProfile = "NONE"

	// StatsProfileRegular enables aggregate request statistics.
	StatsProfileRegular StatsProfile = "REGULAR"

	// StatsProfileMore enables aggregate request statistics plus latency percentiles.
	StatsProfileMore StatsProfile = "MORE"

	// StatsProfileAll enables all available statistics, including raw query text
	// and client-side query plans. See Config.StatsProfile for data-sensitivity
	// and cardinality considerations.
	StatsProfileAll StatsProfile = "ALL"
)

// ParseStatsProfile parses a Java-compatible statistics profile name.
func ParseStatsProfile(value string) (StatsProfile, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", string(StatsProfileNone):
		return StatsProfileNone, nil
	case string(StatsProfileRegular):
		return StatsProfileRegular, nil
	case string(StatsProfileMore):
		return StatsProfileMore, nil
	case string(StatsProfileAll):
		return StatsProfileAll, nil
	default:
		return "", fmt.Errorf("invalid stats profile %q; expected one of: NONE, REGULAR, MORE, ALL", value)
	}
}

func (p StatsProfile) normalized() (StatsProfile, error) {
	return ParseStatsProfile(string(p))
}

func (p StatsProfile) isValid() bool {
	_, err := p.normalized()
	return err == nil
}

func (p StatsProfile) includesPercentiles() bool {
	normalized, err := p.normalized()
	if err != nil {
		return false
	}
	return normalized == StatsProfileMore || normalized == StatsProfileAll
}

// String returns the Java-compatible statistics profile name.
func (p StatsProfile) String() string {
	normalized, err := p.normalized()
	if err != nil {
		return string(p)
	}
	return string(normalized)
}

// StatsPercentileMode controls how latency percentile values are calculated.
//
// StatsPercentileExact stores one latency sample per successful request for the
// full interval and sorts the samples when the interval is emitted. Its memory
// use is O(requests per interval). StatsPercentileHDR uses bounded-memory
// histogram buckets and is recommended for high-throughput clients or long
// intervals.
type StatsPercentileMode string

const (
	// StatsPercentileExact keeps exact latency samples for percentile calculation.
	StatsPercentileExact StatsPercentileMode = "EXACT"

	// StatsPercentileHDR uses a bounded-memory HDR-style histogram.
	StatsPercentileHDR StatsPercentileMode = "HDR"
)

// ParseStatsPercentileMode parses a percentile mode name.
func ParseStatsPercentileMode(value string) (StatsPercentileMode, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", string(StatsPercentileExact), "SAMPLE", "SAMPLES":
		return StatsPercentileExact, nil
	case string(StatsPercentileHDR), "HISTOGRAM":
		return StatsPercentileHDR, nil
	default:
		return "", fmt.Errorf("invalid stats percentile mode %q; expected one of: EXACT, HDR", value)
	}
}

func (m StatsPercentileMode) normalized() (StatsPercentileMode, error) {
	return ParseStatsPercentileMode(string(m))
}

func (m StatsPercentileMode) isValid() bool {
	_, err := m.normalized()
	return err == nil
}

// String returns the configured percentile mode name.
func (m StatsPercentileMode) String() string {
	normalized, err := m.normalized()
	if err != nil {
		return string(m)
	}
	return string(normalized)
}

func activePercentileMode(profile StatsProfile, configured StatsPercentileMode) StatsPercentileMode {
	if !profile.includesPercentiles() {
		return ""
	}
	mode, err := configured.normalized()
	if err != nil {
		return StatsPercentileExact
	}
	return mode
}

// StatsSnapshot is delivered to a StatsHandler when a statistics snapshot is
// produced.
//
// Snapshots contain Java-compatible statistics JSON.
type StatsSnapshot struct {
	json string
}

func newStatsSnapshot(json string) *StatsSnapshot {
	return &StatsSnapshot{json: json}
}

// JSON returns the Java-compatible JSON payload for this statistics snapshot.
func (s *StatsSnapshot) JSON() string {
	if s == nil {
		return ""
	}
	return s.json
}

// String returns the Java-compatible JSON payload for this statistics snapshot.
func (s *StatsSnapshot) String() string {
	return s.JSON()
}

// StatsHandler is called when a statistics snapshot is produced.
//
// Implementations must be safe to call concurrently and should not block for
// long. The SDK invokes handlers from an internal stats emitter goroutine and
// may also invoke them during a profile change or the Client.Close final flush.
// A handler may call Client.Close. If any handler callback is active during
// Close, the final snapshot is logged synchronously and its handler callback is
// deferred until all active callbacks return. Close does not wait for that
// deferred callback.
type StatsHandler interface {
	HandleStats(stats *StatsSnapshot)
}

// StatsHandlerFunc adapts a function to the StatsHandler interface.
type StatsHandlerFunc func(stats *StatsSnapshot)

// HandleStats calls f(stats).
func (f StatsHandlerFunc) HandleStats(stats *StatsSnapshot) {
	if f != nil {
		f(stats)
	}
}

// StatsControl exposes runtime control over client-side statistics settings.
//
// Request observations are collected internally and emitted periodically as
// Java-compatible JSON while the owning Client is alive.
type StatsControl struct {
	inner *statsControlRef
}

type statsHandlerDelivery struct {
	snapshot *StatsSnapshot
	handler  StatsHandler
	logger   *logger.Logger
}

type statsControlRef struct {
	interval            time.Duration
	enableLog           bool
	rateLimitingEnabled bool
	clientID            string
	logger              *logger.Logger

	emitMu     sync.Mutex
	mu         sync.RWMutex
	deliveryMu sync.Mutex

	profile        StatsProfile
	prettyPrint    bool
	handler        StatsHandler
	started        bool
	closed         bool
	enabled        uint32
	percentileMode StatsPercentileMode

	intervalStart time.Time
	requestStats  map[string]*requestLifecycleStats
	queryStats    map[string]*queryEntryStats

	emitterStarted bool
	stopCh         chan struct{}
	doneCh         chan struct{}
	activeHandlers int
	pendingFinal   *statsHandlerDelivery
}

func newStatsControl(cfg Config) *StatsControl {
	profile := cfg.DefaultStatsProfile()
	percentileMode := cfg.DefaultStatsPercentileMode()
	if !percentileMode.isValid() {
		percentileMode = StatsPercentileExact
	}
	enableLog := statsLogEnabledForConfig(cfg)
	return &StatsControl{
		inner: &statsControlRef{
			interval:            cfg.DefaultStatsInterval(),
			enableLog:           enableLog,
			rateLimitingEnabled: cfg.RateLimitingEnabled,
			clientID:            newStatsClientID(),
			logger:              statsLoggerForConfig(cfg, profile, enableLog),
			profile:             profile,
			prettyPrint:         cfg.DefaultStatsPrettyPrint(),
			handler:             cfg.StatsHandler,
			started:             profile != StatsProfileNone,
			enabled:             boolToUint32(profile != StatsProfileNone),
			percentileMode:      percentileMode,
			intervalStart:       time.Now().UTC(),
			requestStats:        make(map[string]*requestLifecycleStats),
			queryStats:          make(map[string]*queryEntryStats),
			stopCh:              make(chan struct{}),
			doneCh:              make(chan struct{}),
		},
	}
}

func newStatsClientID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}

	// Keep client IDs distinct even if the operating system random source is
	// temporarily unavailable.
	return fmt.Sprintf("%x-%x-%x", time.Now().UnixNano(), os.Getpid(), atomic.AddUint64(&nextStatsClientID, 1))
}

func statsLogEnabledForConfig(cfg Config) bool {
	if !cfg.DefaultStatsEnableLog() {
		return false
	}
	if !cfg.DisableLogging {
		return true
	}
	if cfg.StatsEnableLog != nil && *cfg.StatsEnableLog {
		return true
	}
	return cfg.Logger != nil && cfg.Logger != logger.DefaultLogger
}

func statsLoggerForConfig(cfg Config, profile StatsProfile, enableLog bool) *logger.Logger {
	if profile == StatsProfileNone || !enableLog {
		return cfg.Logger
	}
	if cfg.Logger == nil || cfg.Logger == logger.DefaultLogger {
		return logger.New(os.Stderr, logger.Info, false)
	}
	return cfg.Logger
}

func boolToUint32(value bool) uint32 {
	if value {
		return 1
	}
	return 0
}

func (c *StatsControl) startEmitter() {
	if c == nil || c.inner == nil {
		return
	}

	c.inner.mu.Lock()
	if c.inner.closed || c.inner.profile == StatsProfileNone || c.inner.emitterStarted {
		c.inner.mu.Unlock()
		return
	}
	c.inner.emitterStarted = true
	interval := c.inner.interval
	c.inner.mu.Unlock()

	c.logInit()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(c.inner.doneCh)

		for {
			select {
			case <-ticker.C:
				c.emitSnapshot()
			case <-c.inner.stopCh:
				return
			}
		}
	}()
}

func (c *StatsControl) shutdown() {
	if c == nil || c.inner == nil {
		return
	}

	c.inner.mu.Lock()
	if c.inner.closed {
		c.inner.mu.Unlock()
		return
	}
	c.inner.closed = true
	c.inner.started = false
	atomic.StoreUint32(&c.inner.enabled, 0)
	emitterStarted := c.inner.emitterStarted
	close(c.inner.stopCh)
	c.inner.mu.Unlock()

	if emitterStarted {
		c.emitSnapshotInternal(true)
	}
}

func (c *StatsControl) emitSnapshot() *StatsSnapshot {
	return c.emitSnapshotInternal(false)
}

func (c *StatsControl) emitSnapshotInternal(final bool) *StatsSnapshot {
	if c == nil || c.inner == nil {
		return nil
	}

	c.inner.emitMu.Lock()
	if !final {
		c.inner.mu.RLock()
		closed := c.inner.closed
		c.inner.mu.RUnlock()
		if closed {
			c.inner.emitMu.Unlock()
			return nil
		}
	}

	snapshot := c.snapshotAndReset()
	delivery := c.prepareSnapshotDelivery(snapshot, final)
	c.inner.emitMu.Unlock()
	if snapshot == nil {
		return nil
	}
	if delivery != nil {
		c.invokeStatsHandler(delivery)
	}
	return snapshot
}

func (c *StatsControl) prepareSnapshotDelivery(snapshot *StatsSnapshot, final bool) *statsHandlerDelivery {
	if snapshot == nil {
		return nil
	}

	c.inner.mu.RLock()
	handler := c.inner.handler
	enableLog := c.inner.enableLog
	statsLogger := c.inner.logger
	c.inner.mu.RUnlock()

	if enableLog && statsLogger != nil {
		statsLogger.Info("%s%s", StatsLogPrefix, snapshot.JSON())
	}
	if handler == nil {
		return nil
	}

	delivery := &statsHandlerDelivery{
		snapshot: snapshot,
		handler:  handler,
		logger:   statsLogger,
	}
	c.inner.deliveryMu.Lock()
	defer c.inner.deliveryMu.Unlock()
	if final && c.inner.activeHandlers > 0 {
		c.inner.pendingFinal = delivery
		return nil
	}
	c.inner.activeHandlers++
	return delivery
}

func (c *StatsControl) invokeStatsHandler(delivery *statsHandlerDelivery) {
	func() {
		defer func() {
			if r := recover(); r != nil && delivery.logger != nil {
				delivery.logger.Warn("stats handler panicked: %v", r)
			}
		}()
		delivery.handler.HandleStats(delivery.snapshot)
	}()

	c.inner.deliveryMu.Lock()
	c.inner.activeHandlers--
	var pending *statsHandlerDelivery
	if c.inner.activeHandlers == 0 {
		pending = c.inner.pendingFinal
		c.inner.pendingFinal = nil
		if pending != nil {
			c.inner.activeHandlers++
		}
	}
	c.inner.deliveryMu.Unlock()

	if pending != nil {
		c.invokeStatsHandler(pending)
	}
}

func (c *StatsControl) logInit() {
	if c == nil || c.inner == nil {
		return
	}

	c.inner.mu.RLock()
	profile := c.inner.profile
	enableLog := c.inner.enableLog
	statsLogger := c.inner.logger
	clientID := c.inner.clientID
	interval := c.inner.interval
	prettyPrint := c.inner.prettyPrint
	percentileMode := c.inner.percentileMode
	rateLimitingEnabled := c.inner.rateLimitingEnabled
	c.inner.mu.RUnlock()

	if profile == StatsProfileNone || !enableLog || statsLogger == nil {
		return
	}

	payload := statsInitJSON{
		SDKName:             "Oracle NoSQL SDK for Go",
		SDKVersion:          sdkutil.SDKVersion(),
		ClientID:            clientID,
		Profile:             profile.String(),
		IntervalSec:         int64(interval / time.Second),
		PrettyPrint:         prettyPrint,
		PercentileMode:      percentileMode.String(),
		RateLimitingEnabled: rateLimitingEnabled,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	statsLogger.Info("%s%s", StatsLogPrefix, string(data))
}

// GetInterval returns the configured statistics interval.
func (c *StatsControl) GetInterval() time.Duration {
	if c == nil || c.inner == nil {
		return defaultStatsInterval
	}
	return c.inner.interval
}

// GetProfile returns the current statistics profile.
func (c *StatsControl) GetProfile() StatsProfile {
	if c == nil || c.inner == nil {
		return StatsProfileNone
	}
	c.inner.mu.RLock()
	defer c.inner.mu.RUnlock()
	return c.inner.profile
}

// SetProfile updates the current statistics profile.
//
// If the profile changes, the current partial interval is emitted using the old
// profile before the new profile takes effect. SetProfile returns an
// IllegalState error after the owning Client has been closed.
//
// Changing the profile does not modify the collection gate. If IsStarted is
// already true, switching from StatsProfileNone to another profile resumes
// observation immediately; otherwise call Start to enable collection.
func (c *StatsControl) SetProfile(profile StatsProfile) error {
	if c == nil || c.inner == nil {
		return nil
	}

	normalized, err := profile.normalized()
	if err != nil {
		return err
	}

	c.inner.emitMu.Lock()
	c.inner.mu.Lock()
	if c.inner.closed {
		c.inner.mu.Unlock()
		c.inner.emitMu.Unlock()
		return nosqlerr.NewIllegalState("statistics control is closed")
	}
	if c.inner.profile == normalized {
		c.inner.mu.Unlock()
		c.inner.emitMu.Unlock()
		return nil
	}

	now := time.Now().UTC()
	state := c.takeSnapshotStateLocked(now)
	if state == nil {
		c.resetStatsLocked(now)
	}
	c.inner.profile = normalized
	if normalized != StatsProfileNone && c.inner.enableLog &&
		(c.inner.logger == nil || c.inner.logger == logger.DefaultLogger) {
		c.inner.logger = logger.New(os.Stderr, logger.Info, false)
	}
	enabled := c.inner.started && normalized != StatsProfileNone
	atomic.StoreUint32(&c.inner.enabled, boolToUint32(enabled))
	c.inner.mu.Unlock()
	snapshot := statsSnapshotFromState(state)
	delivery := c.prepareSnapshotDelivery(snapshot, false)
	c.inner.emitMu.Unlock()
	if delivery != nil {
		c.invokeStatsHandler(delivery)
	}

	if enabled {
		c.startEmitter()
	}
	return nil
}

func (c *StatsControl) resetStatsLocked(intervalStart time.Time) {
	c.inner.intervalStart = intervalStart
	c.inner.requestStats = make(map[string]*requestLifecycleStats)
	c.inner.queryStats = make(map[string]*queryEntryStats)
}

// GetPrettyPrint returns whether emitted statistics JSON should be pretty-printed.
func (c *StatsControl) GetPrettyPrint() bool {
	if c == nil || c.inner == nil {
		return false
	}
	c.inner.mu.RLock()
	defer c.inner.mu.RUnlock()
	return c.inner.prettyPrint
}

// SetPrettyPrint updates whether emitted statistics JSON should be pretty-printed.
func (c *StatsControl) SetPrettyPrint(prettyPrint bool) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()
	c.inner.prettyPrint = prettyPrint
}

// GetStatsHandler returns the configured statistics handler, if any.
func (c *StatsControl) GetStatsHandler() StatsHandler {
	if c == nil || c.inner == nil {
		return nil
	}
	c.inner.mu.RLock()
	defer c.inner.mu.RUnlock()
	return c.inner.handler
}

// SetStatsHandler updates the statistics handler.
func (c *StatsControl) SetStatsHandler(handler StatsHandler) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()
	c.inner.handler = handler
}

// Start enables the runtime statistics collection gate.
//
// Observations still no-op when the profile is StatsProfileNone. Start is a
// no-op after the owning Client has been closed.
func (c *StatsControl) Start() {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.mu.Lock()
	if c.inner.closed {
		c.inner.mu.Unlock()
		return
	}
	c.inner.started = true
	profile := c.inner.profile
	enabled := profile != StatsProfileNone
	atomic.StoreUint32(&c.inner.enabled, boolToUint32(enabled))
	c.inner.mu.Unlock()

	if enabled {
		c.startEmitter()
	}
}

// Stop disables the runtime statistics collection gate.
func (c *StatsControl) Stop() {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()
	if c.inner.closed {
		return
	}
	c.inner.started = false
	atomic.StoreUint32(&c.inner.enabled, 0)
}

// IsStarted returns whether the runtime statistics collection gate is enabled.
func (c *StatsControl) IsStarted() bool {
	if c == nil || c.inner == nil {
		return false
	}
	c.inner.mu.RLock()
	defer c.inner.mu.RUnlock()
	return c.inner.started
}

func (c *StatsControl) isEnabled() bool {
	return c != nil && c.inner != nil && atomic.LoadUint32(&c.inner.enabled) != 0
}

func (c *StatsControl) queryStatsEnabled() bool {
	if !c.isEnabled() {
		return false
	}
	c.inner.mu.RLock()
	defer c.inner.mu.RUnlock()
	return c.inner.profile == StatsProfileAll
}

func (c *StatsControl) observe(observation statsObservation) {
	if !c.isEnabled() {
		return
	}

	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()
	if c.inner.closed || !c.inner.started || c.inner.profile == StatsProfileNone {
		return
	}

	percentileMode := activePercentileMode(c.inner.profile, c.inner.percentileMode)
	stats := c.inner.requestStats[observation.metadata.requestName]
	if stats == nil {
		stats = &requestLifecycleStats{}
		c.inner.requestStats[observation.metadata.requestName] = stats
	}
	stats.observe(observation, percentileMode)

	if c.inner.profile == StatsProfileAll &&
		observation.metadata.query != nil &&
		observation.metadata.query.query != "" {
		entry := c.inner.queryStats[observation.metadata.query.query]
		if entry == nil {
			entry = newQueryEntryStats(*observation.metadata.query)
			c.inner.queryStats[observation.metadata.query.query] = entry
		}
		entry.observeRequest(observation, percentileMode)
	}
}

func (c *StatsControl) observeQuery(metadata queryStatsMetadata) {
	if !c.queryStatsEnabled() {
		return
	}
	if metadata.query == "" {
		return
	}

	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()
	if c.inner.closed || !c.inner.started || c.inner.profile != StatsProfileAll {
		return
	}

	entry := c.inner.queryStats[metadata.query]
	if entry == nil {
		entry = newQueryEntryStats(metadata)
		c.inner.queryStats[metadata.query] = entry
	}
	entry.observeLogical(metadata)
}

func (c *StatsControl) observeQueryMetadata(metadata queryStatsMetadata) {
	if !c.queryStatsEnabled() {
		return
	}
	if metadata.query == "" {
		return
	}

	c.inner.mu.Lock()
	defer c.inner.mu.Unlock()
	if c.inner.closed || !c.inner.started || c.inner.profile != StatsProfileAll {
		return
	}

	entry := c.inner.queryStats[metadata.query]
	if entry == nil {
		entry = newQueryEntryStats(metadata)
		c.inner.queryStats[metadata.query] = entry
	}
	entry.updateResponseMetadata(metadata)
}

type statsSnapshotState struct {
	clientID       string
	profile        StatsProfile
	percentileMode StatsPercentileMode
	prettyPrint    bool
	startTime      time.Time
	endTime        time.Time
	requestStats   map[string]*requestLifecycleStats
	queryStats     map[string]*queryEntryStats
}

func (c *StatsControl) takeSnapshotStateLocked(endTime time.Time) *statsSnapshotState {
	if c.inner.profile == StatsProfileNone {
		return nil
	}

	state := &statsSnapshotState{
		clientID:       c.inner.clientID,
		profile:        c.inner.profile,
		percentileMode: activePercentileMode(c.inner.profile, c.inner.percentileMode),
		prettyPrint:    c.inner.prettyPrint,
		startTime:      c.inner.intervalStart,
		endTime:        endTime,
		requestStats:   c.inner.requestStats,
		queryStats:     c.inner.queryStats,
	}
	c.resetStatsLocked(endTime)
	return state
}

func statsSnapshotFromState(state *statsSnapshotState) *StatsSnapshot {
	if state == nil {
		return nil
	}

	payload := intervalSnapshotPayload(
		state.clientID,
		state.profile,
		state.percentileMode,
		state.startTime,
		state.endTime,
		state.requestStats,
		state.queryStats,
	)
	var data []byte
	var err error
	if state.prettyPrint {
		data, err = json.MarshalIndent(payload, "", "  ")
	} else {
		data, err = json.Marshal(payload)
	}
	if err != nil {
		return newStatsSnapshot("{}")
	}

	return newStatsSnapshot(string(data))
}

func (c *StatsControl) snapshotAndReset() *StatsSnapshot {
	if c == nil || c.inner == nil {
		return nil
	}

	endTime := time.Now().UTC()

	c.inner.mu.Lock()
	state := c.takeSnapshotStateLocked(endTime)
	c.inner.mu.Unlock()
	return statsSnapshotFromState(state)
}

type statsRequestMetadata struct {
	requestName string
	query       *queryStatsMetadata
}

type statsObservation struct {
	metadata           statsRequestMetadata
	requestSize        int
	responseSize       int
	latency            time.Duration
	retryCount         int
	retryDelayMs       int64
	authRetryCount     int
	throttleRetryCount int
	rateLimitDelayMs   int64
	hasError           bool
}

func newStatsSuccess(
	metadata statsRequestMetadata,
	requestSize int,
	responseSize int,
	latency time.Duration,
	retryCount int,
	retryDelay time.Duration,
	authRetryCount int,
	throttleRetryCount int,
	rateLimitDelay time.Duration,
) statsObservation {
	return statsObservation{
		metadata:           metadata,
		requestSize:        requestSize,
		responseSize:       responseSize,
		latency:            latency,
		retryCount:         retryCount,
		retryDelayMs:       durationMillis(retryDelay),
		authRetryCount:     authRetryCount,
		throttleRetryCount: throttleRetryCount,
		rateLimitDelayMs:   durationMillis(rateLimitDelay),
	}
}

func newStatsError(
	metadata statsRequestMetadata,
	requestSize int,
	responseSize int,
	retryCount int,
	retryDelay time.Duration,
	authRetryCount int,
	throttleRetryCount int,
	rateLimitDelay time.Duration,
) statsObservation {
	return statsObservation{
		metadata:           metadata,
		requestSize:        requestSize,
		responseSize:       responseSize,
		retryCount:         retryCount,
		retryDelayMs:       durationMillis(retryDelay),
		authRetryCount:     authRetryCount,
		throttleRetryCount: throttleRetryCount,
		rateLimitDelayMs:   durationMillis(rateLimitDelay),
		hasError:           true,
	}
}

type requestLifecycleStats struct {
	successCount       uint64
	errorCount         uint64
	latency            durationSummary
	requestSize        sizeSummary
	responseSize       sizeSummary
	retryCount         uint64
	retryDelayMs       int64
	authRetryCount     uint64
	throttleRetryCount uint64
	rateLimitDelayMs   int64
}

func (s *requestLifecycleStats) httpRequestCount() uint64 {
	return s.successCount + s.errorCount
}

func (s *requestLifecycleStats) observe(observation statsObservation, percentileMode StatsPercentileMode) {
	if observation.hasError {
		s.errorCount++
	} else {
		s.successCount++
		s.latency.observe(observation.latency, percentileMode)
		s.requestSize.observe(observation.requestSize)
		s.responseSize.observe(observation.responseSize)
	}

	s.retryCount += uint64(observation.retryCount)
	s.retryDelayMs += observation.retryDelayMs
	s.authRetryCount += uint64(observation.authRetryCount)
	s.throttleRetryCount += uint64(observation.throttleRetryCount)
	s.rateLimitDelayMs += observation.rateLimitDelayMs
}

type queryStatsMetadata struct {
	query      string
	unprepared bool
	simple     bool
	doesWrites bool
	plan       string
}

type queryEntryStats struct {
	count        uint64
	unprepared   uint64
	simple       bool
	doesWrites   bool
	plan         string
	requestStats requestLifecycleStats
}

func newQueryEntryStats(metadata queryStatsMetadata) *queryEntryStats {
	entry := &queryEntryStats{}
	entry.updateResponseMetadata(metadata)
	return entry
}

func (s *queryEntryStats) observeLogical(metadata queryStatsMetadata) {
	s.count++
	if metadata.unprepared {
		s.unprepared++
	} else if metadata.simple {
		s.simple = true
	}
	s.updateResponseMetadata(metadata)
}

func (s *queryEntryStats) observeRequest(observation statsObservation, percentileMode StatsPercentileMode) {
	s.requestStats.observe(observation, percentileMode)
	if observation.metadata.query != nil {
		s.updateResponseMetadata(*observation.metadata.query)
	}
}

func (s *queryEntryStats) updateResponseMetadata(metadata queryStatsMetadata) {
	if metadata.doesWrites {
		s.doesWrites = true
	}
	if s.plan == "" && metadata.plan != "" {
		s.plan = metadata.plan
	}
}

type durationSummary struct {
	count     uint64
	sumMs     int64
	minMs     int64
	maxMs     int64
	samples   []int64
	histogram *latencyHistogram
}

func (s *durationSummary) observe(duration time.Duration, percentileMode StatsPercentileMode) {
	ms := durationMillis(duration)
	if s.count == 0 || ms < s.minMs {
		s.minMs = ms
	}
	if s.count == 0 || ms > s.maxMs {
		s.maxMs = ms
	}
	s.count++
	s.sumMs += ms
	switch percentileMode {
	case StatsPercentileExact:
		s.samples = append(s.samples, ms)
	case StatsPercentileHDR:
		if s.histogram == nil {
			s.histogram = &latencyHistogram{}
		}
		s.histogram.observe(ms)
	}
}

func (s *durationSummary) avgMs() float64 {
	if s.count == 0 {
		return 0
	}
	return float64(s.sumMs) / float64(s.count)
}

func (s *durationSummary) percentilesMs(percentileMode StatsPercentileMode) (int64, int64) {
	if s.count == 0 {
		return 0, 0
	}

	if percentileMode == StatsPercentileHDR {
		if s.histogram == nil {
			return 0, 0
		}
		return s.clampPercentile(s.histogram.percentileMs(0.95)),
			s.clampPercentile(s.histogram.percentileMs(0.99))
	}

	if len(s.samples) == 0 {
		return 0, 0
	}
	sort.Slice(s.samples, func(i, j int) bool {
		return s.samples[i] < s.samples[j]
	})
	return s.clampPercentile(exactPercentileMs(s.samples, 0.95)),
		s.clampPercentile(exactPercentileMs(s.samples, 0.99))
}

func exactPercentileMs(sortedSamples []int64, percentile float64) int64 {
	// Match the zero-based rank selection used by the Java and Rust SDKs.
	index := int(math.Round(percentile*float64(len(sortedSamples)) - 1))
	if index < 0 {
		index = 0
	}
	if index >= len(sortedSamples) {
		index = len(sortedSamples) - 1
	}
	return sortedSamples[index]
}

func (s *durationSummary) clampPercentile(value int64) int64 {
	if value < s.minMs {
		return s.minMs
	}
	if value > s.maxMs {
		return s.maxMs
	}
	return value
}

const (
	latencyHistogramExactBuckets    = 1024
	latencyHistogramBucketsPerPower = 16
	latencyHistogramMinPower        = 10
	latencyHistogramMaxPower        = 62
	latencyHistogramBucketCount     = latencyHistogramExactBuckets +
		(latencyHistogramMaxPower-latencyHistogramMinPower+1)*latencyHistogramBucketsPerPower
)

type latencyHistogram struct {
	count   uint64
	buckets [latencyHistogramBucketCount]uint64
}

func (h *latencyHistogram) observe(ms int64) {
	h.count++
	h.buckets[latencyHistogramBucketIndex(ms)]++
}

func (h *latencyHistogram) percentileMs(percentile float64) int64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(percentile*float64(h.count) + 0.999999)
	if target < 1 {
		target = 1
	}
	var seen uint64
	for index, count := range h.buckets {
		seen += count
		if seen >= target {
			return latencyHistogramBucketValue(index)
		}
	}
	return latencyHistogramBucketValue(len(h.buckets) - 1)
}

func latencyHistogramBucketIndex(ms int64) int {
	if ms <= 0 {
		return 0
	}
	if ms < latencyHistogramExactBuckets {
		return int(ms)
	}

	power := bits.Len64(uint64(ms)) - 1
	if power < latencyHistogramMinPower {
		return int(ms)
	}
	if power > latencyHistogramMaxPower {
		return latencyHistogramBucketCount - 1
	}

	widthShift := power - 4
	bucketOffset := int((uint64(ms) - (uint64(1) << uint(power))) >> uint(widthShift))
	if bucketOffset >= latencyHistogramBucketsPerPower {
		bucketOffset = latencyHistogramBucketsPerPower - 1
	}
	return latencyHistogramExactBuckets +
		(power-latencyHistogramMinPower)*latencyHistogramBucketsPerPower +
		bucketOffset
}

func latencyHistogramBucketValue(index int) int64 {
	if index < latencyHistogramExactBuckets {
		return int64(index)
	}

	offset := index - latencyHistogramExactBuckets
	power := latencyHistogramMinPower + offset/latencyHistogramBucketsPerPower
	subBucket := offset % latencyHistogramBucketsPerPower
	width := uint64(1) << uint(power-4)
	upper := (uint64(1) << uint(power)) + uint64(subBucket+1)*width - 1
	const maxInt64 = uint64(1<<63 - 1)
	if upper > maxInt64 {
		return int64(maxInt64)
	}
	return int64(upper)
}

type sizeSummary struct {
	count uint64
	sum   int64
	min   int64
	max   int64
}

func (s *sizeSummary) observe(size int) {
	value := int64(size)
	if s.count == 0 || value < s.min {
		s.min = value
	}
	if s.count == 0 || value > s.max {
		s.max = value
	}
	s.count++
	s.sum += value
}

func (s *sizeSummary) avg() float64 {
	if s.count == 0 {
		return 0
	}
	return float64(s.sum) / float64(s.count)
}

type statsSnapshotJSON struct {
	ClientID  string        `json:"clientId"`
	StartTime string        `json:"startTime"`
	EndTime   string        `json:"endTime"`
	Requests  []requestJSON `json:"requests"`
	Queries   []queryJSON   `json:"queries,omitempty"`
}

type requestJSON struct {
	HTTPRequestCount     uint64              `json:"httpRequestCount"`
	ResultSize           *sizeSummaryJSON    `json:"resultSize,omitempty"`
	Name                 string              `json:"name"`
	HTTPRequestLatencyMs *latencySummaryJSON `json:"httpRequestLatencyMs,omitempty"`
	RequestSize          *sizeSummaryJSON    `json:"requestSize,omitempty"`
	RateLimitDelayMs     int64               `json:"rateLimitDelayMs"`
	Errors               uint64              `json:"errors"`
	Retry                retryJSON           `json:"retry"`
}

type queryJSON struct {
	DoesWrites           bool                `json:"doesWrites"`
	Unprepared           uint64              `json:"unprepared"`
	HTTPRequestCount     uint64              `json:"httpRequestCount"`
	Query                string              `json:"query"`
	ResultSize           *sizeSummaryJSON    `json:"resultSize,omitempty"`
	Count                uint64              `json:"count"`
	Simple               bool                `json:"simple"`
	HTTPRequestLatencyMs *latencySummaryJSON `json:"httpRequestLatencyMs,omitempty"`
	RequestSize          *sizeSummaryJSON    `json:"requestSize,omitempty"`
	Plan                 string              `json:"plan,omitempty"`
	RateLimitDelayMs     int64               `json:"rateLimitDelayMs"`
	Errors               uint64              `json:"errors"`
	Retry                retryJSON           `json:"retry"`
}

type sizeSummaryJSON struct {
	Min int64   `json:"min"`
	Avg float64 `json:"avg"`
	Max int64   `json:"max"`
}

type latencySummaryJSON struct {
	Min int64   `json:"min"`
	Avg float64 `json:"avg"`
	Max int64   `json:"max"`
	P95 *int64  `json:"95th,omitempty"`
	P99 *int64  `json:"99th,omitempty"`
}

type retryJSON struct {
	DelayMs       int64  `json:"delayMs"`
	AuthCount     uint64 `json:"authCount"`
	ThrottleCount uint64 `json:"throttleCount"`
	Count         uint64 `json:"count"`
}

type statsInitJSON struct {
	SDKName             string `json:"sdkName"`
	SDKVersion          string `json:"sdkVersion"`
	ClientID            string `json:"clientId"`
	Profile             string `json:"profile"`
	IntervalSec         int64  `json:"intervalSec"`
	PrettyPrint         bool   `json:"prettyPrint"`
	PercentileMode      string `json:"percentileMode"`
	RateLimitingEnabled bool   `json:"rateLimitingEnabled"`
}

func intervalSnapshotPayload(
	clientID string,
	profile StatsProfile,
	percentileMode StatsPercentileMode,
	startTime time.Time,
	endTime time.Time,
	requestStats map[string]*requestLifecycleStats,
	queryStats map[string]*queryEntryStats,
) statsSnapshotJSON {
	requests := make([]requestJSON, 0, len(requestStats))
	names := make([]string, 0, len(requestStats))
	for name := range requestStats {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		leftRank := requestOutputRank(names[i])
		rightRank := requestOutputRank(names[j])
		if leftRank == rightRank {
			return names[i] < names[j]
		}
		return leftRank < rightRank
	})

	for _, name := range names {
		requests = append(requests, requestStatsPayload(name, requestStats[name], profile, percentileMode))
	}

	var queries []queryJSON
	if profile == StatsProfileAll && len(queryStats) > 0 {
		queryTexts := make([]string, 0, len(queryStats))
		for query := range queryStats {
			queryTexts = append(queryTexts, query)
		}
		sort.Strings(queryTexts)
		queries = make([]queryJSON, 0, len(queryTexts))
		for _, query := range queryTexts {
			queries = append(queries, queryStatsPayload(query, queryStats[query], profile, percentileMode))
		}
	}

	return statsSnapshotJSON{
		ClientID:  clientID,
		StartTime: formatStatsTime(startTime),
		EndTime:   formatStatsTime(endTime),
		Requests:  requests,
		Queries:   queries,
	}
}

func requestStatsPayload(name string, stats *requestLifecycleStats, profile StatsProfile, percentileMode StatsPercentileMode) requestJSON {
	request := requestJSON{
		HTTPRequestCount: stats.httpRequestCount(),
		Name:             name,
		RateLimitDelayMs: stats.rateLimitDelayMs,
		Errors:           stats.errorCount,
		Retry: retryJSON{
			DelayMs:       stats.retryDelayMs,
			AuthCount:     stats.authRetryCount,
			ThrottleCount: stats.throttleRetryCount,
			Count:         stats.retryCount,
		},
	}

	if stats.responseSize.count > 0 && stats.responseSize.max > 0 {
		request.ResultSize = &sizeSummaryJSON{
			Min: stats.responseSize.min,
			Avg: stats.responseSize.avg(),
			Max: stats.responseSize.max,
		}
	}

	if stats.latency.count > 0 {
		latency := &latencySummaryJSON{
			Min: stats.latency.minMs,
			Avg: stats.latency.avgMs(),
			Max: stats.latency.maxMs,
		}
		if profile.includesPercentiles() {
			p95, p99 := stats.latency.percentilesMs(percentileMode)
			latency.P95 = &p95
			latency.P99 = &p99
		}
		request.HTTPRequestLatencyMs = latency
	}

	if stats.requestSize.count > 0 && stats.requestSize.max > 0 {
		request.RequestSize = &sizeSummaryJSON{
			Min: stats.requestSize.min,
			Avg: stats.requestSize.avg(),
			Max: stats.requestSize.max,
		}
	}

	return request
}

func queryStatsPayload(query string, stats *queryEntryStats, profile StatsProfile, percentileMode StatsPercentileMode) queryJSON {
	requestStats := &stats.requestStats
	entry := queryJSON{
		DoesWrites:       stats.doesWrites,
		Unprepared:       stats.unprepared,
		HTTPRequestCount: requestStats.httpRequestCount(),
		Query:            query,
		Count:            stats.count,
		Simple:           stats.simple,
		Plan:             stats.plan,
		RateLimitDelayMs: requestStats.rateLimitDelayMs,
		Errors:           requestStats.errorCount,
		Retry: retryJSON{
			DelayMs:       requestStats.retryDelayMs,
			AuthCount:     requestStats.authRetryCount,
			ThrottleCount: requestStats.throttleRetryCount,
			Count:         requestStats.retryCount,
		},
	}

	if requestStats.responseSize.count > 0 && requestStats.responseSize.max > 0 {
		entry.ResultSize = &sizeSummaryJSON{
			Min: requestStats.responseSize.min,
			Avg: requestStats.responseSize.avg(),
			Max: requestStats.responseSize.max,
		}
	}

	if requestStats.latency.count > 0 {
		latency := &latencySummaryJSON{
			Min: requestStats.latency.minMs,
			Avg: requestStats.latency.avgMs(),
			Max: requestStats.latency.maxMs,
		}
		if profile.includesPercentiles() {
			p95, p99 := requestStats.latency.percentilesMs(percentileMode)
			latency.P95 = &p95
			latency.P99 = &p99
		}
		entry.HTTPRequestLatencyMs = latency
	}

	if requestStats.requestSize.count > 0 && requestStats.requestSize.max > 0 {
		entry.RequestSize = &sizeSummaryJSON{
			Min: requestStats.requestSize.min,
			Avg: requestStats.requestSize.avg(),
			Max: requestStats.requestSize.max,
		}
	}

	return entry
}

func statsRequestMetadataForRequest(req Request) statsRequestMetadata {
	return statsRequestMetadata{requestName: statsRequestName(req)}
}

func statsRequestName(req Request) string {
	switch req.(type) {
	case *PutRequest:
		return "Put"
	case *WriteMultipleRequest:
		return "WriteMultiple"
	case *GetRequest:
		return "Get"
	case *PrepareRequest:
		return "Prepare"
	case *MultiDeleteRequest:
		return "MultiDelete"
	case *GetTableRequest:
		return "GetTable"
	case *DeleteRequest:
		return "Delete"
	case *TableRequest, *AddReplicaRequest, *DropReplicaRequest:
		return "Table"
	case *ListTablesRequest:
		return "ListTables"
	case *GetIndexesRequest:
		return "GetIndexes"
	case *QueryRequest:
		return "Query"
	case *SystemRequest:
		return "System"
	case *SystemStatusRequest:
		return "SystemStatus"
	case *TableUsageRequest:
		return "TableUsage"
	case *ReplicaStatsRequest:
		return "ReplicaStats"
	default:
		return "Unknown"
	}
}

func isStatsThrottleError(err error) bool {
	return nosqlerr.Is(err,
		nosqlerr.ReadLimitExceeded,
		nosqlerr.WriteLimitExceeded,
		nosqlerr.SizeLimitExceeded,
		nosqlerr.OperationLimitExceeded)
}

func isStatsAuthRetry(err error) bool {
	return nosqlerr.Is(err,
		nosqlerr.SecurityInfoUnavailable,
		nosqlerr.RetryAuthentication)
}

func requestOutputRank(name string) int {
	for index, requestName := range javaRequestOutputOrder {
		if requestName == name {
			return index
		}
	}
	return len(javaRequestOutputOrder)
}

func durationMillis(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64(duration / time.Millisecond)
}

func formatStatsTime(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
}
