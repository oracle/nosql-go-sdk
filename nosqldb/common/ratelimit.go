//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package common

import (
	"fmt"
	"sync"
	"time"
)

// RateLimiterPairInt interface is used in serializer.go
// to make all Requests include a rate limiter pair
type RateLimiterPairInt interface {
	GetReadRateLimiter() RateLimiter
	GetWriteRateLimiter() RateLimiter
	SetReadRateLimiter(rl RateLimiter)
	SetWriteRateLimiter(rl RateLimiter)
}

// RateLimiterPair is the actual struct that gets included
// in every Request type
type RateLimiterPair struct {
	ReadLimiter  RateLimiter
	WriteLimiter RateLimiter
}

// GetReadRateLimiter returns the read limiter for a request
// note this may be nil
func (rlp *RateLimiterPair) GetReadRateLimiter() RateLimiter {
	return rlp.ReadLimiter
}

// GetWriteRateLimiter returns the write limiter for a request
// note this may be nil
func (rlp *RateLimiterPair) GetWriteRateLimiter() RateLimiter {
	return rlp.WriteLimiter
}

// SetReadRateLimiter sets a read rate limiter instance to use during
// request execution.
func (rlp *RateLimiterPair) SetReadRateLimiter(rl RateLimiter) {
	rlp.ReadLimiter = rl
}

// SetWriteRateLimiter sets a write rate limiter instance to use during
// request execution.
func (rlp *RateLimiterPair) SetWriteRateLimiter(rl RateLimiter) {
	rlp.WriteLimiter = rl
}

// Comments note: Note carefully TABs are mixed with space indention in
// function comments. This is on purpose for better formatting in godoc.

// RateLimiter interface provides default methods that all rate limiters
// must implement.
//
// Thread safety
//
// It is expected that all implementing classes of this
// interface may be used by multiple goroutines concurrently. For example, many
// goroutines may be using the same rate limiter instance to ensure that
// all of them together do not exceed a given limit.
//
// Typical usage
//
// The simplest use of the rate limiter is to consume a number of units,
// blocking until they are successfully consumed:
//	delay := rateLimiter.ConsumeUnits(units)
//	// delay indicates how long the consume delayed
//
// To poll a limiter to see if it is currently over the limit:
//	if rateLimiter.TryConsumeUnits(0) {
//	  // limiter is below its limit
//	}
//
// To attempt to consume units, only if they can be immediately consumed
// without waiting:
//	if ratelimiter.TryConsumeUnits(units) {
//	  // successful consume
//	} else {
//	  // could not consume units without waiting
//	}
//
// Usages with timeouts
//
// In cases where the number of units an operation will consume is already
// known before the operation, a simple one-shot method can be used:
//	var units int64 = (known units the operation will use)
//	alwaysConsume := false // don't consume if we time out
//	delay, err := rateLimiter.ConsumeUnitsWithTimeout(units, timeout, alwaysConsume)
//	if err != nil {
//	  // we could not do the operation within the given timeframe.
//	  ...skip operation...
//	} else {
//	  // we waited <delay> for the units to be consumed, and the consume was successful
//	  ...do operation...
//	}
//
// In cases where the number of units an operation will consume is not
// known before the operation, typically two rate limiter calls would be
// used: one to wait till the limiter is below its limit, and a second
// to update the limiter with used units:
//	// wait until we're under the limit
//	delay, err := rateLimiter.ConsumeUnitsWithTimeout(0, timeout, false)
//	if err != nil {
//	  // we could not do the operation within the given timeframe.
//	  ...skip operation...
//	}
//	// we waited <delay> to be under the limit, and were successful
//	var units int64  = (...do operation, get number of units used...)
//	// update rate limiter with consumed units. Next operation on this
//	// limiter may delay due to it being over its limit.
//	rateLimiter.ConsumeUnitsUnconditionally(units)
//
// Alternately, the operation could be always performed, and then the
// limiter could try to wait for the units to be consumed:
//	var units int64  = (...do operation, get number of units used...)
//	alwaysConsume := true // consume, even if we time out
//	delay, err := rateLimiter.ConsumeUnitsWithTimeout(units, timeout, alwaysConsume)
//	if err != nil {
//	  // the rate limiter could not consume the units and go
//	  // below the limit in the given timeframe. Units are
//	  // consumed anyway, and the next call to this limiter
//	  // will likely delay
//	} else {
//	  // we waited <delay> for the units to be consumed, and the
//	  // consume was successful
//	}
//
// Limiter duration
//
// Implementing rate limiters should support a configurable "duration".
// This is sometimes referred to as a "burst mode", or a "window time",
// or "burst duration".
//
// This will allow consumes of units that were not consumed in the recent
// past. For example, if a limiter allows for 100 units per second, and is
// not used for 5 seconds, it should allow an immediate consume of 500
// units with no delay upon a consume call, assuming that the limiter's
// duration is set to at least 5 seconds.
//
// The maximum length of time for this duration should be configurable.
// In all cases a limiter should set a reasonable minimum duration, such
// that a call to TryConsumeUnits(1) has a chance of succeeding.
// It is up to the limiter implementation to determine if units from the past
// before the limiter was created or reset are available for use.
type RateLimiter interface {

	// ConsumeUnits consumes a number of units, blocking until the units are available.
	// It returns the amount of time blocked. If not blocked, 0 is returned.
	ConsumeUnits(units int64) time.Duration

	// TryConsumeUnits consumes the specified number of units if they can be returned
	// immediately without waiting.
	// It returns true if the units were consumed, false if they were not immediately available.
	// If units was zero, true return means the limiter is currently below its limit.
	// units can be zero to poll if the limiter is currently over its limit. Pass negative units to
	// "give back" units (same as calling ConsumeUnits with a negative value).
	TryConsumeUnits(units int64) bool

	// ConsumeUnitsWithTimeout attempts to consume a number of units, blocking until the units are
	// available or the specified timeout expires. It returns the amount of time blocked.
	// If not blocked, 0 is returned.
	//
	// units can be a negative value to "give back" units.
	// Pass a timeout value of 0 to block indefinitely. To poll if the limiter is
	// currently over its limit, use TryConsumeUnits() instead.
	// If alwaysConsume is true, consume units even on timeout.
	ConsumeUnitsWithTimeout(units int64, timeout time.Duration, alwaysConsume bool) (time.Duration, error)

	// GetLimitPerSecond returns the number of units configured for this rate limiter instance
	// (the max number of units per second this limiter allows).
	GetLimitPerSecond() float64

	// SetDuration sets the duration for this rate limiter instance.
	// The duration specifies how far back in time the limiter will
	// go to consume previously unused units.
	//
	// For example, if a limiter had a limit of 1000 units and a
	// duration of 5 seconds, and had no units consumed for at least
	// 5 seconds, a call to TryConsumeUnits(5000) will succeed
	// immediately with no waiting.
	SetDuration(durationSecs float64)

	// GetDuration returns the duration in seconds configured for this rate limiter instance
	GetDuration() float64

	// Reset resets the rate limiter as if it was newly constructed.
	Reset()

	// SetLimitPerSecond sets a new limit (units per second) on the limiter.
	//
	// Note that implementing rate limiters should fully support non-integer
	// (float64) values internally, to avoid issues when the limits are set
	// very low.
	// Changing the limit may lead to unexpected spiky behavior, and may
	// affect other goroutines currently operating on the same limiter instance.
	SetLimitPerSecond(rateLimitPerSecond float64)

	// ConsumeUnitsUnconditionally consumes units without checking or waiting.
	// the internal amount of units consumed will be updated by the given
	// amount, regardless of its current over/under limit state.
	// The number of units to consume may be negative to "give back" units.
	ConsumeUnitsUnconditionally(units int64)

	// GetCurrentRate returns the current rate as a percentage of current limit.
	GetCurrentRate() float64

	// SetCurrentRate sets the current rate as a percentage of current limit.
	// This modifies the internal limiter values; it does not modify
	// the rate limit.
	// rateToSet may be greater than 100.0 to set the limiter to "over its limit".
	SetCurrentRate(rateToSet float64)
}

const nanosPerSecFloat = 1000000000.0

// SimpleRateLimiter is an implementation of RateLimiter interface.
// Despite its name, it also has methods for percentage-based operations
// as well as the methods in RateLimiter. Currently the percentage-based
// methods are only used internally.
type SimpleRateLimiter struct {
	// how many nanoseconds represent one unit
	nanosPerUnit int64

	// how many seconds worth of units can we use from the past
	durationNanos int64

	// last used unit nanosecond: this is the main "value"
	lastNano int64

	// for synchronization
	mux sync.Mutex
}

// NewSimpleRateLimiter Creates a simple time-based rate limiter.
//
// This limiter will allow for one second of duration; that is, it
// will allow unused units from within the last second to be used.
func NewSimpleRateLimiter(rateLimitPerSec float64) (srl *SimpleRateLimiter) {
	return NewSimpleRateLimiterWithDuration(rateLimitPerSec, 1.0)
}

// NewSimpleRateLimiterWithDuration creates a simple time-based rate limiter
// with a specified duration.
func NewSimpleRateLimiterWithDuration(rateLimitPerSec float64, durationSecs float64) (srl *SimpleRateLimiter) {
	srl = &SimpleRateLimiter{}
	srl.SetLimitPerSecond(rateLimitPerSec)
	srl.SetDuration(durationSecs)
	srl.Reset()
	return srl
}

// SetLimitPerSecond sets a new limit (units per second) on the limiter.
// Changing the limit may lead to unexpected spiky behavior, and may
// affect other goroutines currently operating on the same limiter instance.
func (srl *SimpleRateLimiter) SetLimitPerSecond(rateLimitPerSec float64) {
	if rateLimitPerSec <= 0.0 {
		srl.nanosPerUnit = 0
	} else {
		srl.nanosPerUnit = (int64)(nanosPerSecFloat / rateLimitPerSec)
	}
	srl.enforceMinimumDuration()
}

// Ensure that any duration set will allow a consume of 1 unit to
// succeed
func (srl *SimpleRateLimiter) enforceMinimumDuration() {
	if srl.durationNanos < srl.nanosPerUnit {
		srl.durationNanos = srl.nanosPerUnit
	}
}

// GetLimitPerSecond returns the number of units configured for this rate limiter instance
// (the max number of units per second this limiter allows).
func (srl *SimpleRateLimiter) GetLimitPerSecond() float64 {
	return nanosPerSecFloat / (float64)(srl.nanosPerUnit)
}

// GetDuration returns the duration in seconds configured for this rate limiter instance
func (srl *SimpleRateLimiter) GetDuration() float64 {
	return (float64)(srl.durationNanos) / nanosPerSecFloat
}

// SetDuration sets the duration for this rate limiter instance.
// The duration specifies how far back in time the limiter will
// go to consume previously unused units.
//
// For example, if a limiter had a limit of 1000 units and a
// duration of 5 seconds, and had no units consumed for at least
// 5 seconds, a call to TryConsumeUnits(5000) will succeed
// immediately with no waiting.
func (srl *SimpleRateLimiter) SetDuration(durationSecs float64) {
	srl.durationNanos = (int64)(durationSecs * nanosPerSecFloat)
	srl.enforceMinimumDuration()
}

// Reset resets the rate limiter as if it was newly constructed.
func (srl *SimpleRateLimiter) Reset() {
	srl.lastNano = time.Now().UnixNano()
}

// SetCurrentRate sets the current rate as a percentage of current limit.
// This modifies the internal limiter values; it does not modify
// the rate limit.
// rateToSet may be greater than 100.0 to set the limiter to "over its limit".
func (srl *SimpleRateLimiter) SetCurrentRate(percent float64) {
	// Note that "rate" isn't really clearly defined in this type
	// of limiter, because there is no inherent "time period". So
	// all "rate" operations just assume "for 1 second".
	nowNanos := time.Now().UnixNano()
	if percent == 100.0 {
		srl.lastNano = nowNanos
		return
	}
	percent -= 100.0
	srl.lastNano = (nowNanos + (int64)((percent/100.0)*nanosPerSecFloat))
}

// ConsumeUnits consumes a number of units, blocking until the units are available.
// It returns the amount of time blocked. If not blocked, 0 is returned.
func (srl *SimpleRateLimiter) ConsumeUnits(units int64) time.Duration {

	// call internal logic, get the time we need to sleep to
	// complete the consume.
	// note this call immediately consumes the units
	sleepTime := srl.consumeInternal(units, 0, false, time.Now().UnixNano())

	// sleep for the requested time.
	if sleepTime > 0 {
		time.Sleep(sleepTime)
	}

	// return the amount of time slept
	return sleepTime
}

// ConsumeUnitsWithTimeout attempts to consume a number of units, blocking until the units are
// available or the specified timeout expires. It returns the amount of time blocked.
// If not blocked, 0 is returned.
//
// units can be a negative value to "give back" units.
// Pass a timeout value of 0 to block indefinitely. To poll if the limiter is
// currently over its limit, use TryConsumeUnits() instead.
// If alwaysConsume is true, consume units even on timeout.
func (srl *SimpleRateLimiter) ConsumeUnitsWithTimeout(units int64, timeout time.Duration, alwaysConsume bool) (time.Duration, error) {

	// call internal logic, get the time we need to sleep to
	// complete the consume.
	sleepTime := srl.consumeInternal(units, timeout, alwaysConsume, time.Now().UnixNano())
	if sleepTime == 0 {
		return 0, nil
	}

	// if the time required to consume is greater than our timeout,
	// sleep up to the timeout then return a timeout error.
	// Note the units may have already been consumed if alwaysConsume
	// is true.
	if timeout > 0 && sleepTime >= timeout {
		time.Sleep(timeout)
		return timeout, fmt.Errorf("timed out waiting %dms for %d units in rate limiter", (timeout / time.Millisecond), units)
	}

	// sleep for the requested time.
	time.Sleep(sleepTime)

	// return the amount of time slept
	return sleepTime, nil
}

// consumeInternal returns the time to sleep to consume units.
//
// Note this method returns immediately in all cases. It returns
// the number of milliseconds to sleep.
//
// This is the only method that actually "consumes units", i.e.
// updates the lastNano value.
//
func (srl *SimpleRateLimiter) consumeInternal(units int64, timeout time.Duration,
	alwaysConsume bool, nowNanos int64) time.Duration {

	// If disabled, just return success
	if srl.nanosPerUnit <= 0 {
		return 0
	}

	// lock so only one goroutine at a time can update
	srl.mux.Lock()
	defer srl.mux.Unlock()

	// determine how many nanos we need to add based on units requested
	nanosNeeded := units * srl.nanosPerUnit

	// ensure we never use more from the past than duration allows
	maxPast := nowNanos - srl.durationNanos
	if srl.lastNano < maxPast {
		srl.lastNano = maxPast
	}

	// compute the new "last nano used"
	newLast := srl.lastNano + nanosNeeded

	// if units < 0, we're "returning" them
	if units < 0 {
		// consume the units
		srl.lastNano = newLast
		return 0
	}

	// if we are currently under the limit, the consume
	// succeeds immediately (no sleep required).
	if srl.lastNano <= nowNanos {
		// consume the units
		srl.lastNano = newLast
		return 0
	}

	// determine the amount of time that the caller needs to sleep
	// for this limiter to go below its limit. Note that the limiter
	// is not guaranteed to be below the limit after this time, as
	// other consume calls may come in after this one and push the
	// "at the limit time" further out.
	sleepTime := time.Duration(srl.lastNano-nowNanos) * time.Nanosecond

	if alwaysConsume {
		// if we're told to always consume the units no matter what,
		// consume the units
		srl.lastNano = newLast
	} else if timeout == 0 {
		// if the timeout is zero, consume the units
		srl.lastNano = newLast
	} else if sleepTime < timeout {
		// if the sleep time is within the the given timeout, consume the units
		srl.lastNano = newLast
	}

	return sleepTime
}

// TryConsumeUnits consumes the specified number of units if they can be returned
// immediately without waiting.
// It returns true if the units were consumed, false if they were not immediately available.
// If units was zero, true return means the limiter is currently below its limit.
// units can be zero to poll if the limiter is currently over its limit. Pass negative units to
// "give back" units (same as calling ConsumeUnits with a negative value).
func (srl *SimpleRateLimiter) TryConsumeUnits(units int64) bool {
	if srl.consumeInternal(units, 1, false, time.Now().UnixNano()) == 0 {
		return true
	}
	return false
}

// GetCurrentRate returns the current rate as a percentage of current limit (0.0 - 100.0).
func (srl *SimpleRateLimiter) GetCurrentRate() float64 {
	// see comment in setCurrentRate()
	capacity := srl.getCapacity()
	limit := srl.GetLimitPerSecond()
	rate := 100.0 - ((capacity * 100.0) / limit)
	if rate < 0.0 {
		return 0.0
	}
	return rate
}

// ConsumeUnitsUnconditionally consumes units without checking or waiting.
// The internal amount of units consumed will be updated by the given
// amount, regardless of its current over/under limit state.
// The number of units to consume may be negative to "give back" units.
func (srl *SimpleRateLimiter) ConsumeUnitsUnconditionally(units int64) {
	// consume units, ignore amount of time to sleep
	srl.consumeInternal(units, 0, true, time.Now().UnixNano())
}

func (srl *SimpleRateLimiter) getCapacity() float64 {
	// ensure we never use more from the past than duration allows
	nowNanos := time.Now().UnixNano()
	maxPast := nowNanos - srl.durationNanos
	if srl.lastNano > maxPast {
		maxPast = srl.lastNano
	}
	return (float64)(nowNanos-maxPast) / (float64)(srl.nanosPerUnit)
}

func (srl *SimpleRateLimiter) String() string {
	return fmt.Sprintf("lastNano=%v, nanosPerUnit=%v, durationNanos=%v, limit=%v, capacity=%v, rate=%.2f",
		srl.lastNano, srl.nanosPerUnit, srl.durationNanos, srl.GetLimitPerSecond(), srl.getCapacity(), srl.GetCurrentRate())
}
