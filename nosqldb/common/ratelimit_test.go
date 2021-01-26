//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package common

import (
	"math/rand"
	"testing"
	"time"
)

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func TestDurationLogic(t *testing.T) {
	// test that a limiter with zero duration will always wait when
	// consuming multiple units
	limiter := NewSimpleRateLimiterWithDuration(100, 0)

	// first call will always work
	limiter.ConsumeUnits(10)

	// this should always delay
	delay := limiter.ConsumeUnits(10)
	if (delay/time.Millisecond) < 90 || (delay/time.Millisecond) > 110 {
		t.Fatalf("Expected limiter delay of ~100ms, got %dms", (delay / time.Millisecond))
	}

	// Even a zero-duration limiter should allow one unit to be used if
	// sufficient time has passed with no usage
	time.Sleep(time.Duration(1) * time.Second)
	if limiter.TryConsumeUnits(1) == false {
		t.Fatalf("Limiter with zero duration failed 1 unit use")
	}

	// use a default limiter: allows for up to one second
	limiter = NewSimpleRateLimiter(10)

	// first call always works
	limiter.TryConsumeUnits(1)

	// this should fail, because we haven't been around for
	// 1/10th of a second
	if limiter.TryConsumeUnits(1) == true {
		t.Fatalf("Limiter with 1sec duration did not fail immediate 1 unit use")
	}

	// wait for 1/10th of a second to go by
	time.Sleep(time.Duration(100) * time.Millisecond)

	// this should now pass
	if limiter.TryConsumeUnits(1) == false {
		t.Fatalf("Limiter with 1sec duration failed 1 unit use after 100ms")
	}

	// now sleep for a second
	time.Sleep(time.Duration(1) * time.Second)

	// this should not delay
	delay = limiter.ConsumeUnits(10)
	if delay > 0 {
		t.Fatalf("Expected limiter delay of 0ms, got %dus", (delay / time.Microsecond))
	}
}

func TestLoggingLimiter(t *testing.T) {
	// verify that if used as a logging limiter this works
	// properly. Typically a logging limiter will set a simple
	// "one unit per second" limit.
	limiter := NewSimpleRateLimiter(1)

	// simulate log entries
	numLogged := 0

	// run for 10 seconds
	endTime := currentTimeMillis() + 10000
	for currentTimeMillis() < endTime {
		if limiter.TryConsumeUnits(1) == true {
			numLogged++
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}

	if numLogged < 10 || numLogged > 12 {
		t.Fatalf("Expected around 10 log entries, got %d", numLogged)
	}
	if testing.Verbose() {
		t.Logf("got %d log messages in 10 seconds", numLogged)
	}

	// do the same thing, with calls that block indefinitely
	numLogged = 0

	// run for 10 seconds
	endTime = currentTimeMillis() + 10000
	for currentTimeMillis() < endTime {
		limiter.ConsumeUnits(1)
		numLogged++
	}

	if numLogged < 10 || numLogged > 12 {
		t.Fatalf("Expected around 10 log entries, got %d", numLogged)
	}

	if testing.Verbose() {
		t.Logf("got %d log messages in 10 seconds", numLogged)
	}
}

func TestSubUnitLimits(t *testing.T) {
	// test that limiters work with less than 1 unit per second limits
	limiter := NewSimpleRateLimiterWithDuration(0.2, 1.0)

	// internally the limiter should have a duration of 5 seconds
	durationSecs := limiter.GetDuration()
	if durationSecs <= 4.9 || durationSecs >= 5.1 {
		t.Fatalf("Expected duration duration of 5 seconds, got %.2f", durationSecs)
	}

	// wait for 3 seconds, we should not be able to consume one unit
	// without waiting
	time.Sleep(time.Duration(3) * time.Second)

	limiter.TryConsumeUnits(1) // first consume always works
	if limiter.TryConsumeUnits(1) == true {
		t.Fatalf("Consumed 1 unit after only 3 seconds " +
			"(should take 5 seconds)")
	}

	// wait another 2 seconds, we should be able to consume one unit
	// without waiting
	time.Sleep(time.Duration(2) * time.Second)

	if limiter.TryConsumeUnits(1) == false {
		t.Fatalf("Could not consume one unit after 5 seconds")
	}

	// it should take around another 5 seconds to consume another unit
	delay := limiter.ConsumeUnits(1)
	if (delay/time.Millisecond) < 4500 || (delay/time.Millisecond) > 5100 {
		t.Fatalf("Expected delay of 5000ms, actual delay=%dms", (delay / time.Millisecond))
	}
}

func TestWithoutTimeouts(t *testing.T) {
	// verify operation with no timeouts
	limiter := NewSimpleRateLimiterWithDuration(1000, 5.0)

	// reset so the wall clock time for construction is erased
	limiter.Reset()

	// sleep for 5 seconds to allow duration
	time.Sleep(time.Duration(5) * time.Second)

	// consume 2 seconds worth of units
	limiter.ConsumeUnits(2000)

	// with duration, there should be 3 seconds worth left
	if limiter.TryConsumeUnits(3000) == false {
		t.Fatalf("Unable to consume 3 seconds worth of units.\n"+
			"limiter = %s", limiter)
	}

	if testing.Verbose() {
		t.Log("Limiter consumed 5 seconds duration rate")
	}

	// consume more, this should put the limiter over its
	// limit
	limiter.ConsumeUnitsUnconditionally(500)

	// getting the rate should show ~= 100%
	rate := limiter.GetCurrentRate()
	if rate < 99.5 {
		t.Fatalf("expected limiter to be at/over limit, instead rate is %.2f"+
			"\nlimiter = %s", rate, limiter)
	}
	if testing.Verbose() {
		t.Logf("Limiter rate = %.2f", rate)
	}

	// trying to consume should fail
	if limiter.TryConsumeUnits(100) == true {
		t.Fatalf("Expected failure consuming 100 units, but got success.\n"+
			"limiter = %s", limiter)
	}
}

func TestWithTimeouts(t *testing.T) {
	// verify operation with timeouts
	limiter := NewSimpleRateLimiterWithDuration(1000, 5.0)

	// sleep for 5 seconds to allow duration
	time.Sleep(time.Duration(5) * time.Second)

	// try to consume 2 seconds worth of units. should not delay.
	delay, err := limiter.ConsumeUnitsWithTimeout(2000, (10 * time.Millisecond), false)
	if err != nil {
		t.Fatalf("Got unexpected error calling consume that should "+
			"have finished immediately: %s\nlimiter = %s", err, limiter)
	}
	if delay > 0 {
		t.Fatalf("Expected no delay for consume, instead delayed %dns. limiter = %s", delay, limiter)
	}

	// with duration, there should be 3 seconds worth left
	// since we're still under the limit, a call to consume 4 seconnds
	// worth should not delay. But the next call for even 1 unit should
	// delay about a second.
	delay, err = limiter.ConsumeUnitsWithTimeout(4000, (2000 * time.Millisecond), false)
	if err != nil {
		t.Fatalf("Got unexpected error calling consume that should "+
			"have not delayed: %s\nlimiter = %s", err, limiter)
	}
	if delay > 0 {
		t.Fatalf("Expected no delay for consume, instead delayed %dns. limiter = %s", delay, limiter)
	}

	// try to consume 1 unit. we should delay about one second
	delay, err = limiter.ConsumeUnitsWithTimeout(1000, (2000 * time.Millisecond), false)
	if err != nil {
		t.Fatalf("Got unexpected error calling consume that should "+
			"have delayed 1 second: %s\nlimiter = %s", err, limiter)
	}
	if (delay/time.Millisecond) < 900 || (delay/time.Millisecond) > 1200 {
		t.Fatalf("Expected around 1000ms delay for consume, instead "+
			"delayed %dms. limiter = %s", (delay / time.Millisecond), limiter)
	}
}

func TestLongDuration(t *testing.T) {
	runTest(t, 15, 10000, 5.0, 12000, 15000, 9000, 65000)
}

func TestIdealDuration(t *testing.T) {
	runTest(t, 15, 10000, 1.0, 10500, 12000, 9200, 22000)
}

func TestZeroDuration(t *testing.T) {
	runTest(t, 15, 10000, 0.0, 8000, 10100, 7000, 11000)
}

func TestLongDurationThreads(t *testing.T) {
	runTestWithThreads(t, 5, 15, 10000, 5.0, 9800, 20000)
}

func TestIdealDurationThreads(t *testing.T) {
	runTestWithThreads(t, 5, 15, 10000, 1.0, 9800, 11500)
}

func TestZeroDurationThreads(t *testing.T) {
	runTestWithThreads(t, 5, 15, 10000, 0.0, 8500, 10200)
}

func TestBigStarvation(t *testing.T) {
	verifyNoStarvation(t, 10, 15, 100000, 5.0)
}

func TestSmallStarvation(t *testing.T) {
	verifyNoStarvation(t, 5, 10, 100, 1.0)
}

func runTest(t *testing.T,
	testDurationSecs int32,
	perSecondLimit float64,
	durationSeconds float64,
	minAvgRate float64,
	maxAvgRate float64,
	minOneSecondRate float64,
	maxOneSecondRate float64) {

	if testing.Verbose() {
		t.Logf("runTest: time=%ds limit=%.0f durationSecs=%.1f\n",
			testDurationSecs, perSecondLimit, durationSeconds)
	}

	limiter := NewSimpleRateLimiterWithDuration(perSecondLimit, durationSeconds)

	// sleep for durationSecs so that the limiter allows for burst
	millis := int32(1000.0 * durationSeconds)
	time.Sleep(time.Duration(millis) * time.Millisecond)

	var totalUnits int64 = 0
	var unitsThisSecond int64 = 0

	startTime := currentTimeMillis()
	endTime := startTime + (int64(testDurationSecs) * 1000)

	var minObservedRate int64 = 100000000
	var maxObservedRate int64 = 0

	var delay time.Duration = 0
	var secNum int64 = 0

	for currentTimeMillis() < endTime {
		// do at least 50 consumes per second to get a decent metric
		unitsToConsume := (rand.Int63() % (int64(perSecondLimit) / 50)) + 1

		ms, err := limiter.ConsumeUnitsWithTimeout(unitsToConsume, (100 * time.Millisecond), false)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		delay += ms
		totalUnits += unitsToConsume

		unitsThisSecond += unitsToConsume
		tDiff := currentTimeMillis() - startTime
		if (tDiff - (secNum * 1000)) >= 1000 {
			if float64(unitsThisSecond) < minOneSecondRate {
				t.Fatalf("units for second %d (%d) lower than expected mimimum (%.1f)", secNum, unitsThisSecond, minOneSecondRate)
			}
			if float64(unitsThisSecond) > maxOneSecondRate {
				t.Fatalf("units for second %d (%d) greater than expected maximum (%.1f)", secNum, unitsThisSecond, maxOneSecondRate)
			}
			for (tDiff - (secNum * 1000)) >= 1000 {
				secNum++
			}
			if unitsThisSecond > maxObservedRate {
				maxObservedRate = unitsThisSecond
			}
			if unitsThisSecond < minObservedRate {
				minObservedRate = unitsThisSecond
			}
			unitsThisSecond = 0
		}
	}

	avgRate := float64(totalUnits) / float64(testDurationSecs)
	if testing.Verbose() {
		t.Logf("Average rate=%.2f, minrate=%d, maxrate=%d, delay=%dms", avgRate, minObservedRate, maxObservedRate, (delay / time.Millisecond))
	}

	if avgRate < minAvgRate {
		t.Fatalf("Average units per second (%.2f) lower than expected mimimum (%.2f)", avgRate, minAvgRate)
	}
	if avgRate > maxAvgRate {
		t.Fatalf("Average units per second (%.2f) greater than expected maximum (%.2f)", avgRate, maxAvgRate)
	}

}

func runTestWithThreads(t *testing.T,
	numThreads int,
	testDurationSecs int32,
	perSecondLimit float64,
	durationSeconds float64,
	minAvgRate float64,
	maxAvgRate float64) {

	if testing.Verbose() {
		t.Logf("runTestWithThreads: time=%ds limit=%.0f durationSecs=%.1f\n",
			testDurationSecs, perSecondLimit, durationSeconds)
	}

	limiter := NewSimpleRateLimiterWithDuration(perSecondLimit, durationSeconds)

	// sleep for durationSecs so that the limiter allows for burst
	millis := int32(1000.0 * durationSeconds)
	time.Sleep(time.Duration(millis) * time.Millisecond)

	var totalUnits int64 = 0

	startTime := currentTimeMillis()
	endTime := startTime + (int64(testDurationSecs) * 1000)

	ch := make(chan int64)

	for x := 0; x < numThreads; x++ {
		go func() {
			var units int64 = 0
			for currentTimeMillis() < endTime {
				// do at least 50 consumes per second to get a decent metric
				unitsToConsume := (rand.Int63() % (int64(perSecondLimit) / 50)) + 1
				_, err := limiter.ConsumeUnitsWithTimeout(unitsToConsume, (100 * time.Millisecond), false)
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				units += unitsToConsume
			}
			ch <- units
		}()
	}

	for x := 0; x < numThreads; x++ {
		totalUnits += <-ch
	}

	avgRate := float64(totalUnits) / float64(testDurationSecs)
	if testing.Verbose() {
		t.Logf("Average rate=%.2f", avgRate)
	}

	if avgRate < minAvgRate {
		t.Fatalf("Average units per second (%.2f) lower than expected mimimum (%.2f)", avgRate, minAvgRate)
	}
	if avgRate > maxAvgRate {
		t.Fatalf("Average units per second (%.2f) greater than expected maximum (%.2f)", avgRate, maxAvgRate)
	}
}

func verifyNoStarvation(t *testing.T,
	numThreads int,
	testDurationSecs int32,
	perSecondLimit float64,
	durationSeconds float64) {

	if testing.Verbose() {
		t.Logf("verifyNoStarvation: time=%ds limit=%.0f durationSecs=%.1f\n",
			testDurationSecs, perSecondLimit, durationSeconds)
	}

	limiter := NewSimpleRateLimiterWithDuration(perSecondLimit, durationSeconds)

	// sleep for durationSecs so that the limiter allows for burst
	millis := int32(1000.0 * durationSeconds)
	time.Sleep(time.Duration(millis) * time.Millisecond)

	var totalUnits int64 = 0

	startTime := currentTimeMillis()
	endTime := startTime + (int64(testDurationSecs) * 1000)

	ch := make(chan int64)

	// run one large consumer and many small consumers
	for x := 0; x < numThreads; x++ {
		go func() {
			var units int64 = 0
			for currentTimeMillis() < endTime {
				// large consume: take about a second
				var unitsToConsume int64 = int64(perSecondLimit)
				if x > 0 {
					// small consumes: happen quickly
					unitsToConsume = (rand.Int63() % (int64(perSecondLimit) / 100)) + 1
				}
				_, err := limiter.ConsumeUnitsWithTimeout(unitsToConsume, (2200 * time.Millisecond), false)
				if err != nil {
					t.Logf("Unexpected error: %s", err)
					units = -1
					break
				} else {
					units += unitsToConsume
				}
			}
			ch <- units
		}()
	}

	var errors int64 = 0
	for x := 0; x < numThreads; x++ {
		n := <-ch
		if n < 0 {
			errors++
		} else {
			totalUnits += n
		}
	}

	avgRate := float64(totalUnits) / float64(testDurationSecs)
	if testing.Verbose() {
		t.Logf("Average rate=%.2f", avgRate)
	}

	if errors > 0 {
		t.Fatalf("Large consumes starved by small consumes (errors=%d)", errors)
	}
}
