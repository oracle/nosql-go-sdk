//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"errors"
	"math/rand"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

// RetryHandler interface is used by the request handling system when a
// retryable error is returned. It controls the number of retries as well as
// frequency of retries using a delaying algorithm.
//
// A default RetryHandler is always configured on a Client instance and can be
// controlled or overridden using Config.RetryHandler.
//
// It is not recommended that applications rely on a RetryHandler for
// regulating provisioned throughput. It is best to add rate-limiting to the
// application based on a table's capacity and access patterns to avoid
// throttling errors.
//
// Implementations of this interface must be immutable so they can be shared.
type RetryHandler interface {
	// MaxNumRetries returns the maximum number of retries that this handler
	// instance will allow before the error is reported to the application.
	MaxNumRetries() uint

	// ShouldRetry indicates whether the request should continue to retry upon
	// receiving the specified error and having attempted the specified number
	// of retries.
	//
	// This method is used by the request handling system when a retryable error
	// is returned, to determine whether to perform a retry or not based on the
	// specified parameters.
	ShouldRetry(req Request, numRetries uint, err error) bool

	// Delay is called when a retryable error is reported. It is determined
	// that the request will be retried based on the return value of
	// ShouldRetry(). It provides a delay between retries. Most implementations
	// will sleep for some period of time. The method should not return until
	// the desired delay period has passed.
	//
	// Implementations should not busy-wait in a tight loop.
	Delay(req Request, numRetries uint, err error)
}

const securityErrorRetryInterval = 100 * time.Millisecond

// DefaultRetryHandler represents the default implementation of RetryHandler interface.
type DefaultRetryHandler struct {
	maxNumRetries uint
	retryInterval time.Duration
}

// NewDefaultRetryHandler creates a DefaultRetryHandler with the specified
// maximum number of retries and retry interval. The retry interval must be
// greater than or equal to 1 millisecond.
func NewDefaultRetryHandler(maxNumRetries uint, retryInterval time.Duration) (*DefaultRetryHandler, error) {
	if retryInterval < time.Millisecond {
		return nil, errors.New("retry interval must be greater than or equal to 1 millisecond")
	}

	return &DefaultRetryHandler{
		maxNumRetries: maxNumRetries,
		retryInterval: retryInterval,
	}, nil
}

// MaxNumRetries returns the maximum number of retries that this handler
// will allow before the error is reported to the application.
func (r DefaultRetryHandler) MaxNumRetries() uint {
	return r.maxNumRetries
}

// Delay causes the current goroutine to pause for a peroid of time.
// It is called when a retryable error is reported. It is determined that the
// request will be retried based on the return value of ShouldRetry().
//
// If a non-zero retryInterval is configured for the retry handler, this method
// uses retryInterval. Otherwise, it uses an exponential backoff algorithm to
// compute the time of delay.
//
// If the reported retryable error is SecurityInfoUnavailable, it pauses for
// securityErrorRetryInterval period of time when the number of retries is
// less than or equal to 10. Otherwise, it uses the exponential backoff algorithm
// to compute the time of delay.
func (r DefaultRetryHandler) Delay(req Request, numRetries uint, err error) {
	d := r.retryInterval
	if nosqlerr.IsSecurityInfoUnavailable(err) {
		d = securityInfoNotReadyDelay(numRetries)
	} else if d <= 0 {
		d = computeBackoffDelay(numRetries, time.Second)
	}

	time.Sleep(d)
}

// ShouldRetry reports whether the request should continue to retry upon
// receiving the specified error and having attempted the specified number
// of retries.
//
// The default behavior is to NOT retry OperationThrottlingError because the
// retry time is likely much longer than normal because they are DDL operations.
// In addition, NOT retry any requests that should not be retried, including
// TableRequest, ListTablesRequest, GetTableRequest, TableUsageRequest and
// GetIndexesRequest.
//
// Always retry SecurityInfoUnavailable error until exceed the request timeout.
// It's not restrained by the maximum retries configured for this handler, the
// driver with retry handler with 0 retry setting would still retry the request
// upon receiving this error.
func (r DefaultRetryHandler) ShouldRetry(req Request, numRetries uint, err error) bool {
	if err, ok := err.(*nosqlerr.Error); ok {
		if err.Code == nosqlerr.OperationLimitExceeded {
			return false
		}
		if err.Code == nosqlerr.SecurityInfoUnavailable {
			// Always retry if security info is not ready.
			return true
		}
	}

	if !req.shouldRetry() {
		return false
	}

	return numRetries < r.maxNumRetries
}

// Use an exponential backoff algorithm to compute time of delay.
//
// Assumption: numRetries starts with 1
// DelayMS = 2^(numRetries-1) + random MS (0-1000)
func computeBackoffDelay(numRetries uint, baseDelay time.Duration) time.Duration {
	if numRetries < 1 {
		return baseDelay
	}
	d := (1 << (numRetries - 1)) * baseDelay
	d += (time.Duration(rand.Intn(1000)) * time.Millisecond)
	return d
}

// Handle security information not ready retries. If number of retries
// is less than or equal to 10, delay for securityErrorRetryInterval.
// Otherwise, use the backoff algorithm to compute the time of delay.
func securityInfoNotReadyDelay(numRetries uint) time.Duration {
	if numRetries <= 10 {
		return securityErrorRetryInterval
	}
	return computeBackoffDelay(numRetries-10, securityErrorRetryInterval)
}
