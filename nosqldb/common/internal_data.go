//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package common

import (
	"time"
)

// InternalRequestDataInt is used to give all requests a
// set of common internal data (rate limiters, retry stats, etc)
type InternalRequestDataInt interface {
	RateLimiterPairInt
	GetRetryTime() time.Duration
	SetRetryTime(d time.Duration)
}

// InternalRequestData is the actual struct that gets included
// in every Request type
type InternalRequestData struct {
	RateLimiterPair
	retryTime time.Duration
}

// GetRetryTime returns the current time spent in the client in retries
func (ird *InternalRequestData) GetRetryTime() time.Duration {
	return ird.retryTime
}

// SetRetryTime sets the current time spent in the client in retries
func (ird *InternalRequestData) SetRetryTime(d time.Duration) {
	ird.retryTime = d
}
