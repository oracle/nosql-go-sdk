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
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

func TestNewDefaultRetryHandler(t *testing.T) {
	e1 := errors.New("retry interval must be greater than or equal to 1 millisecond")
	tests := []struct {
		retries  uint
		interval time.Duration
		wantErr  error
	}{
		{0, time.Millisecond - 1, e1},
		{0, time.Millisecond, nil},
		{9, time.Second, nil},
	}

	for _, r := range tests {
		h, err := NewDefaultRetryHandler(r.retries, r.interval)
		if !equalError(err, r.wantErr) {
			t.Errorf("NewDefaultRetryHandler(%d, %s) got error: %v; want error: %v",
				r.retries, r.interval, err, r.wantErr)
		}
		if err == nil && h.MaxNumRetries() != r.retries {
			t.Errorf("MaxNumRetries() got %d; want %d", h.MaxNumRetries(), r.retries)
		}
	}
}

func TestComputeBackoffDelay(t *testing.T) {
	tests := []struct {
		numRetries uint
		baseDelay  time.Duration
		wantDelay  time.Duration
	}{
		{0, time.Second, time.Second},
		{1, time.Second, time.Second},
		{2, time.Second, 2 * time.Second},
		{3, time.Second, 4 * time.Second},
		{10, time.Second, 512 * time.Second},
	}

	for _, r := range tests {
		d := computeBackoffDelay(r.numRetries, r.baseDelay)
		if d < r.wantDelay {
			t.Errorf("computeBackoffDelay(%d, %v) got %v; want at least %v",
				r.numRetries, r.baseDelay, d, r.wantDelay)
		}
	}
}

func TestSecurityInfoNotReadyDelay(t *testing.T) {
	baseDelay := securityErrorRetryInterval
	tests := []struct {
		numRetries uint
		wantDelay  time.Duration
	}{
		{0, baseDelay},
		{1, baseDelay},
		{10, baseDelay},
		{11, baseDelay},
		{12, 2 * baseDelay},
		{13, 4 * baseDelay},
		{20, 512 * baseDelay},
	}

	for _, r := range tests {
		d := securityInfoNotReadyDelay(r.numRetries)
		if d < r.wantDelay {
			t.Errorf("securityInfoNotReadyDelay(%d) got %v; want at least %v",
				r.numRetries, d, r.wantDelay)
		}
	}
}

func TestShouldRetry(t *testing.T) {
	// retryable request
	prepareReq := &PrepareRequest{
		Statement: "select id from T1",
	}
	// not-retryable request
	listTableReq := &ListTablesRequest{}

	errSecurityInfoUnavailable := &nosqlerr.Error{
		Code: nosqlerr.SecurityInfoUnavailable,
	}
	errOpLimitExceeded := &nosqlerr.Error{
		Code: nosqlerr.OperationLimitExceeded,
	}
	errServiceUnavailable := &nosqlerr.Error{
		Code: nosqlerr.ServiceUnavailable,
	}

	tests := []struct {
		req        Request
		numRetried uint // the number of retried operations
		maxRetries uint // max number of retries for the retry handler
		err        error
		want       bool
	}{
		{prepareReq, 0, 3, errSecurityInfoUnavailable, true},
		{prepareReq, 1, 3, errOpLimitExceeded, false},
		{prepareReq, 0, 3, errServiceUnavailable, true},
		{prepareReq, 3, 3, errServiceUnavailable, false},
		{prepareReq, 4, 3, errServiceUnavailable, false},
		{listTableReq, 0, 3, errSecurityInfoUnavailable, true},
		{listTableReq, 1, 3, errOpLimitExceeded, false},
		{listTableReq, 0, 3, errServiceUnavailable, false},
		{listTableReq, 3, 3, errServiceUnavailable, false},
		{listTableReq, 4, 3, errServiceUnavailable, false},
	}

	for i, r := range tests {
		h, _ := NewDefaultRetryHandler(r.maxRetries, time.Second)
		b := h.ShouldRetry(r.req, r.numRetried, r.err)
		if b != r.want {
			t.Errorf("Test %d: ShouldRetry(req=%#v, numRetried=%d, maxRetries=%d, err=%s) got %t; want %t",
				i+1, r.req, r.numRetried, r.maxRetries, r.err, b, r.want)
		}
	}
}
