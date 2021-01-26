//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteErrorHandling(t *testing.T) {
	client, err := newMockClient()
	require.NoErrorf(t, err, "failed to create client, got error %v.", err)
	// GetRequest is a retryable request.
	getReq := &GetRequest{
		TableName: "T1",
		Key:       types.NewMapValue(map[string]interface{}{"id": 1}),
	}
	// ListTablesRequest is a non-retryable request.
	listTablesReq := &ListTablesRequest{Limit: 4}

	// Create and assign a mock executor to client.
	mockExec := &mockExecutor{
		errChan: make(chan error),
	}
	client.executor = mockExec
	defer mockExec.close()

	testCases := []struct {
		injectErrors     []error       // Injected errors.
		req              Request       // Request to execute.
		timeout          time.Duration // Request timeout value.
		expectTimeoutErr bool          // Expect RequestTimeout error or not.
		maxNumRetries    uint          // Maximum number of retries.
		retryInterval    time.Duration // Retry interval.
	}{
		// Case 1:
		// Inject 3 retryable errors.
		// Expect to return a RequestTimeout error as the specified timeout
		// expires after performing 2 retries.
		{
			injectErrors: []error{
				// Temporary errors are retryable
				mockErr{msg: "mock retryable error 1", isTemp: true},
				mockErr{msg: "mock retryable error 2", isTemp: true},
				mockErr{msg: "mock retryable error 3", isTemp: true},
			},
			req:              getReq,
			timeout:          2 * time.Second,
			expectTimeoutErr: true,
			maxNumRetries:    3,
			retryInterval:    time.Second,
		},
		// Case 2:
		// Inject 3 retryable errors.
		// Expect to return 3rd error (not a RequestTimeout error as the
		// specified timeout does not expire) after performing 2 retries.
		// Client cannot continue to retry upon receiving 3rd error because max
		// number of retries has reached.
		{
			injectErrors: []error{
				mockErr{msg: "mock retryable error 1", isTemp: true},
				mockErr{msg: "mock retryable error 2", isTemp: true},
				mockErr{msg: "mock retryable error 3", isTemp: true},
			},
			req:              getReq,
			timeout:          5 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    2,
			retryInterval:    time.Second,
		},
		// Case 3:
		// Inject 2 retryable errors and 1 non-retryable error.
		// Expect to return 3rd error which is not retryable.
		{
			injectErrors: []error{
				mockErr{msg: "mock retryable error 1", isTemp: true},
				mockErr{msg: "mock retryable error 2", isTemp: true},
				mockErr{msg: "mock non-retryable error 3", isTemp: false},
			},
			req:              getReq,
			timeout:          5 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 4:
		// Inject 1 non-retryable error.
		// Expect to return the error immediately.
		{
			injectErrors: []error{
				mockErr{msg: "mock non-retryable error 1", isTemp: false},
			},
			req:              getReq,
			timeout:          5 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 5:
		// Inject 1 retryable error.
		// Expect to return the error immediately as retry handler is not set.
		{
			injectErrors: []error{
				mockErr{msg: "mock retryable error 1", isTemp: true},
			},
			req:              getReq,
			timeout:          5 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    0,
			retryInterval:    time.Second,
		},
		// Case 6:
		// Inject 1 retryable error.
		// Expect to return the error immediately as the specified
		// ListTablesRequest is a non-retryable request.
		{
			injectErrors: []error{
				mockErr{msg: "mock retryable error 1", isTemp: true},
			},
			req:              listTablesReq,
			timeout:          10 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 7:
		// Inject 1 retryable error and 1 non-retryable TableNotFound error.
		// Expect to return TableNotFound error.
		{
			injectErrors: []error{
				mockErr{msg: "mock retryable error 1", isTemp: true},
				nosqlerr.New(nosqlerr.TableNotFound, "non-retryable TableNotFound error"),
			},
			req:              getReq,
			timeout:          10 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 8:
		// Inject 4 retryable errors.
		// Expect to return a RequestTimeout error wrapped over by the last
		// ReadLimitExceeded error as the specified timeout expires after
		// performing 3 retries.
		{
			injectErrors: []error{
				nosqlerr.New(nosqlerr.TableBusy, "retryable TableBusy error"),
				nosqlerr.New(nosqlerr.ReadLimitExceeded, "retryable ReadLimitExceeded error 1"),
				nosqlerr.New(nosqlerr.ReadLimitExceeded, "retryable ReadLimitExceeded error 2"),
				nosqlerr.New(nosqlerr.ReadLimitExceeded, "retryable ReadLimitExceeded error 3"),
			},
			req:              getReq,
			timeout:          3 * time.Second,
			expectTimeoutErr: true,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 9:
		// Inject 1 retryable error and 1 non-retryable error.
		// Expect to return 2nd error after performing 1 retry.
		{
			injectErrors: []error{
				nosqlerr.New(nosqlerr.ServerError, "retryable ServerError"),
				nosqlerr.New(nosqlerr.TableNotFound, "non-retryable TableNotFound error 1"),
			},
			req:              getReq,
			timeout:          4 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 10:
		// Inject an HTTP 400 error.
		// Expect to return that error immediately.
		{
			injectErrors: []error{
				mockErr{errCode: http.StatusBadRequest, msg: "non-retryable HTTP 400 Bad Request error"},
			},
			req:              getReq,
			timeout:          4 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
		// Case 11:
		// Inject an HTTP 502 error.
		// Expect to return that error immediately.
		{
			injectErrors: []error{
				mockErr{errCode: http.StatusBadGateway, msg: "non-retryable HTTP 502 Bad Gateway error"},
			},
			req:              getReq,
			timeout:          4 * time.Second,
			expectTimeoutErr: false,
			maxNumRetries:    5,
			retryInterval:    time.Second,
		},
	}

	for i, r := range testCases {
		prefixMsg := fmt.Sprintf("Testcase %d: ", i+1)
		if r.maxNumRetries > 0 {
			retryHandler, err := NewDefaultRetryHandler(r.maxNumRetries, r.retryInterval)
			if !assert.NoErrorf(t, err, prefixMsg+"failed to create a retry handler") {
				continue
			}
			client.RetryHandler = retryHandler
		} else {
			client.RetryHandler = nil
		}

		// A channel that indicates if the execution has done.
		doneCh := make(chan struct{})
		numErr := len(r.injectErrors)
		// Inject errors in a separate goroutine.
		go func() {
			for _, e := range r.injectErrors {
				select {
				case <-doneCh:
					return
				default:
					mockExec.errChan <- e
				}
			}
		}()

		if gr, ok := r.req.(*GetRequest); ok {
			gr.Timeout = r.timeout
			_, err = client.Get(gr)
		} else if lr, ok := r.req.(*ListTablesRequest); ok {
			lr.Timeout = r.timeout
			_, err = client.ListTables(lr)
		}

		close(doneCh)

		var chkErr error
		if r.expectTimeoutErr {
			if assert.Truef(t,
				nosqlerr.Is(err, nosqlerr.RequestTimeout),
				prefixMsg+"expect RequestTimeout, got %v",
				err) {
				e := err.(*nosqlerr.Error)
				// Need to check the cause of RequestTimeout error.
				chkErr = e.Cause
			} else {
				continue
			}

		} else {
			chkErr = err
		}

		switch e := chkErr.(type) {
		case *url.Error:
			// Check the cause of url.Error
			assert.Equalf(t, r.injectErrors[numErr-1], e.Err, prefixMsg+"got unexpected error")
		case *nosqlerr.Error:
			assert.Equalf(t, r.injectErrors[numErr-1], e, prefixMsg+"got unexpected error")
		default:
			// Expect an HTTP not OK response error.
			expectErr := r.injectErrors[numErr-1]
			if me, ok := expectErr.(mockErr); ok {
				// See the error returned from processNotOKResponse()
				expectErr = client.processNotOKResponse([]byte(me.msg), me.errCode)
			}
			assert.Equalf(t, expectErr, e, prefixMsg+"got unexpected error")
		}
	}
}

func newMockClient() (*Client, error) {
	authProvider := &DummyAccessTokenProvider{
		TenantID: "TestTenantId",
	}

	cfg := Config{
		Endpoint:              "mockHost:8080",
		AuthorizationProvider: authProvider,
	}
	client, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

type mockExecutor struct {
	// A channel used to receive injected errors.
	errChan chan error
}

// Do implements httputil.RequestExecutor interface.
// It can be assigned to nosqldb.Client.executor.
//
// Any errors returned from http.Client.Do() will be of type *url.Error, so
// mockExecutor.Do() also returns *url.Error upon receiving injected errors.
func (m *mockExecutor) Do(req *http.Request) (*http.Response, error) {
	if m.errChan == nil {
		return nil, nil
	}

	injectErr := <-m.errChan
	switch e := injectErr.(type) {
	case *nosqlerr.Error:
		// Generate and return an http response and nil error
		resp := m.generateResponse(e, req)
		return resp, nil
	case mockErr:
		// Generate and return an http response and nil error
		if e.errCode != 0 {
			resp := m.generateResponse(e, req)
			return resp, nil
		}
		// Return a nil http response and *url.Error
		return nil, &url.Error{
			Op:  req.Method,
			URL: req.URL.String(),
			Err: e,
		}
	default:
		return nil, injectErr
	}
}

func (m *mockExecutor) generateResponse(err error, httpReq *http.Request) *http.Response {
	httpResp := &http.Response{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Request:    httpReq,
		Header:     make(http.Header, 0),
	}

	if ne, ok := err.(*nosqlerr.Error); ok {
		// Generate an HTTP OK response with nosqlerr.Error
		wr := binary.NewWriter()
		wr.Write([]byte{byte(ne.Code)})
		wr.WriteString(&ne.Message)

		body := wr.Bytes()
		httpResp.StatusCode = 200
		httpResp.Status = "200 OK"
		httpResp.Body = ioutil.NopCloser(bytes.NewReader(body))
		httpResp.ContentLength = int64(len(body))
		return httpResp
	}

	// Generate an HTTP non-OK response
	if e, ok := err.(mockErr); ok {
		httpResp.StatusCode = e.errCode
		httpResp.Status = fmt.Sprintf("mock HTTP error: %d", e.errCode)
		httpResp.Body = ioutil.NopCloser(bytes.NewBufferString(e.msg))
		httpResp.ContentLength = int64(len(e.msg))
	}

	return httpResp
}

func (m *mockExecutor) close() {
	if m.errChan != nil {
		close(m.errChan)
	}
}

// A mock error
type mockErr struct {
	msg     string // Error message
	isTemp  bool   // Is temporary error
	errCode int    // A non-zero error code indicates an HTTP error.
}

func (e mockErr) Error() string {
	return e.msg
}

func (e mockErr) Temporary() bool {
	return e.isTemp
}
