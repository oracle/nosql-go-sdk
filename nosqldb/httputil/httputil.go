//
// Copyright (c) 2019, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package httputil

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

const (
	retryInterval = time.Second
)

// NewPostRequest creates an http POST request using the specified url and data.
// It wraps data into an io.Reader and calls http.NewRequest with POST method.
func NewPostRequest(url string, data []byte) (*http.Request, error) {
	body := bytes.NewReader(data)
	return http.NewRequest(http.MethodPost, url, body)
}

// NewGetRequest create an http GET request using the specified url.
func NewGetRequest(url string) (*http.Request, error) {
	return http.NewRequest(http.MethodGet, url, http.NoBody)
}

// RequestExecutor represents an interface used to execute an HTTP request.
type RequestExecutor interface {
	// Do is used to send an http request to server, returns an http response
	// and an error if occurred during execution.
	Do(req *http.Request) (*http.Response, error)
}

// Response represents a response that contains the content and status code
// of an http.Response returned from server.
type Response struct {
	Body []byte // HTTP response body.
	Code int    // HTTP response status code.
}

// newHTTPRequest creates an http request using the specified method, url and
// data. The http request header is populated with specified headers.
func newHTTPRequest(method string, url string, data []byte, headers map[string]string) (*http.Request, error) {
	var rd io.Reader
	if len(data) > 0 {
		rd = bytes.NewReader(data)
	}

	httpReq, err := http.NewRequest(method, url, rd)
	if err != nil {
		return nil, err
	}

	// Set http headers.
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	if httpReq.Header.Get("Host") == "" {
		httpReq.Header.Set("Host", httpReq.URL.Hostname())
	}

	return httpReq, nil
}

// executeRequest creates an http request using the specified method, url, data
// and headers, then executes the request using the specified executor.
func executeRequest(ctx context.Context, executor RequestExecutor, timeout time.Duration,
	method string, url string, data []byte, headers map[string]string,
	logger *logger.Logger) (*Response, error) {

	httpReq, err := newHTTPRequest(method, url, data, headers)
	if err != nil {
		return nil, err
	}

	reqCtx, reqCancel := context.WithTimeout(ctx, timeout)
	defer reqCancel()

	httpReq = httpReq.WithContext(reqCtx)
	httpResp, err := executor.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	return &Response{
		Code: httpResp.StatusCode,
		Body: body,
	}, nil
}

// DoRequest creates an http request using the specified method, url, data and
// headers, then executes the request using the specified executor.
// When executor returns an http response after execution, DoRequest checks the
// response status code, it returns immediately if status code is less than 500,
// otherwise, it retries the request until either the request gets executed
// successfully or the specified timeout elapses.
func DoRequest(ctx context.Context, executor RequestExecutor, timeout time.Duration,
	method string, url string, data []byte, headers map[string]string,
	logger *logger.Logger) (*Response, error) {

	var err error
	var resp *Response
	var timer *time.Timer
	var delay time.Duration
	var numAttempts uint

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

DoRetry:
	for {
		numAttempts++
		resp, err = executeRequest(reqCtx, executor, timeout, method, url, data, headers, logger)
		if err != nil {
			break
		}

		if resp.Code < 500 {
			return resp, nil
		}

		// Retry if status code >= 500.
		logger.Fine("Remote server temporarily unavailable, status code: %d, response: %s",
			resp.Code, string(resp.Body))
		delay = (1 << (numAttempts - 1)) * retryInterval
		logger.Fine("DoRequest(): number of attempts: %d, will retry in %v.", numAttempts, delay)

		if timer == nil {
			timer = time.NewTimer(delay)
			defer timer.Stop()
		} else {
			timer.Reset(delay)
		}

		select {
		case <-timer.C: // Stop timer and retry the request.
			timer.Stop()
		case <-reqCtx.Done(): // Request timeout or canceled.
			timer.Stop()
			break DoRetry
		}
	}

	var errMsg string
	if err != nil {
		errMsg = fmt.Sprintf(", got error: %v", err)
	}
	ctxErr := reqCtx.Err()
	switch {
	case ctxErr == context.DeadlineExceeded:
		return nil, nosqlerr.NewRequestTimeout("request timed out after %v, "+
			"number of attempts: %d"+errMsg, timeout, numAttempts)
	case ctxErr == context.Canceled:
		return nil, fmt.Errorf("request was canceled" + errMsg)
	default:
		return nil, err
	}
}

// BasicAuth returns a basic authentication string of the format:
//
//	Basic base64(clientId:clientSecret)
func BasicAuth(clientID string, clientSecret []byte) string {
	s := fmt.Sprintf("%s:%s", clientID, string(clientSecret))
	buf := UTF8Encode(s)
	return "Basic " + base64.StdEncoding.EncodeToString(buf)
}

// UTF8Encode returns the UTF-8 encoding of the specified string.
func UTF8Encode(s string) []byte {
	rs := []rune(s)
	byteLen := 0
	for _, r := range rs {
		byteLen += utf8.RuneLen(r)
	}
	buf := make([]byte, byteLen)
	off := 0
	for _, r := range rs {
		off += utf8.EncodeRune(buf[off:], r)
	}
	return buf
}
