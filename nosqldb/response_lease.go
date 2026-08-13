//
// Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"

	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

const maxPooledResponseBufferCapacity = 64 * 1024

var (
	responseBufferPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
	// ErrResponseSizeLimitExceeded identifies a response that exceeded MaxResponseSize.
	ErrResponseSizeLimitExceeded = errors.New("response size limit exceeded")
)

// ResponseSizeLimitError reports an oversized response. Observed is a lower
// bound when reading stopped after MaxResponseSize plus one byte.
type ResponseSizeLimitError struct {
	Limit    int64
	Observed int64
}

func (e *ResponseSizeLimitError) Error() string {
	return fmt.Sprintf("response size limit exceeded: limit=%d observed-at-least=%d", e.Limit, e.Observed)
}

func (e *ResponseSizeLimitError) Unwrap() error {
	return ErrResponseSizeLimitExceeded
}

type responseBufferLease struct {
	buffer *bytes.Buffer
	once   sync.Once
}

func acquireResponseBufferLease() *responseBufferLease {
	buffer := responseBufferPool.Get().(*bytes.Buffer)
	buffer.Reset()
	return &responseBufferLease{buffer: buffer}
}

func (l *responseBufferLease) bytes() []byte {
	if l == nil || l.buffer == nil {
		return nil
	}
	return l.buffer.Bytes()
}

func (l *responseBufferLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		buffer := l.buffer
		l.buffer = nil
		if buffer == nil || buffer.Cap() > maxPooledResponseBufferCapacity {
			return
		}
		buffer.Reset()
		responseBufferPool.Put(buffer)
	})
}

func readHTTPResponseBodyLease(httpResp *http.Response, limit int64) (*responseBufferLease, error) {
	if httpResp == nil {
		return nil, nosqlerr.New(nosqlerr.UnknownError, "nil http response")
	}
	if httpResp.Body == nil {
		return nil, nil
	}
	defer httpResp.Body.Close()

	unlimited := limit == 0 || limit == math.MaxInt64
	if !unlimited && !httpResp.Uncompressed && httpResp.ContentLength > limit {
		return nil, &ResponseSizeLimitError{Limit: limit, Observed: httpResp.ContentLength}
	}

	lease := acquireResponseBufferLease()
	reader := io.Reader(httpResp.Body)
	if !unlimited {
		reader = &io.LimitedReader{R: httpResp.Body, N: limit + 1}
	}
	if _, err := lease.buffer.ReadFrom(reader); err != nil {
		lease.release()
		return nil, err
	}
	if !unlimited && int64(lease.buffer.Len()) > limit {
		observed := int64(lease.buffer.Len())
		lease.release()
		return nil, &ResponseSizeLimitError{Limit: limit, Observed: observed}
	}
	return lease, nil
}
