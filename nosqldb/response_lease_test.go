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
	"io"
	"math"
	"net/http"
	"testing"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/require"
)

type trackedResponseBody struct {
	io.Reader
	closed bool
}

func (b *trackedResponseBody) Close() error {
	b.closed = true
	return nil
}

func TestResponseLeaseClosesBodyAndReleasesBuffer(t *testing.T) {
	body := &trackedResponseBody{Reader: bytes.NewBufferString("response")}
	lease, err := readHTTPResponseBodyLease(&http.Response{Body: body}, 0)
	require.NoError(t, err)
	require.True(t, body.closed)
	require.Equal(t, []byte("response"), lease.bytes())
	require.NotNil(t, lease.buffer)

	lease.release()
	require.Nil(t, lease.buffer)
	lease.release()
}

func TestResponseValuesDoNotAliasPooledBuffer(t *testing.T) {
	writer := binary.NewWriter()
	value := types.NewOrderedMapValueWithCapacity(3)
	value.Put("bytes", []byte{1, 2, 3, 4})
	value.Put("nested", types.NewOrderedMapValue().Put("name", "value"))
	value.Put("array", []types.FieldValue{"one", []byte{5, 6, 7}})
	_, err := writer.WriteMap(value)
	require.NoError(t, err)

	lease, err := readHTTPResponseBodyLease(&http.Response{Body: io.NopCloser(bytes.NewReader(writer.Bytes()))}, 0)
	require.NoError(t, err)
	reader := binary.GetReader(bytes.NewBuffer(lease.bytes()))
	decoded, err := reader.ReadMap()
	binary.PutReader(reader)
	require.NoError(t, err)
	lease.release()

	pressure := acquireResponseBufferLease()
	pressure.buffer.Write(bytes.Repeat([]byte{0xa5}, len(writer.Bytes())))
	pressure.release()

	gotBytes, ok := decoded.GetBinary("bytes")
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4}, gotBytes)
	nestedValue, ok := decoded.Get("nested")
	require.True(t, ok)
	gotNested, ok := nestedValue.(*types.MapValue)
	require.True(t, ok)
	gotName, ok := gotNested.GetString("name")
	require.True(t, ok)
	require.Equal(t, "value", gotName)
	arrayValue, ok := decoded.Get("array")
	require.True(t, ok)
	gotArray, ok := arrayValue.([]types.FieldValue)
	require.True(t, ok)
	require.Equal(t, "one", gotArray[0])
	require.Equal(t, []byte{5, 6, 7}, gotArray[1])
}

func TestResponseLeaseDropsLargeBuffer(t *testing.T) {
	body := bytes.Repeat([]byte("x"), maxPooledResponseBufferCapacity+1)
	lease, err := readHTTPResponseBodyLease(&http.Response{Body: io.NopCloser(bytes.NewReader(body))}, 0)
	require.NoError(t, err)
	buffer := lease.buffer
	require.True(t, buffer.Cap() > maxPooledResponseBufferCapacity)
	lease.release()
	require.Nil(t, lease.buffer)
}

func TestResponseSizeLimit(t *testing.T) {
	tests := []struct {
		name          string
		limit         int64
		body          []byte
		contentLength int64
		uncompressed  bool
		wantError     bool
	}{
		{"unlimited zero", 0, []byte("abcdef"), 6, false, false},
		{"unlimited max int", math.MaxInt64, []byte("abcdef"), 6, false, false},
		{"max int minus one", math.MaxInt64 - 1, []byte("abcdef"), 6, false, false},
		{"exact limit", 6, []byte("abcdef"), 6, false, false},
		{"known oversized", 5, []byte("abcdef"), 6, false, true},
		{"unknown oversized", 5, []byte("abcdef"), -1, false, true},
		{"decompressed oversized", 5, []byte("abcdef"), 3, true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedResponseBody{Reader: bytes.NewReader(tc.body)}
			lease, err := readHTTPResponseBodyLease(&http.Response{
				Body:          body,
				ContentLength: tc.contentLength,
				Uncompressed:  tc.uncompressed,
			}, tc.limit)
			require.True(t, body.closed)
			if !tc.wantError {
				require.NoError(t, err)
				require.Equal(t, tc.body, lease.bytes())
				lease.release()
				return
			}

			require.Error(t, err)
			require.True(t, errors.Is(err, ErrResponseSizeLimitExceeded))
			var sizeErr *ResponseSizeLimitError
			require.True(t, errors.As(err, &sizeErr))
			require.Equal(t, tc.limit, sizeErr.Limit)
			require.True(t, sizeErr.Observed > tc.limit)
			require.Nil(t, lease)
		})
	}
}

type failingResponseReader struct{}

func (failingResponseReader) Read([]byte) (int, error) {
	return 0, errors.New("read failure")
}

func TestResponseSizeLimitReadErrorClosesBody(t *testing.T) {
	body := &trackedResponseBody{Reader: failingResponseReader{}}
	lease, err := readHTTPResponseBodyLease(&http.Response{Body: body, ContentLength: -1}, 8)
	require.Error(t, err)
	require.True(t, body.closed)
	require.Nil(t, lease)
}

func TestNegativeMaxResponseSizeRejected(t *testing.T) {
	cfg := Config{MaxResponseSize: -1}
	require.Error(t, cfg.validate())
}
