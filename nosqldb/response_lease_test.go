//
// Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"io"
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
	lease, err := readHTTPResponseBodyLease(&http.Response{Body: body})
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

	lease, err := readHTTPResponseBodyLease(&http.Response{Body: io.NopCloser(bytes.NewReader(writer.Bytes()))})
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
	lease, err := readHTTPResponseBodyLease(&http.Response{Body: io.NopCloser(bytes.NewReader(body))})
	require.NoError(t, err)
	buffer := lease.buffer
	require.True(t, buffer.Cap() > maxPooledResponseBufferCapacity)
	lease.release()
	require.Nil(t, lease.buffer)
}
