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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestLeaseHTTPMetadataAndReplay(t *testing.T) {
	payload := []byte("request payload")
	lease := newRequestBufferLeaseFromBytes(payload)
	req, err := newLeasedPostRequest("http://localhost/request", lease)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), req.ContentLength)
	require.Equal(t, strconv.Itoa(len(payload)), req.Header.Get("Content-Length"))

	got, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, req.Body.Close())

	replay, err := req.GetBody()
	require.NoError(t, err)
	got, err = io.ReadAll(replay)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, replay.Close())

	lease.releaseOwner()
	_, err = req.GetBody()
	require.Equal(t, errRequestLeaseReleased, err)
}

func TestLeaseBodyReadAfterCloseAndDoubleClose(t *testing.T) {
	lease := newRequestBufferLeaseFromBytes([]byte("payload"))
	body, err := lease.newBody()
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.NoError(t, body.Close())

	_, err = body.Read(make([]byte, 1))
	require.Equal(t, http.ErrBodyReadAfterClose, err)
	lease.releaseOwner()
	require.Equal(t, int32(0), atomic.LoadInt32(&lease.refs))
}

func TestLeaseBodyConcurrentReadAndClose(t *testing.T) {
	lease := newRequestBufferLeaseFromBytes(bytes.Repeat([]byte("x"), 64*1024))
	body, err := lease.newBody()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 127)
		for {
			_, readErr := body.Read(buf)
			if readErr == io.EOF || readErr == http.ErrBodyReadAfterClose {
				return
			}
			require.NoError(t, readErr)
		}
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, body.Close())
	}()
	wg.Wait()
	lease.releaseOwner()
}

func TestRequestLeaseDelayedBodyClosePreventsReuse(t *testing.T) {
	payload := bytes.Repeat([]byte("first-request-"), 256)
	lease := newRequestBufferLeaseFromBytes(payload)
	body, err := lease.newBody()
	require.NoError(t, err)
	lease.releaseOwner()

	writer := lease.writer
	require.NotNil(t, writer)
	for i := 0; i < 64; i++ {
		pressure := newRequestBufferLeaseFromBytes(bytes.Repeat([]byte{byte(i)}, len(payload)))
		if writer == pressure.writer {
			t.Fatal("writer recycled while request body was still open")
		}
		pressure.releaseOwner()
	}

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NotNil(t, lease.writer)
	require.NoError(t, body.Close())
	require.Nil(t, lease.writer)
}

func TestRequestLeaseReleaseGuards(t *testing.T) {
	lease := newRequestBufferLeaseFromBytes([]byte("payload"))
	lease.releaseOwner()
	lease.releaseOwner()
	require.Equal(t, int32(0), atomic.LoadInt32(&lease.refs))
	_, err := lease.newBody()
	require.Equal(t, errRequestLeaseReleased, err)
	require.Panics(t, lease.releaseRef)
}
