package httputil

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxConnsPerHostConfiguration(t *testing.T) {
	client, err := NewHTTPClient(HTTPConfig{MaxConnsPerHost: 256})
	require.NoError(t, err)
	transport, ok := client.client.Transport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 256, transport.MaxConnsPerHost)

	_, err = NewHTTPClient(HTTPConfig{MaxConnsPerHost: -1})
	require.Error(t, err)
}

func TestCloseIdleConnectionsDoesNotInterruptActiveRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		response, requestErr := client.client.Get(server.URL)
		if requestErr == nil {
			_, requestErr = io.ReadAll(response.Body)
			response.Body.Close()
		}
		result <- requestErr
	}()

	<-started
	client.CloseIdleConnections()
	close(release)
	require.NoError(t, <-result)
}
