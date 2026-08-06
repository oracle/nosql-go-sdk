package nosqldb

import (
	"bytes"
	"net/http"
	"sync"

	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

const maxPooledResponseBufferCapacity = 64 * 1024

var responseBufferPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

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

func readHTTPResponseBodyLease(httpResp *http.Response) (*responseBufferLease, error) {
	if httpResp == nil {
		return nil, nosqlerr.New(nosqlerr.UnknownError, "nil http response")
	}
	if httpResp.Body == nil {
		return nil, nil
	}
	defer httpResp.Body.Close()

	lease := acquireResponseBufferLease()
	if _, err := lease.buffer.ReadFrom(httpResp.Body); err != nil {
		lease.release()
		return nil, err
	}
	return lease, nil
}
