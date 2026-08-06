package nosqldb

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
)

const maxPooledRequestBufferCapacity = 64 * 1024

var (
	errRequestLeaseReleased = errors.New("request buffer lease is released")
	requestWriterPool       = sync.Pool{New: func() interface{} { return binary.NewWriter() }}
)

type requestBufferLease struct {
	writer      *binary.Writer
	data        []byte
	refs        int32
	ownerOnce   sync.Once
	recycleOnce sync.Once
}

func acquireRequestBufferLease() *requestBufferLease {
	writer := requestWriterPool.Get().(*binary.Writer)
	writer.Reset()
	return &requestBufferLease{writer: writer, refs: 1}
}

func newRequestBufferLeaseFromBytes(data []byte) *requestBufferLease {
	lease := acquireRequestBufferLease()
	_, _ = lease.writer.Write(data)
	lease.data = lease.writer.Bytes()
	return lease
}

func (l *requestBufferLease) bytes() []byte {
	if l == nil {
		return nil
	}
	return l.data
}

func (l *requestBufferLease) newBody() (io.ReadCloser, error) {
	if l == nil {
		return nil, errRequestLeaseReleased
	}
	for {
		refs := atomic.LoadInt32(&l.refs)
		if refs <= 0 {
			return nil, errRequestLeaseReleased
		}
		if atomic.CompareAndSwapInt32(&l.refs, refs, refs+1) {
			return &leaseBody{reader: bytes.NewReader(l.data), lease: l}, nil
		}
	}
}

func (l *requestBufferLease) releaseOwner() {
	if l == nil {
		return
	}
	l.ownerOnce.Do(l.releaseRef)
}

func (l *requestBufferLease) releaseRef() {
	for {
		refs := atomic.LoadInt32(&l.refs)
		if refs <= 0 {
			panic("nosqldb: request buffer lease reference underflow")
		}
		if !atomic.CompareAndSwapInt32(&l.refs, refs, refs-1) {
			continue
		}
		if refs == 1 {
			l.recycleOnce.Do(l.recycle)
		}
		return
	}
}

func (l *requestBufferLease) recycle() {
	writer := l.writer
	data := l.data
	l.data = nil
	l.writer = nil
	if writer == nil || cap(data) > maxPooledRequestBufferCapacity {
		return
	}
	writer.Reset()
	requestWriterPool.Put(writer)
}

func newLeasedPostRequest(url string, lease *requestBufferLease) (*http.Request, error) {
	body, err := lease.newBody()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		body.Close()
		return nil, err
	}
	length := int64(len(lease.bytes()))
	req.ContentLength = length
	req.Header.Set("Content-Length", strconv.FormatInt(length, 10))
	req.GetBody = func() (io.ReadCloser, error) {
		return lease.newBody()
	}
	return req, nil
}

type leaseBody struct {
	reader *bytes.Reader
	lease  *requestBufferLease
	mu     sync.Mutex
	closed bool
	once   sync.Once
}

func (b *leaseBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	return b.reader.Read(p)
}

func (b *leaseBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		b.lease.releaseRef()
	})
	return nil
}
