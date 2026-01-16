//
// Copyright (c) 2019, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sync"
	"unicode/utf8"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

const (
    readerBufStartLength = 64
    readerBufCap = 256          
    maxPooledReaderBufCap = 64 * 1024 
)

// Create a pool of *Reader (a sync.Pool) so that we avoid allocating a new Reader
// and its buf slice on each call.
var readerPool = sync.Pool{
    New: func() interface{} {
        return &Reader{
            rd:  new(bytes.Buffer),
            buf: make([]byte, readerBufStartLength, readerBufCap),
        }
    },
}

// Reader reads byte sequences from the underlying io.Reader and decodes the
// bytes to construct in-memory representations according to the Binary Protocol
// which defines the data exchange format between the Oracle NoSQL Database
// proxy and drivers.
//
// Reader implements the io.Reader and io.ByteReader interfaces.
type Reader struct {
	// The underlying io.Reader.
	rd *bytes.Buffer

	// A buffer that holds the bytes for decoding.
	buf []byte
}

// GetReader retrieves a *Reader instance that reads from the given *bytes.Buffer.
// It reuses a Reader from the internal pool when available, minimizing allocations.
//
// The returned Reader must be treated as *borrowed* — its ownership belongs to the pool.
// After the caller finishes using it, it **must** call PutReader(r) to return it.
//
// Note: Readers obtained from the pool are **not safe for concurrent use** by multiple
// goroutines. Once PutReader has been called, the Reader must not be accessed again.
func GetReader(b *bytes.Buffer) *Reader {
    r := readerPool.Get().(*Reader)
    r.rd = b

    // ensure r.buf has at least the starting length
    if r.buf == nil || cap(r.buf) < readerBufStartLength {
        r.buf = make([]byte, readerBufStartLength, readerBufCap)
    } else {
        // set length to the minimum needed
        r.buf = r.buf[:readerBufStartLength]
    }
    return r
}

// PutReader returns a Reader to the internal pool for reuse.
//
// WARNING: After calling PutReader(r), the Reader must not be used again by the caller.
// Doing so may result in data corruption or race conditions, since the Reader may be
// given to another goroutine at any time.
func PutReader(r *Reader) {
	if r == nil {
		 return 
	}
    // drop reference to underlying buffer to avoid keeping user data alive.
    r.rd = nil

    // if internal buffer is huge, discard it to prevent memory accumulation.
    if cap(r.buf) <= maxPooledReaderBufCap {
        readerPool.Put(r)
    }
}

// NewReader creates a reader for the binary protocol.
// func NewReader(b *bytes.Buffer) *Reader {
// 	return GetReader(b)
// }

// GetBuffer returns the underlying bytes Buffer.
func (r *Reader) GetBuffer() *bytes.Buffer {
	return r.rd
}

// Read reads up to len(p) bytes into p.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (r *Reader) Read(p []byte) (n int, err error) {
	return r.rd.Read(p)
}

// ReadByte reads and returns a single byte or any error encountered.
func (r *Reader) ReadByte() (byte, error) {
	buf, err := r.readFull(1)
	return buf[0], err
}

// ReadBoolean reads and decodes a single byte as a bool value.
// A zero byte is decoded as false, and any other non-zero byte is decoded as true.
func (r *Reader) ReadBoolean() (bool, error) {
	b, err := r.ReadByte()
	return b != 0, err
}

// ReadInt16 reads and decodes 2 bytes as an int16 value.
func (r *Reader) ReadInt16() (int16, error) {
	buf, err := r.readFull(2)
	if err != nil {
		return 0, err
	}
	value := binary.BigEndian.Uint16(buf)
	return int16(value), nil
}

// ReadInt reads and decodes 4 bytes as an int32 value.
func (r *Reader) ReadInt() (int, error) {
	buf, err := r.readFull(4)
	if err != nil {
		return 0, err
	}
	value := binary.BigEndian.Uint32(buf)
	return int(int32(value)), nil
}

// ReadPackedInt reads a variable length of bytes that is an encoding of packed
// integer, decodes the bytes as an int32 value.
func (r *Reader) ReadPackedInt() (int, error) {
	r.ensure(maxPackedInt32Length)
	_, err := io.ReadFull(r, r.buf[:1])
	if err != nil {
		return 0, err
	}

	byteLen := getReadSortedInt32Length(r.buf[:1], 0)
	if byteLen < 1 || byteLen > maxPackedInt32Length {
		return 0, errors.New("binary.Reader: invalid packed int")
	}

	if byteLen > 1 {
		_, err = io.ReadFull(r, r.buf[1:byteLen])
		if err != nil {
			return 0, err
		}
	}

	return int(readSortedInt32(r.buf[:byteLen], 0)), nil
}

// ReadPackedLong reads a variable length of bytes that is an encoding of packed
// long value, decodes the bytes as an int64 value.
func (r *Reader) ReadPackedLong() (int64, error) {
	r.ensure(maxPackedInt64Length)
	_, err := io.ReadFull(r, r.buf[:1])
	if err != nil {
		return 0, err
	}

	byteLen := getReadSortedInt64Length(r.buf[:1], 0)
	if byteLen < 1 || byteLen > maxPackedInt64Length {
		return 0, errors.New("binary.Reader: invalid packed long")
	}

	if byteLen > 1 {
		_, err = io.ReadFull(r, r.buf[1:byteLen])
		if err != nil {
			return 0, err
		}
	}

	return readSortedInt64(r.buf[:byteLen], 0), nil
}

// ReadDouble reads and decodes 8 bytes as a float64 value.
func (r *Reader) ReadDouble() (float64, error) {
	buf, err := r.readFull(8)
	if err != nil {
		return 0, err
	}

	bits := binary.BigEndian.Uint64(buf)
	value := math.Float64frombits(bits)
	return value, nil
}

// ReadString reads a variable length of bytes that is an encoding of packed
// UTF-8 string value, decodes the bytes as a string. It returns a pointer to
// the string value or any error encountered.
func (r *Reader) ReadString() (*string, error) {
	byteLen, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	switch {
	case byteLen < -1:
		return nil, errors.New("binary.Reader: invalid length of string")
	case byteLen == -1:
		return nil, nil
	case byteLen == 0:
		s := ""
		return &s, nil
	}

	buf, err := r.readFull(byteLen)
	if err != nil {
		return nil, err
	}

	cnt := utf8.RuneCount(buf)
	runeBuf := make([]rune, cnt)
	for i := 0; i < cnt && len(buf) > 0; i++ {
		r, off := utf8.DecodeRune(buf)
		runeBuf[i] = r
		buf = buf[off:]
	}
	s := string(runeBuf)
	return &s, nil
}

// ReadNonNilString reads a string. If there is an error, it will return
// an empty string and the error.
func (r *Reader) ReadNonNilString() (string, error) {
	str, err := r.ReadString()
	if str == nil || err != nil {
		return "", err
	}
	return *str, nil
}

// ReadVersion reads byte sequences and decodes as a types.Version.
func (r *Reader) ReadVersion() (types.Version, error) {
	return r.ReadByteArray()
}

// ReadMap reads a structured byte sequences that represent the encoding of a
// Map value, decodes the bytes and returns as an ordered *types.MapValue.
func (r *Reader) ReadMap() (*types.MapValue, error) {
	// The integer value that represents the number of bytes consumed by the map.
	// This is discarded as it is not used.
	_, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	// The number of entries in the map.
	size, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	value := types.NewOrderedMapValue()
	for i := 0; i < size; i++ {
		k, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		v, err := r.ReadFieldValue()
		if err != nil {
			return nil, err
		}

		if k != nil {
			value.Put(*k, v)
		}
	}

	return value, nil
}

// ReadArray reads a structured byte sequences that represent the encoding of an
// array, decodes the bytes and returns as a slice of types.FieldValue.
func (r *Reader) ReadArray() ([]types.FieldValue, error) {
	// The integer value that represents the number of bytes consumed by the array.
	// This is discarded as it is not used.
	_, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	// The number of elements in the array.
	size, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	value := make([]types.FieldValue, size)
	for i := 0; i < size; i++ {
		value[i], err = r.ReadFieldValue()
		if err != nil {
			return nil, err
		}
	}

	return value, nil
}

// ReadFieldValue reads a fixed or variable length of bytes, decodes them as
// a value of a table field and returns the value or any error encountered.
func (r *Reader) ReadFieldValue() (types.FieldValue, error) {
	t, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch types.DbType(t) {
	case types.Array:
		return r.ReadArray()

	case types.Binary:
		return r.ReadByteArray()

	case types.Boolean:
		return r.ReadBoolean()

	case types.Double:
		return r.ReadDouble()

	case types.Integer:
		return r.ReadPackedInt()

	case types.Long:
		return r.ReadPackedLong()

	case types.Map:
		return r.ReadMap()

	case types.String:
		s, err := r.ReadString()
		if err != nil {
			return s, err
		}

		if s == nil {
			return nil, nil
		}
		return *s, nil

	case types.Timestamp:
		s, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, errors.New("binary.Reader: invalid Timestamp value")
		}
		return types.ParseDateTime(*s)

	case types.Number:
		s, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		if s == nil {
			return nil, errors.New("binary.Reader: invalid Number value")
		}

		number, ok := new(big.Rat).SetString(*s)
		if ok {
			return number, nil
		}

		// Return as a string.
		return *s, nil

	case types.JSONNull:
		return types.JSONNullValueInstance, nil

	case types.Null:
		return types.NullValueInstance, nil

	case types.Empty:
		return types.EmptyValueInstance, nil

	default:
		return nil, fmt.Errorf("binary.Reader: unsupported field value %v of type %[1]T", t)
	}
}

// ReadStructValue deserializes data into a native struct.
// The passed in value must be a pointer to a struct.
func (r *Reader) ReadStructValue(v any) error {
	return UnmarshalFromReader(v, r)
}

// ReadByteArray reads byte sequences and returns as a slice of byte or any error encountered.
// The returned bytes could be nil.
func (r *Reader) ReadByteArray() ([]byte, error) {
	byteLen, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	switch {
	case byteLen < -1:
		return nil, fmt.Errorf("binary.Reader: invalid length of byte array: %d", byteLen)
	case byteLen == -1:
		return nil, nil
	case byteLen == 0:
		return []byte{}, nil
	default:
		buf := make([]byte, byteLen)
		_, err := io.ReadFull(r, buf)
		return buf, err
	}
}

// ReadByteArrayWithInt reads byte sequences and returns as a slice of byte or any error encountered.
// The returned bytes is non-nil.
func (r *Reader) ReadByteArrayWithInt() ([]byte, error) {
	byteLen, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	if byteLen <= 0 {
		return nil, fmt.Errorf("binary.Reader: invalid length of byte array: %d", byteLen)
	}

	buf := make([]byte, byteLen)
	_, err = io.ReadFull(r, buf)
	return buf, err
}

// ensure checks if there are space available in the buffer to hold n more bytes.
// It grows the buffer if needed to guarantee space for n more bytes.
func (r *Reader) ensure(n int) {
	if n <= cap(r.buf) {
		r.buf = r.buf[:n]
	} else {
		r.buf = make([]byte, n, 2*n)
	}
}

// readFull reads exactly n bytes from the underlying reader into the buffer.
func (r *Reader) readFull(n int) ([]byte, error) {
	r.ensure(n)
	_, err := io.ReadFull(r, r.buf)
	return r.buf, err
}
