//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package binary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"time"
	"unicode/utf8"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// Reader is a binary protocol reader that reads byte sequences from the server
// and interprets them into objects according to the protocol established
// between client and server.
//
// The ReadXXX() methods of the Reader reads a fixed length or variable length
// of bytes and decodes them as the corresponding values using big endian byte order.
//
// Reader implements the io.Reader and io.ByteReader interfaces.
type Reader struct {
	// The underlying io.Reader.
	rd io.Reader
}

// NewReader creates a new binary protocol Reader.
// If the provided io.Reader is already a binary protocol Reader, it returns
// the provided one without creating a new Reader.
func NewReader(r io.Reader) *Reader {
	if r, ok := r.(*Reader); ok {
		return r
	}
	return &Reader{rd: r}
}

// Read reads up to len(p) bytes into p.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (r *Reader) Read(p []byte) (n int, err error) {
	return r.rd.Read(p)
}

// ReadByte reads and returns the next byte from the input or any error encountered.
func (r *Reader) ReadByte() (byte, error) {
	buf, err := r.readFull(1)
	return buf[0], err
}

// ReadBoolean reads 1 byte, decodes the byte and returns as a bool value or any error encountered.
// A zero byte is decoded as false, and any other non-zero byte is decoded as true.
func (r *Reader) ReadBoolean() (bool, error) {
	b, err := r.ReadByte()
	return b != 0, err
}

// ReadInt16 reads 2 bytes, decodes the bytes using big endian byte order and
// returns as an int16 value or any error encountered.
func (r *Reader) ReadInt16() (int16, error) {
	buf, err := r.readFull(2)
	if err != nil {
		return 0, err
	}
	value := binary.BigEndian.Uint16(buf)
	return int16(value), nil
}

// ReadInt reads 4 bytes, decodes the bytes using big endian byte order and
// returns an int value or any error encountered.
func (r *Reader) ReadInt() (int, error) {
	buf, err := r.readFull(4)
	if err != nil {
		return 0, err
	}
	value := binary.BigEndian.Uint32(buf)
	return int(int32(value)), nil
}

// ReadPackedInt reads a variable length of bytes that is an encoding of packed
// int value, decodes the bytes using big endian byte order and returns as an
// int value or any error encountered.
func (r *Reader) ReadPackedInt() (int, error) {
	buf := make([]byte, maxPackedInt32Length)
	_, err := io.ReadFull(r, buf[:1])
	if err != nil {
		return 0, err
	}

	byteLen := getReadSortedInt32Length(buf, 0)
	if byteLen < 1 || byteLen > len(buf) {
		return 0, errors.New("binary.Reader: invalid packed int")
	}

	_, err = io.ReadFull(r, buf[1:byteLen])
	if err != nil {
		return 0, err
	}

	return int(readSortedInt32(buf[:byteLen], 0)), nil
}

// ReadPackedLong reads a variable length of bytes that is an encoding of packed
// long value, decodes the bytes using big endian byte order and returns as an
// int64 value or any error encountered.
func (r *Reader) ReadPackedLong() (int64, error) {
	buf := make([]byte, maxPackedInt64Length)
	_, err := io.ReadFull(r, buf[:1])
	if err != nil {
		return 0, err
	}

	byteLen := getReadSortedInt64Length(buf[:1], 0)
	if byteLen < 1 || byteLen > len(buf) {
		return 0, errors.New("binary.Reader: invalid packed long")
	}

	_, err = io.ReadFull(r, buf[1:byteLen])
	if err != nil {
		return 0, err
	}

	return readSortedInt64(buf[:byteLen], 0), nil
}

// ReadDouble reads 8 bytes, decodes the bytes using big endian byte order and
// returns as a float64 value or any error encountered.
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
// UTF-8 string value, decodes the bytes using big endian byte order and returns
// as a pointer to the string value or any error encountered.
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

// ReadVersion reads byte sequences and returns as a types.Version or any error encountered.
func (r *Reader) ReadVersion() (types.Version, error) {
	return r.ReadByteArray()
}

// ReadMap reads a structured byte sequences that represent the encoding of a
// Map value, decodes the bytes and returns as an ordered *types.MapValue or any error encountered.
func (r *Reader) ReadMap() (*types.MapValue, error) {
	// Read and discard the length of bytes.
	r.ReadInt()

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
// array, decodes the bytes and returns as a slice of types.FieldValue or any error encountered.
func (r *Reader) ReadArray() ([]types.FieldValue, error) {
	// Read and discard the length of bytes.
	r.ReadInt()

	size, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	value := make([]types.FieldValue, size)
	for i := 0; i < size; i++ {
		v, err := r.ReadFieldValue()
		if err != nil {
			return nil, err
		}

		value[i] = v
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
		return time.Parse(types.ISO8601Layout, *s)

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

	case types.JsonNull:
		return types.JSONNullValueInstance, nil

	case types.Null:
		return types.NullValueInstance, nil

	case types.Empty:
		return types.EmptyValueInstance, nil

	default:
		return nil, fmt.Errorf("binary.Reader: unsupported field value %v of type %[1]T", t)
	}
}

// ReadByteArray reads byte sequences and returns as a slice of byte or any error encountered.
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

func (r *Reader) readFull(byteLen int) ([]byte, error) {
	buf := make([]byte, byteLen)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
