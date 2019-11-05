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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// Writer is a binary protocol writer that writes the atomic values or objects
// as byte sequences according to the protocol established between client and server.
//
// Writer implements the io.Write and io.ByteWriter interfaces.
type Writer struct {
	wr io.Writer
	n  int
}

// NewWriter creates a new binary protocol Writer.
// If the provided io.Writer is already a binary protocol Writer, it returns
// the provided one without creating a new Writer.
func NewWriter(w io.Writer) *Writer {
	if w, ok := w.(*Writer); ok {
		w.n = 0
		return w
	}
	return &Writer{wr: w}
}

// Write writes len(p) bytes from p to the output.
func (w *Writer) Write(p []byte) (n int, err error) {
	n, err = w.wr.Write(p)
	w.n += n
	return
}

// WriteByte writes the specified byte to the output.
func (w *Writer) WriteByte(b byte) error {
	_, err := w.Write([]byte{b})
	return err
}

// NumBytes returns the number of bytes that have been written.
func (w *Writer) NumBytes() int {
	return w.n
}

// WriteInt16 writes the specified int16 value to the output.
func (w *Writer) WriteInt16(value int16) (int, error) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(value))
	return w.Write(buf)
}

// WriteInt writes the specified int value to the output.
func (w *Writer) WriteInt(value int) (int, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return w.Write(buf)
}

// WritePackedInt writes the specified int value using packed int encoding to the output.
func (w *Writer) WritePackedInt(value int) (int, error) {
	buf := make([]byte, maxPackedInt32Length)
	off := writeSortedInt32(buf, 0, int32(value))
	return w.Write(buf[:off])
}

// WritePackedLong writes the specified int64 value using packed long encoding to the output.
func (w *Writer) WritePackedLong(value int64) (int, error) {
	buf := make([]byte, maxPackedInt64Length)
	off := writeSortedInt64(buf, 0, value)
	return w.Write(buf[:off])
}

// WriteDouble writes the specified float64 value to the output.
func (w *Writer) WriteDouble(value float64) (int, error) {
	bits := math.Float64bits(value)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return w.Write(buf)
}

// WriteString writes the specified string value to the output.
func (w *Writer) WriteString(value *string) (n int, err error) {
	if value == nil {
		return w.WritePackedInt(-1)
	}

	rs := []rune(*value)
	byteLen := 0
	for _, r := range rs {
		byteLen += utf8.RuneLen(r)
	}
	n, err = w.WritePackedInt(byteLen)
	if err != nil || byteLen == 0 {
		return
	}

	buf := make([]byte, byteLen)
	off := 0
	for _, r := range rs {
		off += utf8.EncodeRune(buf[off:], r)
	}
	cnt, err := w.Write(buf[:off])
	n += cnt
	return
}

// WriteBoolean writes the specified bool value to the output.
// A true value is written as one and a false value is written as zero.
func (w *Writer) WriteBoolean(value bool) (int, error) {
	if value {
		return w.writeOneByte(1)
	}
	return w.writeOneByte(0)
}

// WriteMap writes the specified MapValue to the output.
func (w *Writer) WriteMap(value *types.MapValue) (n int, err error) {
	if value == nil {
		return 0, errors.New("binary.Writer: nil MapValue")
	}

	var buf bytes.Buffer
	contentWr := NewWriter(&buf)
	size := value.Len()

	// Ignores the number of bytes that were written because the content was
	// written into a bytes.Buffer temporarily, it has not been written into
	// the user provided io.Writer.
	_, err = contentWr.WriteInt(size)
	if err != nil {
		return
	}

	for k, v := range value.Map() {
		_, err = contentWr.WriteString(&k)
		if err != nil {
			return
		}

		_, err = contentWr.WriteFieldValue(v)
		if err != nil {
			return
		}
	}

	return w.WriteByteArrayWithInt(buf.Bytes())
}

// WriteArray writes the specified slice of FieldValues to the output.
func (w *Writer) WriteArray(value []types.FieldValue) (n int, err error) {
	var buf bytes.Buffer
	contentWr := NewWriter(&buf)
	size := len(value)
	_, err = contentWr.WriteInt(size)
	if err != nil {
		return
	}

	for _, v := range value {
		_, err = contentWr.WriteFieldValue(v)
		if err != nil {
			return
		}
	}

	return w.WriteByteArrayWithInt(buf.Bytes())
}

// WriteByteArray writes the specified slice of bytes to the output.
func (w *Writer) WriteByteArray(value []byte) (n int, err error) {
	var byteLen int
	// value is nil or len(value) == 0
	if byteLen = len(value); byteLen == 0 {
		return w.WritePackedInt(-1)
	}

	n, err = w.WritePackedInt(byteLen)
	if err != nil {
		return
	}
	cnt, err := w.Write(value)
	n += cnt
	return
}

// WriteByteArrayWithInt writes the specified slice of bytes to the output.
func (w *Writer) WriteByteArrayWithInt(value []byte) (n int, err error) {
	n, err = w.WriteInt(len(value))
	if err != nil {
		return
	}
	cnt, err := w.Write(value)
	n += cnt
	return
}

// WriteFieldRange writes the specified FieldRange to the output.
func (w *Writer) WriteFieldRange(fieldRange *types.FieldRange) (n int, err error) {
	if fieldRange == nil {
		return w.WriteBoolean(false)
	}

	var buf bytes.Buffer
	contentWr := NewWriter(&buf)
	contentWr.WriteBoolean(true)
	contentWr.WriteString(&fieldRange.FieldPath)
	if fieldRange.Start != nil {
		contentWr.WriteBoolean(true)
		contentWr.WriteFieldValue(fieldRange.Start)
		contentWr.WriteBoolean(fieldRange.StartInclusive)
	} else {
		contentWr.WriteBoolean(false)
	}

	if fieldRange.End != nil {
		contentWr.WriteBoolean(true)
		contentWr.WriteFieldValue(fieldRange.End)
		contentWr.WriteBoolean(fieldRange.EndInclusive)
	} else {
		contentWr.WriteBoolean(false)
	}

	return w.Write(buf.Bytes())
}

// WriteSerialVersion writes the specified SerialVersion v to the output.
func (w *Writer) WriteSerialVersion(v int16) (int, error) {
	return w.WriteInt16(v)
}

// WriteOpCode writes the specified OpCode op to the output.
func (w *Writer) WriteOpCode(op proto.OpCode) (int, error) {
	return w.writeOneByte(byte(op))
}

// WriteTimeout writes the specified timeout to the output.
func (w *Writer) WriteTimeout(timeout time.Duration) (int, error) {
	timeoutMs := int(timeout.Nanoseconds() / 1e6)
	return w.WritePackedInt(timeoutMs)
}

// WriteConsistency writes the specified consistency to the output.
func (w *Writer) WriteConsistency(c types.Consistency) (int, error) {
	return w.writeOneByte(w.getConsistency(c))
}

// WriteTTL writes the specified TTL value to the output.
func (w *Writer) WriteTTL(ttl *types.TimeToLive) (n int, err error) {
	if ttl == nil {
		return w.WritePackedLong(-1)
	}
	if ttl.Unit != types.Days && ttl.Unit != types.Hours {
		return 0, errors.New("binary.Writer: invalid TTL unit")
	}

	if n, err = w.WritePackedLong(ttl.Value); err != nil {
		return
	}
	cnt, err := w.writeOneByte(byte(ttl.Unit))
	n += cnt
	return
}

// WriteVersion writes the specified version to the output.
func (w *Writer) WriteVersion(version types.Version) (int, error) {
	if version == nil {
		return 0, errors.New("binary.Writer: version cannot be null")
	}
	return w.WriteByteArray(version)
}

// WriteFieldValue writes the specified field value to the output.
func (w *Writer) WriteFieldValue(value types.FieldValue) (int, error) {
	switch v := value.(type) {
	case string:
		return w.writeStringValue(&v)

	case *string:
		return w.writeStringValue(v)

	case int:
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			return w.writeIntegerValue(v)
		}
		return w.writeLongValue(int64(v))

	case uint:
		if v <= math.MaxInt32 {
			return w.writeIntegerValue(int(v))
		}
		if v <= math.MaxInt64 {
			return w.writeLongValue(int64(v))
		}
		s := strconv.FormatUint(uint64(v), 10)
		return w.writeNumberValue(s)

	case int8:
		return w.writeIntegerValue(int(v))

	case uint8:
		return w.writeIntegerValue(int(v))

	case int16:
		return w.writeIntegerValue(int(v))

	case uint16:
		return w.writeIntegerValue(int(v))

	case int32:
		return w.writeIntegerValue(int(v))

	case uint32:
		if v <= math.MaxInt32 {
			return w.writeIntegerValue(int(v))
		}
		return w.writeLongValue(int64(v))

	case int64:
		return w.writeLongValue(v)

	case uint64:
		if v <= math.MaxInt64 {
			return w.writeLongValue(int64(v))
		}
		s := strconv.FormatUint(v, 10)
		return w.writeNumberValue(s)

	case float32:
		return w.writeDoubleValue(float64(v))

	case float64:
		return w.writeDoubleValue(v)

	case bool:
		return w.writeBooleanValue(v)

	case *types.MapValue:
		return w.writeMapValue(v)

	case map[string]interface{}:
		return w.writeMapValue(types.NewMapValue(v))

	case []types.FieldValue:
		return w.writeArrayValue(v)

	case []interface{}:
		arr := make([]types.FieldValue, len(v))
		for i, e := range v {
			arr[i] = e
		}
		return w.writeArrayValue(arr)

	case time.Time:
		return w.writeTimestampValue(v)

	case *big.Rat:
		var strNum string
		if v.IsInt() {
			strNum = v.RatString()
		} else {
			floatVal, _ := v.Float64()
			strNum = fmt.Sprintf("%g", floatVal)
		}
		return w.writeNumberValue(strNum)

	case []byte:
		return w.writeBinaryValue(v)

	case json.Number:
		iv, err := v.Int64()
		if err == nil {
			if iv >= math.MinInt32 && iv <= math.MaxInt32 {
				return w.writeIntegerValue(int(iv))
			}
			return w.writeLongValue(iv)
		}
		fv, err := v.Float64()
		if err == nil {
			return w.writeDoubleValue(fv)
		}
		return w.writeNumberValue(v.String())
		// return 0, fmt.Errorf("unable to parse json.Number as int64 or float64, got %v, %T", v, v)

	case *types.EmptyValue:
		return w.writeOneByte(byte(types.Empty))

	case *types.NullValue:
		return w.writeOneByte(byte(types.Null))

	case *types.JSONNullValue:
		return w.writeOneByte(byte(types.JsonNull))

	case nil:
		return w.writeOneByte(byte(types.JsonNull))

	default:
		return 0, fmt.Errorf("binary.Writer: unsupported field value %v of type %[1]T", v)
	}
}

func (w *Writer) writeIntegerValue(value int) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Integer)); err != nil {
		return
	}
	cnt, err := w.WritePackedInt(value)
	n += cnt
	return
}

func (w *Writer) writeLongValue(value int64) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Long)); err != nil {
		return
	}
	cnt, err := w.WritePackedLong(value)
	n += cnt
	return
}

func (w *Writer) writeDoubleValue(value float64) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Double)); err != nil {
		return
	}
	cnt, err := w.WriteDouble(value)
	n += cnt
	return
}

func (w *Writer) writeStringValue(value *string) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.String)); err != nil {
		return
	}
	cnt, err := w.WriteString(value)
	n += cnt
	return
}

func (w *Writer) writeBooleanValue(value bool) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Boolean)); err != nil {
		return
	}
	cnt, err := w.WriteBoolean(value)
	n += cnt
	return
}

func (w *Writer) writeMapValue(value *types.MapValue) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Map)); err != nil {
		return
	}
	cnt, err := w.WriteMap(value)
	n += cnt
	return
}

func (w *Writer) writeArrayValue(value []types.FieldValue) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Array)); err != nil {
		return
	}
	cnt, err := w.WriteArray(value)
	n += cnt
	return
}

func (w *Writer) writeTimestampValue(value time.Time) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Timestamp)); err != nil {
		return
	}
	s := value.UTC().Format(time.RFC3339Nano)
	cnt, err := w.WriteString(&s)
	n += cnt
	return
}

func (w *Writer) writeNumberValue(value string) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Number)); err != nil {
		return
	}
	cnt, err := w.WriteString(&value)
	n += cnt
	return
}

func (w *Writer) writeBinaryValue(value []byte) (n int, err error) {
	if n, err = w.writeOneByte(byte(types.Binary)); err != nil {
		return
	}
	cnt, err := w.WriteByteArray(value)
	n += cnt
	return
}

// getConsistency returns the consistency value accepted by server, which is
// types.Consistency minus one
func (w *Writer) getConsistency(c types.Consistency) byte {
	if c == types.Absolute || c == types.Eventual {
		return byte(c) - 1
	}
	return byte(c)
}

func (w *Writer) writeOneByte(b byte) (n int, err error) {
	err = w.WriteByte(b)
	if err == nil {
		n = 1
	}
	return
}
