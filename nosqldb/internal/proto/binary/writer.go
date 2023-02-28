//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// Writer encodes data into the wire format for the Binary Protocol and writes
// to a buffer. The Binary Protocol defines the data exchange format between
// the Oracle NoSQL Database proxy and drivers.
//
// Writer implements the io.Write and io.ByteWriter interfaces.
type Writer struct {
	// The underlying byte buffer.
	buf []byte
}

// Initial capacity for the buffer.
const initCap int = 64

// NewWriter creates a writer for the binary protocol.
func NewWriter() *Writer {
	return &Writer{
		buf: make([]byte, 0, initCap),
	}
}

// Write writes len(p) bytes from p to the buffer.
func (w *Writer) Write(p []byte) (n int, err error) {
	off := w.ensure(len(p))
	n = copy(w.buf[off:], p)
	return n, nil
}

// ensure checks if there are space available in the buffer to hold n more bytes.
// It grows the buffer if needed to guarantee space for n more bytes.
// It returns the offset in the buffer where bytes should be written.
func (w *Writer) ensure(n int) (off int) {
	off = len(w.buf)
	if n <= cap(w.buf)-off {
		w.buf = w.buf[:off+n]
		return off
	}

	// Grow the buffer.
	newCap := 2*cap(w.buf) + n
	bs := make([]byte, off+n, newCap)
	copy(bs, w.buf[:off])
	w.buf = bs
	return off
}

// WriteByte writes a single byte.
//
// This implements io.ByteWriter.
func (w *Writer) WriteByte(b byte) error {
	off := w.ensure(1)
	w.buf[off] = b
	return nil
}

// Size returns the number of bytes in the buffer.
func (w *Writer) Size() int {
	return len(w.buf)
}

// Bytes returns a slice of bytes in the buffer.
func (w *Writer) Bytes() []byte {
	return w.buf
}

// Reset resets the buffer.
func (w *Writer) Reset() {
	if len(w.buf) > 0 {
		w.buf = w.buf[:0]
	}
}

// WriteInt16 encodes and writes the int16 value to the buffer.
func (w *Writer) WriteInt16(value int16) (int, error) {
	off := w.ensure(2)
	binary.BigEndian.PutUint16(w.buf[off:], uint16(value))
	return 2, nil
}

// WriteInt encodes and writes the int value to the buffer.
// It assumes the provided value fits into a signed 32-bit integer.
func (w *Writer) WriteInt(value int) (int, error) {
	off := w.ensure(4)
	binary.BigEndian.PutUint32(w.buf[off:], uint32(value))
	return 4, nil
}

// WriteIntAtOffset encodes and writes the int value to the buffer
// at a specific offset.
// It assumes the provided value fits into a signed 32-bit integer.
func (w *Writer) WriteIntAtOffset(value int, offset int) (int, error) {
	len := len(w.buf)
	if len < offset-4 {
		return 0, errors.New("invalid offset")
	}
	binary.BigEndian.PutUint32(w.buf[offset:offset+4], uint32(value))
	return 4, nil
}

// WritePackedInt encodes the int value using packed integer encoding
// and writes to the buffer.
// It assumes the provided value fits into a signed 32-bit integer.
func (w *Writer) WritePackedInt(value int) (int, error) {
	n := getWriteSortedInt32Length(int32(value))
	off := w.ensure(n)
	writeSortedInt32(w.buf, uint(off), int32(value))
	return n, nil
}

// WritePackedLong encodes the int64 value using packed long encoding
// and writes to the buffer.
func (w *Writer) WritePackedLong(value int64) (int, error) {
	n := getWriteSortedInt64Length(value)
	off := w.ensure(n)
	writeSortedInt64(w.buf, uint(off), value)
	return n, nil
}

// WriteDouble encodes and writes the float64 value to the buffer.
func (w *Writer) WriteDouble(value float64) (int, error) {
	off := w.ensure(8)
	bits := math.Float64bits(value)
	binary.BigEndian.PutUint64(w.buf[off:], bits)
	return 8, nil
}

// WriteString encodes and writes the string value to the buffer.
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

	off := w.ensure(byteLen)
	startOff := off
	for _, r := range rs {
		off += utf8.EncodeRune(w.buf[off:], r)
	}
	n += (off - startOff)
	return
}

// WriteBoolean encodes and writes the bool value to the buffer.
// A true value is encoded as 1 while a false value is encoded as 0.
func (w *Writer) WriteBoolean(value bool) (int, error) {
	if value {
		return w.writeOneByte(1)
	}
	return w.writeOneByte(0)
}

// WriteMap encodes and writes the map value to the buffer.
func (w *Writer) WriteMap(value *types.MapValue) (n int, err error) {
	if value == nil {
		return 0, errors.New("binary.Writer: nil MapValue")
	}

	off := len(w.buf)
	// We don't know the number of bytes needed to store the encoded MapValue
	// until all entries in the MapValue are written, so write a dummy value 0
	// as a place holder.
	c, err := w.WriteInt(0)
	if err != nil {
		return w.Size() - off, err
	}

	startOff := off + c
	// The number of entries in the MapValue.
	size := value.Len()
	_, err = w.WriteInt(size)
	if err != nil {
		return w.Size() - off, err
	}

	for k, v := range value.Map() {
		_, err = w.WriteString(&k)
		if err != nil {
			return w.Size() - off, err
		}

		_, err = w.WriteFieldValue(v)
		if err != nil {
			return w.Size() - off, err
		}
	}

	// Calculate the number of bytes consumed by the MapValue and overwrite
	// the dummy value 0.
	numBytes := w.Size() - startOff
	binary.BigEndian.PutUint32(w.buf[off:off+c], uint32(numBytes))
	return w.Size() - off, nil
}

// WriteArray encodes and writes an array of FieldValues to the buffer.
func (w *Writer) WriteArray(value []types.FieldValue) (n int, err error) {
	off := len(w.buf)
	// We don't know the number of bytes needed to store the encoded array
	// until all elements in the array are written, so write a dummy value 0
	// as a place holder.
	c, err := w.WriteInt(0)
	if err != nil {
		return w.Size() - off, err
	}

	startOff := off + c
	// The number of elements in the array.
	size := len(value)
	_, err = w.WriteInt(size)
	if err != nil {
		return w.Size() - off, err
	}

	for _, v := range value {
		_, err = w.WriteFieldValue(v)
		if err != nil {
			return w.Size() - off, err
		}
	}

	// Calculate the number of bytes consumed by the array and overwrite
	// the dummy value 0.
	numBytes := w.Size() - startOff
	binary.BigEndian.PutUint32(w.buf[off:off+c], uint32(numBytes))
	return w.Size() - off, nil
}

// WriteByteArray encodes and writes a slice of bytes to the buffer.
// The slice of bytes could be nil.
func (w *Writer) WriteByteArray(value []byte) (n int, err error) {
	if value == nil {
		return w.WritePackedInt(-1)
	}

	byteLen := len(value)
	n, err = w.WritePackedInt(byteLen)
	if err != nil || byteLen == 0 {
		return
	}

	cnt, err := w.Write(value)
	n += cnt
	return
}

// WriteByteArrayWithInt encodes and writes a slice of bytes to the buffer.
// The slice of bytes must be non-nil.
func (w *Writer) WriteByteArrayWithInt(value []byte) (n int, err error) {
	n, err = w.WriteInt(len(value))
	if err != nil {
		return
	}

	cnt, err := w.Write(value)
	n += cnt
	return
}

// WriteFieldRange encodes and writes the FieldRange to the buffer.
func (w *Writer) WriteFieldRange(fieldRange *types.FieldRange) (n int, err error) {
	if fieldRange == nil {
		return w.WriteBoolean(false)
	}

	off := w.Size()
	_, err = w.WriteBoolean(true)
	if err != nil {
		return w.Size() - off, err
	}

	_, err = w.WriteString(&fieldRange.FieldPath)
	if err != nil {
		return w.Size() - off, err
	}

	if fieldRange.Start != nil {
		_, err = w.WriteBoolean(true)
		if err != nil {
			return w.Size() - off, err
		}

		_, err = w.WriteFieldValue(fieldRange.Start)
		if err != nil {
			return w.Size() - off, err
		}

		_, err = w.WriteBoolean(fieldRange.StartInclusive)
		if err != nil {
			return w.Size() - off, err
		}

	} else {
		_, err = w.WriteBoolean(false)
		if err != nil {
			return w.Size() - off, err
		}
	}

	if fieldRange.End != nil {
		_, err = w.WriteBoolean(true)
		if err != nil {
			return w.Size() - off, err
		}

		_, err = w.WriteFieldValue(fieldRange.End)
		if err != nil {
			return w.Size() - off, err
		}

		_, err = w.WriteBoolean(fieldRange.EndInclusive)
		if err != nil {
			return w.Size() - off, err
		}

	} else {
		_, err = w.WriteBoolean(false)
		if err != nil {
			return w.Size() - off, err
		}
	}

	return w.Size() - off, err
}

// WriteSerialVersion encodes and writes the SerialVersion v to the buffer.
func (w *Writer) WriteSerialVersion(v int16) (int, error) {
	return w.WriteInt16(v)
}

// WriteOpCode encodes and writes the OpCode op to the buffer.
func (w *Writer) WriteOpCode(op proto.OpCode) (int, error) {
	return w.writeOneByte(byte(op))
}

// WriteTimeout encodes and writes the timeout value to the buffer.
func (w *Writer) WriteTimeout(timeout time.Duration) (int, error) {
	// Starting with go1.13, use timeout.Milliseconds()
	timeoutMs := int(timeout.Nanoseconds() / 1e6)
	return w.WritePackedInt(timeoutMs)
}

// WriteDurability encodes and writes the Durability value to the buffer.
func (w *Writer) WriteDurability(c types.Durability, serialVersion int16) (int, error) {
	if serialVersion < 3 {
		return 0, nil
	}
	return w.writeOneByte(w.getDurability(c))
}

// WriteConsistency encodes and writes the consistency value to the buffer.
func (w *Writer) WriteConsistency(c types.Consistency) (int, error) {
	return w.writeOneByte(w.getConsistency(c))
}

// WriteCapacityMode encodes and writes the limits mode value to the buffer.
func (w *Writer) WriteCapacityMode(lm types.CapacityMode, serialVersion int16) (int, error) {
	if serialVersion <= 2 {
		return 0, nil
	}
	return w.writeOneByte(w.getCapacityMode(lm))
}

// WriteTTL encodes and writes the TTL value to the buffer.
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

// WriteVersion encodes and writes the specified version to the buffer.
func (w *Writer) WriteVersion(version types.Version) (int, error) {
	if version == nil {
		return 0, errors.New("binary.Writer: nil version")
	}
	return w.WriteByteArray(version)
}

// WriteFieldValue encodes and writes the specified field value to the buffer.
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
		if uint64(v) <= math.MaxInt64 {
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

	case *types.EmptyValue:
		return w.writeOneByte(byte(types.Empty))

	case *types.NullValue:
		return w.writeOneByte(byte(types.Null))

	case *types.JSONNullValue:
		return w.writeOneByte(byte(types.JSONNull))

	case nil:
		return w.writeOneByte(byte(types.JSONNull))

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

// getDurability returns the durability value accepted by server, which is
// a single byte with 3 2-bit fields.
func (w *Writer) getDurability(d types.Durability) byte {
	var dur byte = byte(d.MasterSync)
	dur |= byte(d.ReplicaSync << 2)
	dur |= byte(d.ReplicaAck << 4)
	return dur
}

// getConsistency returns the consistency value accepted by server, which is
// types.Consistency minus one.
func (w *Writer) getConsistency(c types.Consistency) byte {
	if c == types.Absolute || c == types.Eventual {
		return byte(c) - 1
	}
	return byte(c)
}

// getCapacityMode returns the capacity mode value accepted by the server,
// which is 1 for provisioned and 2 for on demand.
func (w *Writer) getCapacityMode(lm types.CapacityMode) byte {
	if lm == types.OnDemand {
		return 2
	}
	// the default is Provisioned
	return 1
}

// writeOneByte is a wrapper for WriteByte. It writes a byte to the buffer and
// returns the number of bytes written.
func (w *Writer) writeOneByte(b byte) (n int, err error) {
	err = w.WriteByte(b)
	if err == nil {
		n = 1
	}
	return
}
