//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"bytes"
	"encoding/base64"
	"io"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

type ReadWriteTestSuite struct {
	suite.Suite
}

var packedIntTests = []int{
	0, -123456789, 123456789,
	// Int values that take 1 byte to store.
	-v119, v120,
	// Int values that take 2 bytes to store.
	-max1 - v119 - 1, -v119 - 1,
	v120 + 1, max1 + v120 + 1,
	// Int values that take 3 bytes to store.
	-max2 - v119 - 1, -max2,
	-max1 - v119 - 99, -max1 - v119 - 2,
	max1 + v120 + 2, max1 + v120 + 99,
	max2, max2 + v120 + 1,
	// Int values that take 4 bytes to store.
	-max3 - v119 - 1, -max3,
	-max2 - v119 - 99, -max2 - v119 - 2,
	max2 + v120 + 2, max2 + v120 + 99,
	max3, max3 + v120 + 1,
	// Int values that take 5 bytes to store.
	minInt32, minInt32 + 99,
	maxInt32 - 99, maxInt32,
}

var packedLongTests = []int64{
	0, -1234567890123456789, 1234567890123456789,
	// Int64 values that take 1 byte to store.
	-v119, v120,
	// Int64 values that take 2 bytes to store.
	-max1 - v119 - 1, -v119 - 1,
	v120 + 1, max1 + v120 + 1,
	// Int64 values that take 3 bytes to store.
	-max2 - v119 - 1, -max2,
	-max1 - v119 - 99, -max1 - v119 - 2,
	max1 + v120 + 2, max1 + v120 + 99,
	max2, max2 + v120 + 1,
	// Int64 values that take 4 bytes to store.
	-max3 - v119 - 1, -max3,
	-max2 - v119 - 99, -max2 - v119 - 2,
	max2 + v120 + 2, max2 + v120 + 99,
	max3, max3 + v120 + 1,
	// Int64 values that take 5 bytes to store.
	-max4 - v119 - 1, -max4,
	-max3 - v119 - 99, -max3 - v119 - 2,
	max3 + v120 + 2, max3 + v120 + 99,
	max4, max4 + v120 + 1,
	// Int64 values that take 6 bytes to store.
	-max5 - v119 - 1, -max5,
	-max4 - v119 - 99, -max4 - v119 - 2,
	max4 + v120 + 2, max4 + v120 + 99,
	max5, max5 + v120 + 1,
	// Int64 values that take 7 bytes to store.
	-max6 - v119 - 1, -max6,
	-max5 - v119 - 99, -max5 - v119 - 2,
	max5 + v120 + 2, max5 + v120 + 99,
	max6, max6 + v120 + 1,
	// Int64 values that take 8 bytes to store.
	-max7 - v119 - 1, -max7,
	-max6 - v119 - 99, -max6 - v119 - 2,
	max6 + v120 + 2, max6 + v120 + 99,
	max7, max7 + v120 + 1,
	// Int64 values that take 9 bytes to store.
	minInt64, minInt64 + 99,
	maxInt64 - 99, maxInt64,
}

var byteArrayTests = [][]byte{
	nil,
	make([]byte, 0),
	{},
	{0},
	{0, 0},
	genBytes(1024),
}

var stringTests = []string{
	"",
	" ",
	"nil",
	"null",
	genString(0),
	genString(1024),
	"☺☻☹",
	"日a本b語ç日ð本Ê語þ日¥本¼語i日©",
	"你好, 世界",
}

func TestReadWrite(t *testing.T) {
	suite.Run(t, &ReadWriteTestSuite{})
}

func (suite *ReadWriteTestSuite) TestReadWriteByte() {
	w := NewWriter()
	tests := []byte{0, 1, math.MaxUint8}
	for _, v := range tests {
		w.WriteByte(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadByte()
		if suite.NoErrorf(err, "ReadByte() got error %v", err) {
			suite.Equalf(in, out, "ReadByte() got unexpected value")
		}
	}

	_, err := r.ReadByte()
	suite.Equalf(io.EOF, err, "ReadByte() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteInt16() {
	w := NewWriter()
	tests := []int16{0, math.MinInt16, math.MaxInt16}
	for _, v := range tests {
		w.WriteInt16(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadInt16()
		if suite.NoErrorf(err, "ReadInt16() got error %v", err) {
			suite.Equalf(in, out, "ReadInt16() got unexpected value")
		}
	}

	_, err := r.ReadInt16()
	suite.Equalf(io.EOF, err, "ReadInt16() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteInt() {
	w := NewWriter()
	tests := []int{0, math.MinInt32, math.MaxInt32}
	for _, v := range tests {
		w.WriteInt(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadInt()
		if suite.NoErrorf(err, "ReadInt() got error %v", err) {
			suite.Equalf(in, out, "ReadInt() got unexpected value")
		}
	}

	_, err := r.ReadInt()
	suite.Equalf(io.EOF, err, "ReadInt() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWritePackedInt() {
	w := NewWriter()
	for _, v := range packedIntTests {
		w.WritePackedInt(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range packedIntTests {
		out, err := r.ReadPackedInt()
		if suite.NoErrorf(err, "ReadPackedInt() got error %v", err) {
			suite.Equalf(in, out, "ReadPackedInt() got unexpected value")
		}
	}

	_, err := r.ReadPackedInt()
	suite.Equalf(io.EOF, err, "ReadPackedInt() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWritePackedLong() {
	w := NewWriter()
	for _, v := range packedLongTests {
		w.WritePackedLong(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range packedLongTests {
		out, err := r.ReadPackedLong()
		if suite.NoErrorf(err, "ReadPackedLong() got error %v", err) {
			suite.Equalf(in, out, "ReadPackedLong() got unexpected value")
		}
	}

	_, err := r.ReadPackedLong()
	suite.Equalf(io.EOF, err, "ReadPackedLong() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteDouble() {
	w := NewWriter()
	tests := []float64{math.SmallestNonzeroFloat64, math.MaxFloat64,
		0.0, -1.1231421132132132, 132124.132132132132}
	for _, v := range tests {
		w.WriteDouble(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadDouble()
		if suite.NoErrorf(err, "ReadDouble() got error %v", err) {
			suite.Equalf(in, out, "ReadDouble() got unexpected value")
		}
	}

	_, err := r.ReadDouble()
	suite.Equalf(io.EOF, err, "ReadDouble() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteString() {
	w := NewWriter()
	for _, v := range stringTests {
		w.WriteString(&v)
	}

	w.WriteString(nil)

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range stringTests {
		out, err := r.ReadString()
		if suite.NoErrorf(err, "ReadString() got error %v", err) {
			suite.Equalf(in, *out, "ReadString() got unexpected value")
		}
	}

	out, err := r.ReadString()
	if suite.NoErrorf(err, "ReadString() got error %v", err) {
		suite.Nilf(out, "ReadString() got unexpected value")
	}

	_, err = r.ReadString()
	suite.Equalf(io.EOF, err, "ReadString() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteBoolean() {
	w := NewWriter()
	tests := []bool{true, false}
	for _, v := range tests {
		w.WriteBoolean(v)
	}

	// Verify that zero byte is parsed as false and non-zero byte is parsed as true.
	w.WriteByte(0)
	w.WriteByte(1)
	w.WriteByte(2)
	tests = append(tests, []bool{false, true, true}...)

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadBoolean()
		if suite.NoErrorf(err, "ReadBoolean() got error %v", err) {
			suite.Equalf(in, out, "ReadBoolean() got unexpected value")
		}
	}

	_, err := r.ReadBoolean()
	suite.Equalf(io.EOF, err, "ReadBoolean() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteByteArray() {
	w := NewWriter()
	for _, v := range byteArrayTests {
		w.WriteByteArray(v)
	}

	// Invalid length of byte array.
	w.WritePackedInt(-2)

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range byteArrayTests {
		out, err := r.ReadByteArray()
		if suite.NoErrorf(err, "ReadByteArray() got error %v", err) {
			suite.Equalf(in, out, "ReadByteArray() got unexpected value")
		}
	}

	// Test invalid byte array.
	_, err := r.ReadByteArray()
	if suite.Errorf(err, "ReadByteArray() should have failed") {
		suite.Containsf(err.Error(), "invalid length of byte array",
			"ReadByteArray() got unexpected error %v", err)
	}

	_, err = r.ReadByteArray()
	suite.Equalf(io.EOF, err, "ReadByteArray() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteByteArrayWithInt() {
	w := NewWriter()
	// WriteByteArrayWithInt accepts a byte slice whose length is non-zero.
	tests := byteArrayTests[3:]
	for _, v := range tests {
		w.WriteByteArrayWithInt(v)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadByteArrayWithInt()
		if suite.NoErrorf(err, "ReadByteArrayWithInt() got error %v", err) {
			suite.Equalf(in, out, "ReadByteArrayWithInt() got unexpected value")
		}
	}

	_, err := r.ReadByteArrayWithInt()
	suite.Equalf(io.EOF, err, "ReadByteArrayWithInt() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteVersion() {
	w := NewWriter()
	tests := byteArrayTests[1:]
	for _, v := range tests {
		ver := types.Version(v)
		w.WriteVersion(ver)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out, err := r.ReadVersion()
		if suite.NoErrorf(err, "ReadVersion() got error %v", err) {
			inVer := types.Version(in)
			suite.Equalf(inVer, out, "ReadVersion() got unexpected value")
		}
	}

	_, err := r.ReadVersion()
	suite.Equalf(io.EOF, err, "ReadVersion() got unexpected error")
}

func (suite *ReadWriteTestSuite) TestReadWriteFieldValue() {
	t := suite.T()

	// Run as sub tests for different field values.

	// Binary value
	binaryTests := byteArrayTests
	t.Run("BinaryValue", func(t *testing.T) {
		for _, v := range binaryTests {
			suite.roundTrip(v)
		}
	})

	// Boolean value.
	booleanTests := []bool{true, false}
	t.Run("BooleanValue", func(t *testing.T) {
		for _, v := range booleanTests {
			suite.roundTrip(v)
		}
	})

	// Integer value.
	integerTests := packedIntTests
	t.Run("IntegerValue", func(t *testing.T) {
		for _, v := range integerTests {
			suite.roundTrip(v)
		}
	})

	// Long value.
	longTests := packedLongTests
	t.Run("LongValue", func(t *testing.T) {
		for _, v := range longTests {
			suite.roundTrip(v)
		}
	})

	// Float value.
	floatTests := []float32{math.SmallestNonzeroFloat32, math.MaxFloat32, 0.0, -1.1231421, 132124.1}
	t.Run("FloatValue", func(t *testing.T) {
		for _, v := range floatTests {
			suite.roundTrip(v)
		}
	})

	// Double value.
	doubleTests := []float64{math.SmallestNonzeroFloat64, math.MaxFloat64,
		0.0, -1.1231421132132132, 132124.132132132132}
	t.Run("DoubleValue", func(t *testing.T) {
		for _, v := range doubleTests {
			suite.roundTrip(v)
		}
	})

	// String value.
	t.Run("StringValue", func(t *testing.T) {
		for _, v := range stringTests {
			suite.roundTrip(v)
		}
	})

	// Array value.
	arrayTests := [][]types.FieldValue{
		{1, 2, 3, 4},
		{int64(1), int64(2), int64(3), int64(4)},
		{"a", "b", "c", "d"},
	}
	t.Run("ArrayValue", func(t *testing.T) {
		for _, v := range arrayTests {
			suite.roundTrip(v)
		}
	})

	// Map value.
	mv1 := &types.MapValue{}
	mv1.Put("int", 1).Put("long", int64(1))
	mv1.Put("float64", float64(3.14))
	mv1.Put("string", "Oracle NoSQL Database")
	mv1.Put("bytes", []byte{1, 2, 3, 4, 5, 6, 7, 8})

	mapTests := []*types.MapValue{mv1}
	t.Run("MapValue", func(t *testing.T) {
		for _, v := range mapTests {
			suite.roundTrip(v)
		}
	})

	t.Run("JavaSerializedString", func(t *testing.T) {
		suite.javaSerializedString()
	})

}

func (suite *ReadWriteTestSuite) roundTrip(in types.FieldValue) {
	wr := NewWriter()
	wr.WriteFieldValue(in)
	br := bytes.NewBuffer(wr.Bytes())
	r := NewReader(br)
	out, err := r.ReadFieldValue()
	if !suite.NoErrorf(err, "ReadFieldValue(value=%v, type=%[1]T) got error %v", in, err) {
		return
	}

	// A float64 value is expected if the input is a float32.
	if f32, ok := in.(float32); ok {
		in = float64(f32)
	}

	if inMV, ok := in.(*types.MapValue); ok {
		outMV, ok := out.(*types.MapValue)
		if suite.Truef(ok, "ReadFieldValue() got value %#[1]v (type %[1]T); want %#[2]v (type %[2]T)", out, in) {
			suite.Truef(reflect.DeepEqual(inMV.Map(), outMV.Map()),
				"ReadFieldValue() got value %#[1]v (type %[1]T); want %#[2]v (type %[2]T)", out, in)
		}

		return
	}

	suite.Truef(reflect.DeepEqual(in, out),
		"ReadFieldValue() got value %#[1]v (type %[1]T); want %#[2]v (type %[2]T)", out, in)

}

func (suite *ReadWriteTestSuite) javaSerializedString() {
	// Test that an nson value serialized by java nson library can
	// be accurately deserialized here, and re-serialized to the same string
	// This base64-encoded string is generated by java nson library tests
	javaString := "BgAAAvgAAAABimFycmF5X3ZhbHVlAAAAAuMAAAAXBgAAABIAAAABiGludF92YWx1ZQT5BFkGAAAAEgAAAAGGaW50X21heAT7f///hgYAAAASAAAAAYZpbnRfbWluBASAAAB3BgAAABYAAAABiWxvbmdfdmFsdWUF/By+mRmbBgAAABcAAAABh2xvbmdfbWF4Bf9/////////hgYAAAAXAAAAAYdsb25nX21pbgUAgAAAAAAAAHcGAAAAGgAAAAGLZG91YmxlX3ZhbHVlAz/zwIMSbpeNBgAAABgAAAABiWRvdWJsZV9tYXgDf+////////8GAAAAGAAAAAGJZG91YmxlX21pbgMAAAAAAAAAAQYAAAAZAAAAAYpkb3VibGVfemVybwMAAAAAAAAAAAYAAAAYAAAAAYlkb3VibGVfTmFOA3/4AAAAAAAABgAAAB0AAAABi251bWJlcl92YWx1ZQmJMjE0NzQ4MzY0NwYAAAAaAAAAAYtzdHJpbmdfdmFsdWUHhmFiY2RlZmcGAAAALAAAAAGJdGltZV92YWx1ZQiaMjAxNy0wNy0xNVQxNToxODo1OS4xMjM0NTZaBgAAAC0AAAABinRpbWVfdmFsdWUxCJoyMDE3LTA3LTE1VDE1OjE4OjU5LjEyMzQ1NloGAAAAKAAAAAGKdGltZV92YWx1ZTIIlTE5MjctMDctMDVUMTU6MDg6MDkuMVoGAAAAJgAAAAGKdGltZV92YWx1ZTMIkzE5MjctMDctMDVUMDA6MDA6MDBaBgAAACYAAAABinRpbWVfdmFsdWU0CJMxOTI3LTA3LTA1VDAwOjAwOjAwWgYAAAARAAAAAYl0cnVlX3ZhbHVlAgEGAAAAEgAAAAGKZmFsc2VfdmFsdWUCAAYAAAAQAAAAAYludWxsX3ZhbHVlCwYAAAARAAAAAYplbXB0eV92YWx1ZQwGAAAALwAAAAGLYmluYXJ5X3ZhbHVlAZthYmNkZWZnQUJDREVGR2FiY2RlZmdBQkNERUZH"

	// base64 decode above string
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(javaString)))
	n, err := base64.StdEncoding.Decode(dst, []byte(javaString))
	if !suite.NoErrorf(err, "Base64 decoding java string got error %v", err) {
		return
	}
	dst = dst[:n]

	// deserialize bytes to FieldValue
	br := bytes.NewBuffer(dst)
	r := NewReader(br)
	out, err := r.ReadFieldValue()
	if !suite.NoErrorf(err, "ReadFieldValue(java bytes) got error %v", err) {
		return
	}

	// expect a MapValue
	_, ok := out.(*types.MapValue)
	suite.Truef(ok, "ReadFieldValue() got value %#[1]v (type %[1]T); want MapValue", out)

	// reserialize, compare bytes
	wr := NewWriter()
	wr.WriteFieldValue(out)

	// compare bytes
	suite.Equalf(dst, wr.Bytes(), "Expected go bytes and java bytes to be the same")

	// compare bse64 encoded bytes
	goString := base64.StdEncoding.EncodeToString(wr.Bytes())
	suite.Equalf(javaString, goString, "Expected go string and java string to be the same")
}

func genBytes(n int) []byte {
	buf := make([]byte, n)
	rand.Read(buf)
	return buf
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func genString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (suite *ReadWriteTestSuite) TestReadWriteStruct() {
	type s1 struct {
		A int32   `nosql:"columna"`
		B float64 `json:"columnb"`
		C string
		d bool
		E *int64
		F []byte
		G []int16
	}
	type s2 struct {
		s1
		S2A interface{}
		S2B time.Time
	}
	w := NewWriter()
	var eval int64 = 123456789
	fval := [8]byte{1, 2, 3, 4, 5, 6, 7, 0}
	gval := [5]int16{0, 0, 0, 2345, -1234}

	tests := []s1{
		{A: 25, B: 1234.56, C: "test string", d: false, E: &eval, F: fval[:], G: gval[:]},
		{A: 0, B: 34.56, C: "", d: false, E: nil, F: nil, G: nil},
		{A: 12345678, B: -123.45, C: "foobar", d: false},
	}
	s2tests := []s2{
		{tests[0], &eval, time.Now().UTC()},
	}
	for _, v := range tests {
		MarshalToWriter(v, w)
	}
	for _, v := range s2tests {
		MarshalToWriter(v, w)
	}

	r := NewReader(bytes.NewBuffer(w.Bytes()))
	for _, in := range tests {
		out := &s1{}
		err := UnmarshalFromReader(out, r)
		if suite.NoErrorf(err, "UnmarshalFromReader() got error %v", err) {
			// TODO: deepEqual
			suite.Equalf(in, *out, "UnmarshalFromReader() got unexpected value")
		}
	}
	for _, in := range s2tests {
		out := &s2{}
		err := UnmarshalFromReader(out, r)
		if suite.NoErrorf(err, "UnmarshalFromReader() got error %v", err) {
			// TODO: deepEqual
			suite.Equalf(in, *out, "UnmarshalFromReader() got unexpected value")
		}
	}

	_, err := r.ReadInt()
	suite.Equalf(io.EOF, err, "ReadInt() got unexpected error")

}
