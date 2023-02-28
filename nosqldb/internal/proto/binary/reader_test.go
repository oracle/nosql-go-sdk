//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"bytes"
	"testing"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func BenchmarkRead(b *testing.B) {
	buf := []byte{1, 2, 3, 4}
	r := NewReader(bytes.NewBuffer(buf))
	n := len(buf)
	p := make([]byte, n)
	b.SetBytes(int64(n))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Read(p)
	}
}

func BenchmarkReadByte(b *testing.B) {
	buf := []byte{1}
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadByte()
	}
}

func BenchmarkReadByteArray(b *testing.B) {
	w := NewWriter()
	bs := []byte{1, 2, 3, 4}
	w.WriteByteArray(bs)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadByteArray()
	}
}

func BenchmarkReadByteArrayWithInt(b *testing.B) {
	w := NewWriter()
	bs := []byte{1, 2, 3, 4}
	w.WriteByteArrayWithInt(bs)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadByteArrayWithInt()
	}
}

func BenchmarkReadBoolean(b *testing.B) {
	w := NewWriter()
	w.WriteBoolean(true)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadBoolean()
	}
}

func BenchmarkReadInt(b *testing.B) {
	w := NewWriter()
	w.WriteInt(10)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadInt()
	}
}

func BenchmarkReadInt16(b *testing.B) {
	w := NewWriter()
	w.WriteInt16(int16(10))
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadInt16()
	}
}

func BenchmarkReadPackedInt(b *testing.B) {
	w := NewWriter()
	w.WritePackedInt(10)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadPackedInt()
	}
}

func BenchmarkReadPackedLong(b *testing.B) {
	w := NewWriter()
	w.WritePackedLong(int64(10))
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadPackedLong()
	}
}

func BenchmarkReadDouble(b *testing.B) {
	w := NewWriter()
	w.WriteDouble(float64(3.1415926))
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadDouble()
	}
}

func BenchmarkReadString(b *testing.B) {
	w := NewWriter()
	s := "Oracle NoSQL Database"
	w.WriteString(&s)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadString()
	}
}

func BenchmarkReadMap(b *testing.B) {
	w := NewWriter()
	mv := &types.MapValue{}
	mv.Put("int", 1).Put("long", int64(1))
	mv.Put("float32", float32(3.14)).Put("float64", float64(3.14))
	mv.Put("string", "Oracle NoSQL Database")
	mv.Put("map", map[string]interface{}{"k1": 1001, "k2": "str1001"})
	mv.Put("array", []types.FieldValue{1, 2, 3, 4})
	mv.Put("bytes", []byte{1, 2, 3, 4, 5, 6, 7, 8})
	w.WriteMap(mv)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadMap()
	}
}

func BenchmarkReadArray(b *testing.B) {
	w := NewWriter()
	arr := []types.FieldValue{
		1, int64(1), float32(3.14), float64(3.14),
		"Oracle NoSQL Database",
		map[string]interface{}{"k1": 1001, "k2": "str1001"},
		[]types.FieldValue{1, 2, 3, 4},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	w.WriteArray(arr)
	buf := w.Bytes()
	r := NewReader(bytes.NewBuffer(buf))
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadArray()
	}
}
