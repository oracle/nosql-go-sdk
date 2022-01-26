//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"testing"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func BenchmarkWrite(b *testing.B) {
	w := NewWriter()
	p := []byte{1, 2, 3, 4}
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Write(p)
		w.Reset()
	}
}

func BenchmarkWriteByte(b *testing.B) {
	w := NewWriter()
	b.SetBytes(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteByte(byte(i))
		w.Reset()
	}
}

func BenchmarkWriteInt16(b *testing.B) {
	w := NewWriter()
	b.SetBytes(2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteInt16(int16(i))
		w.Reset()
	}
}

func BenchmarkWriteInt(b *testing.B) {
	w := NewWriter()
	b.SetBytes(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteInt(i)
		w.Reset()
	}
}

func BenchmarkWritePackedInt(b *testing.B) {
	w := NewWriter()
	// The maxinum number of bytes required for a packed int.
	b.SetBytes(maxPackedInt32Length)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WritePackedInt(i)
		w.Reset()
	}
}

func BenchmarkWritePackedLong(b *testing.B) {
	w := NewWriter()
	// The maxinum number of bytes required for a packed long.
	b.SetBytes(maxPackedInt64Length)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WritePackedLong(int64(i))
		w.Reset()
	}
}

func BenchmarkWriteDouble(b *testing.B) {
	w := NewWriter()
	d := float64(3.1415926)
	b.SetBytes(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteDouble(d)
		w.Reset()
	}
}

func BenchmarkWriteString(b *testing.B) {
	w := NewWriter()
	str := "Oracle NoSQL Database"
	b.SetBytes(int64(len(str)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteString(&str)
		w.Reset()
	}
}

func BenchmarkWriteByteArray(b *testing.B) {
	w := NewWriter()
	p := []byte{1, 2, 3, 4}
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteByteArray(p)
		w.Reset()
	}
}

func BenchmarkWriteByteArrayWithInt(b *testing.B) {
	w := NewWriter()
	p := []byte{1, 2, 3, 4}
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteByteArrayWithInt(p)
		w.Reset()
	}
}

func BenchmarkWriteMap(b *testing.B) {
	w := NewWriter()
	mv := &types.MapValue{}
	mv.Put("int", 1).Put("long", int64(1))
	mv.Put("float32", float32(3.14)).Put("float64", float64(3.14))
	mv.Put("string", "Oracle NoSQL Database")
	mv.Put("map", map[string]interface{}{"k1": 1001, "k2": "str1001"})
	mv.Put("array", []types.FieldValue{1, 2, 3, 4})
	mv.Put("bytes", []byte{1, 2, 3, 4, 5, 6, 7, 8})
	n, _ := w.WriteMap(mv)
	b.SetBytes(int64(n))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteMap(mv)
		w.Reset()
	}
}

func BenchmarkWriteArray(b *testing.B) {
	w := NewWriter()
	arr := []types.FieldValue{
		1, int64(1), float32(3.14), float64(3.14),
		"Oracle NoSQL Database",
		map[string]interface{}{"k1": 1001, "k2": "str1001"},
		[]types.FieldValue{1, 2, 3, 4},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	n, _ := w.WriteArray(arr)
	b.SetBytes(int64(n))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteArray(arr)
		w.Reset()
	}
}
