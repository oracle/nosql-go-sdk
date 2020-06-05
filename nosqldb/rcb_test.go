//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/assert"
)

func TestSizeOf(t *testing.T) {

	type S1 struct {
		a int32
	}

	type S2 struct {
		b  int64
		s1 *S1
	}

	type S3 struct {
		c  []int
		s2 S2
	}

	m1 := map[string]interface{}{
		"K1": int32(1),
		"K2": int64(2),
	}

	mv := &types.MapValue{}
	mv.Put("K1", int32(1)).Put("K2", int64(2))

	greeting := "hello world"
	tests := []struct {
		v        interface{}
		wantSize int
	}{
		{true, 1},
		{int(0), ptrSize},
		{int8(0), 1},
		{int16(0), 2},
		{int32(0), 4},
		{int64(0), 8},
		{uint8(0), 1},
		{uint16(0), 2},
		{uint32(0), 4},
		{uint64(0), 8},
		{float32(1.2), 4},
		{float64(1.5), 8},
		{[]byte(greeting), int(unsafe.Sizeof(reflect.SliceHeader{})) + len(greeting)},
		{greeting, int(unsafe.Sizeof(reflect.StringHeader{})) + len(greeting)},
		{&greeting, ptrSize + int(unsafe.Sizeof(reflect.StringHeader{})) + len(greeting)},
		{S1{}, 4},
		{S2{}, 8 + ptrSize},
		{S2{b: 3, s1: &S1{a: 5}}, 12 + ptrSize},
		{S3{}, 8 + ptrSize + int(unsafe.Sizeof(reflect.SliceHeader{}))},
		{m1, ptrSize + hmapSize + 2*int(unsafe.Sizeof(reflect.StringHeader{})) + 4},
		// see the types.MapValue struct
		{mv, 1 + int(unsafe.Sizeof(reflect.SliceHeader{})) + ptrSize + hmapSize +
			mv.Len()*int(unsafe.Sizeof(reflect.StringHeader{})) + 16},
	}

	for _, r := range tests {
		actual := sizeOf(r.v)
		if _, ok := r.v.(*types.MapValue); ok {
			assert.GreaterOrEqualf(t, actual, r.wantSize, "unexpected memory consumed for value %v of type %[1]T", r.v)
			continue
		}

		switch reflect.ValueOf(r.v).Kind() {
		case reflect.Slice, reflect.Map:
			assert.GreaterOrEqualf(t, actual, r.wantSize, "unexpected memory consumed for value %v of type %[1]T", r.v)

		default:
			assert.Equalf(t, r.wantSize, actual, "unexpected memory consumed for value %v of type %[1]T", r.v)
		}
	}
}
