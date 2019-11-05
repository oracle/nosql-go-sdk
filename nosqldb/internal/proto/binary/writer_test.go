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
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func TestSimpleTypeValues(t *testing.T) {
	// Binary
	binTests := [][]byte{
		make([]byte, 0),
		[]byte{0},
		[]byte{0, 0},
		genBytes(1024),
	}
	for _, v := range binTests {
		roundTrip(t, v)
	}

	// Boolean
	boolTests := []bool{true, false}
	for _, v := range boolTests {
		roundTrip(t, v)
	}

	// Integer
	intTests := []int{0, math.MinInt32, math.MaxInt32, -123456789, 123456789}
	for _, v := range intTests {
		roundTrip(t, v)
	}

	// Long
	longTests := []int64{0, math.MinInt64, math.MaxInt64, -1234567890123456789, 1234567890123456789}
	for _, v := range longTests {
		roundTrip(t, v)
	}

	// Float
	floatTests := []float32{math.SmallestNonzeroFloat32, math.MaxFloat32, 0.0, -1.1231421, 132124.1}
	for _, v := range floatTests {
		roundTrip(t, float64(v))
	}

	// Double
	doubleTests := []float64{math.SmallestNonzeroFloat64, math.MaxFloat64, 0.0, -1.1231421132132132, 132124.132132132132}
	for _, v := range doubleTests {
		roundTrip(t, v)
	}

	// String
	stringTests := []string{
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
	for _, v := range stringTests {
		roundTrip(t, v)
	}

	//TODO -- test more type
}

func roundTrip(t *testing.T, in types.FieldValue) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteFieldValue(in)

	br := bytes.NewReader(buf.Bytes())
	r := NewReader(br)
	out, err := r.ReadFieldValue()
	if err != nil {
		t.Errorf("ReadFieldValue() got error %v", err)
	}

	outKind := reflect.TypeOf(out).Kind()
	switch outKind {
	// slice cannot be compared directly
	case reflect.Slice:
		// compare byte slice
		if in, ok := in.([]byte); ok {
			if out, ok := out.([]byte); ok {
				if bytes.Compare(in, out) != 0 {
					t.Errorf("ReadFieldValue() got value %v; want %v", out, in)
				}
			}
			return
		}
	case reflect.Ptr:
		// ReadFieldValue may return *string
		v := reflect.ValueOf(out).Elem()
		if v.Kind() == reflect.String {
			if in, ok := in.(string); ok {
				if in != v.String() {
					t.Errorf("ReadFieldValue() got value %s; want %s", v, in)
					return
				}
			}
		}

	default:
		if in != out {
			t.Errorf("ReadFieldValue() got value %[1]v (type %[1]T); want %[2]v (type %[2]T)", out, in)
		}
	}

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
