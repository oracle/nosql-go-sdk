//
// Copyright (c) 2019, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package types

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

type MapValueTestSuite struct {
	suite.Suite
}

// TestNewMapValue tests the NewMapValue() function that creates an unordered MapValue.
func (suite *MapValueTestSuite) TestNewMapValue() {
	suite.doMapValueTest(false)
}

// TestNewOrderedMapValue tests the NewOrderedMapValue() function that creates an ordered MapValue.
func (suite *MapValueTestSuite) TestNewOrderedMapValue() {
	suite.doMapValueTest(true)
}

// TestNewMapValueFromJSON tests the NewMapValueFromJSON() function that
// creates an unordered MapValue from the specified JSON.
func (suite *MapValueTestSuite) TestNewMapValueFromJSON() {
	var m *MapValue
	var err error

	testCases := []struct {
		jsonStr   string
		ok        bool
		expectLen int
	}{
		// valid JSON.
		{`{}`, true, 0},
		{`null`, true, 0},
		{`{"k1": ""}`, true, 1},
		{`{"k1": 1, "k2": "str", "k3": 3.14, "k4": [1,2,3,4], "k5": {"e1": 1, "e2": 2}}`, true, 5},
		// invalid or not supported JSON
		{``, false, 0},
		{`"null"`, false, 0},
		{`nil`, false, 0},
		{`"nil"`, false, 0},
		{`[]`, false, 0}, // JSON Array is not supported for MapValue.
		{`{"k1"}`, false, 0},
		{`{[]}`, false, 0},
		{`{"k1": 1, "k2": 2`, false, 0},
		{`{"k1": 1, }`, false, 0},
		{`{123: "str"}`, false, 0},
	}
	for i, r := range testCases {
		m, err = NewMapValueFromJSON(r.jsonStr)
		if r.ok {
			suite.NoErrorf(err, "Testcase %d: NewMapValueFromJSON(str=%q): %v", i+1, r.jsonStr, err)
			suite.Equalf(r.expectLen, m.Len(), "Testcase %d: unexpected length of MapValue", i+1)

		} else {
			suite.Errorf(err, "Testcase %d: NewMapValueFromJSON(str=%q) should have failed", i+1, r.jsonStr)
			suite.Nilf(m, "Testcase %d: NewMapValueFromJSON(str=%q) should have returned nil", i+1, r.jsonStr)
		}
	}

	var str string
	var k string
	var v interface{}
	str = `{` +
		`"MaxInt32": 2147483647, "MinInt32": -2147483648, ` +
		`"MaxInt64": 9223372036854775807, "MinInt64": -9223372036854775808, ` +
		`"MaxFloat32": 3.4028234663852886e+38, ` +
		`"MaxFloat64": 1.7976931348623157e+308, ` +
		`"EmptyStr": "", "Str": "testStr", ` +
		`"JsonNull": null` +
		`}`
	m, err = NewMapValueFromJSON(str)
	suite.NoErrorf(err, "NewMapValueFromJSON(str=%q): %v", str, err)

	k = "MaxInt32"
	i, _ := m.GetInt(k)
	suite.Equalf(2147483647, i, "GetInt(key=%s) got wrong value", k)

	k = "MinInt32"
	i, _ = m.GetInt(k)
	suite.Equalf(-2147483648, i, "GetInt(key=%s) got wrong value", k)

	k = "MaxInt64"
	i64, _ := m.GetInt64(k)
	suite.Equalf(int64(9223372036854775807), i64, "GetInt64(key=%s) got wrong value", k)

	k = "MinInt64"
	i64, _ = m.GetInt64(k)
	suite.Equalf(int64(-9223372036854775808), i64, "GetInt64(key=%s) got wrong value", k)

	k = "MaxFloat32"
	f64, _ := m.GetFloat64(k)
	suite.Equalf(float64(3.4028234663852886e+38), f64, "GetFloat64(key=%s) got wrong value", k)

	k = "MaxFloat64"
	f64, _ = m.GetFloat64(k)
	suite.Equalf(float64(1.7976931348623157e+308), f64, "GetFloat64(key=%s) got wrong value", k)

	k = "EmptyStr"
	s, _ := m.GetString(k)
	suite.Equalf("", s, "GetString(key=%s) got wrong value", k)

	k = "Str"
	s, _ = m.GetString(k)
	suite.Equalf("testStr", s, "GetString(key=%s) got wrong value", k)

	k = "JsonNull"
	v, ok := m.Get(k)
	suite.Truef(ok, "Get(key=%s) should have returned true", k)
	suite.Nilf(v, "Get(key=%s) got wrong value", k)
}

// TestGet tests GetXXX methods of MapValue.
func (suite *MapValueTestSuite) TestGet() {
	values := []interface{}{
		1001,
		int64(1002),
		float64(3.1415),
		"string_value",
		[]byte("abcdefg"),
		[...]string{"A", "B", "C", "D"},
		[]string{"E", "F", "G", "H"},
		map[string]interface{}{"k1": "v1", "k2": "v2"},
	}

	m := &MapValue{}
	for i, v := range values {
		k := "key" + strconv.Itoa(i+1)
		m.Put(k, v)
	}

	suite.Equalf(len(values), m.Len(), "wrong length of MapValue")

	// Flags that represent if GetXXX method should be called on a key for nagative tests.
	const (
		chkInt     uint8 = 1 << iota // 1
		chkInt64                     // 2
		chkFloat64                   // 4
		chkString                    // 8
		chkAll                       // 16
	)

	var ok bool
	for j, v := range values {
		check := chkAll - 1 // 0x1111
		k := "key" + strconv.Itoa(j+1)
		switch val := v.(type) {
		case int:
			i, ok := m.GetInt(k)
			suite.Truef(ok, "GetInt(key=%s) failed", k)
			suite.Equalf(val, i, "GetInt(key=%s) got wrong value", k)
			check &^= chkInt

		case int64:
			i64, ok := m.GetInt64(k)
			suite.Truef(ok, "GetInt64(key=%s) failed", k)
			suite.Equalf(val, i64, "GetInt64(key=%s) got wrong value", k)
			check &^= chkInt64

		case float64:
			f64, ok := m.GetFloat64(k)
			suite.Truef(ok, "GetFloat64(key=%s) failed", k)
			suite.Equalf(val, f64, "GetFloat64(key=%s) got wrong value", k)
			check &^= chkFloat64

		case string:
			str, ok := m.GetString(k)
			suite.Truef(ok, "GetString(key=%s) failed", k)
			suite.Equalf(val, str, "GetString(key=%s) got wrong value", k)
			check &^= chkString
		}

		// Do negative tests.
		if check&chkInt != 0 {
			_, ok = m.GetInt(k)
			suite.Falsef(ok, "GetInt(key=%s) should have failed", k)
		}

		if check&chkInt64 != 0 {
			_, ok = m.GetInt64(k)
			suite.Falsef(ok, "GetInt64(key=%s) should have failed", k)
		}

		if check&chkFloat64 != 0 {
			_, ok = m.GetFloat64(k)
			suite.Falsef(ok, "GetFloat64(key=%s) should have failed", k)
		}

		if check&chkString != 0 {
			_, ok = m.GetString(k)
			suite.Falsef(ok, "GetString(key=%s) should have failed", k)
		}
	}

}

func (suite *MapValueTestSuite) doMapValueTest(ordered bool) {
	var m *MapValue
	var mv map[string]interface{}
	var ok bool
	var k string
	var v interface{}

	if ordered {
		m = NewOrderedMapValue()
		suite.Truef(m.IsOrdered(), "expect an ordered MapValue")
	} else {
		m = &MapValue{}
		suite.Falsef(m.IsOrdered(), "expect an unordered MapValue")
	}

	suite.Equalf(0, m.Len(), "wrong length of MapValue")

	// Try to get values from an empty MapValue.
	_, ok = m.Get("k1")
	suite.Falsef(ok, "Get(key=k1) from an empty MapValue should have returned false.")

	if ordered {
		_, _, ok = m.GetByIndex(1)
		suite.Falsef(ok, "Get(index=1) from an empty MapValue should have returned false.")
	}

	// Put
	n := 20
	for i := 1; i <= n; i++ {
		k = fmt.Sprintf("k%d", i)
		v = fmt.Sprintf("v%d", i)
		m.Put(k, v)
	}

	suite.Equalf(n, m.Len(), "wrong length")
	_, ok = m.Get("k0")
	suite.Falsef(ok, "value indexed by k0 should not exists")

	// Get
	for i := 1; i <= n; i++ {
		k = fmt.Sprintf("k%d", i)
		expectVal := fmt.Sprintf("v%d", i)
		actualVal, ok := m.Get(k)
		suite.Truef(ok, "value indexed by %s should exists", k)
		suite.Equalf(expectVal, actualVal, "unexpected value for key=%s", k)
	}

	// Delete
	numDeleted := 0
	for i := 1; i <= n/2; i++ {
		if i%2 == 0 {
			continue
		}
		k = fmt.Sprintf("k%d", i)
		m.Delete(k)
		// Delete with k again, should be no-op
		m.Delete(k)
		numDeleted++
	}
	suite.Equalf(n-numDeleted, m.Len(), "unexpected length")

	for i := 1; i <= n; i++ {
		k = fmt.Sprintf("k%d", i)
		_, ok = m.Get(k)
		if i <= n/2 {
			if i%2 == 0 {
				suite.Truef(ok, "value indexed by %s should exists", k)
			} else {
				suite.Falsef(ok, "value indexed by %s should have been deleted.", k)
			}
		} else {
			suite.Truef(ok, "value indexed by %s should exists", k)
		}
	}

	if !ordered {
		// GetByIndex() is not supported on an unordered MapValue
		_, _, ok = m.GetByIndex(1)
		suite.Falsef(ok, "GetByIndex() should not be supported on an unordered MapValue")

	} else {

		var expKey string
		var expVal interface{}
		// Call GetByIndex on an ordered MapValue
		for i := 1; i <= m.Len(); i++ {
			k, v, ok = m.GetByIndex(i)
			if i <= numDeleted {
				expKey = fmt.Sprintf("k%d", i*2)
				expVal = fmt.Sprintf("v%d", i*2)
			} else {
				expKey = fmt.Sprintf("k%d", i-numDeleted+n/2)
				expVal = fmt.Sprintf("v%d", i-numDeleted+n/2)
			}

			suite.Truef(ok, "GetByIndex(%d) should exists", i)
			suite.Equalf(expKey, k, "unexpected key returned from GetByIndex(%d)", i)
			suite.Equalf(expVal, v, "unexpected value returned from GetByIndex(%d)", i)
		}

		invalidIndexes := []int{-1, 0, m.Len() + 1}
		for _, i := range invalidIndexes {
			_, _, ok = m.GetByIndex(i)
			suite.Falsef(ok, "GetByIndex(%d) should have failed", i)
		}
	}

	// Check the underlying map
	mv = m.Map()
	suite.Equalf(m.Len(), len(mv), "unexpected length")
	v2 := mv["k2"]
	suite.Equalf("v2", v2, "unexpected value for key=k2")

	if !ordered {
		nestedMapVal := map[string]interface{}{
			"nestedK1": 1001,
			"nestedK2": 1002,
			"nestedK3": 1003,
		}
		mv = map[string]interface{}{
			"mk1": "mv1",
			"mk2": 2,
			"mk3": 3.1415,
			"mk4": []int{10, 20, 30, 40},
			"mk5": nestedMapVal,
		}
		// create a MapValue with the specified map
		m = NewMapValue(mv)

		suite.Equalf(5, m.Len(), "wrong length")
		v, _ = m.Get("mk2")
		suite.Equalf(2, v, "unexpected value for key=mk2")
		v, _ = m.Get("mk3")
		suite.Equalf(3.1415, v, "unexpected value for key=mk3")
		v, _ = m.Get("mk4")
		suite.Equalf([]int{10, 20, 30, 40}, v, "unexpected value for key=mk4")
		v, _ = m.Get("mk5")
		suite.Equalf(nestedMapVal, v, "unexpected value for key=mk5")
	}
}

func TestMapValue(t *testing.T) {
	suite.Run(t, &MapValueTestSuite{})
}
