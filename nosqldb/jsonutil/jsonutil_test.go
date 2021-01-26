//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
package jsonutil

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// JSONUtilTestSuite test the JSON utility functions.
type JSONUtilTestSuite struct {
	suite.Suite
}

func (suite *JSONUtilTestSuite) TestAsJSON() {
	const lf, indent string = "\n", "  "
	tests := []struct {
		in        interface{}
		out       string
		prettyOut string // Expected output for AsPrettyJSON()
	}{
		{
			in:        nil,
			out:       "null",
			prettyOut: "null",
		},
		{
			in:        map[string]interface{}{},
			out:       `{}`,
			prettyOut: `{}`,
		},
		{
			in:        map[string]int{"k1": 1, "k2": 2},
			out:       `{"k1":1,"k2":2}`,
			prettyOut: `{` + lf + indent + `"k1": 1,` + lf + indent + `"k2": 2` + lf + `}`,
		},
		{
			in:        []interface{}{},
			out:       `[]`,
			prettyOut: `[]`,
		},
		{
			in:        []interface{}{1, 2, 3},
			out:       `[1,2,3]`,
			prettyOut: `[` + lf + indent + `1,` + lf + indent + `2,` + lf + indent + `3` + lf + `]`,
		},
		{
			in:        `"value"`,
			out:       "\"\\\"value\\\"\"",
			prettyOut: "\"\\\"value\\\"\"",
		},
		{
			in:        123,
			out:       "123",
			prettyOut: "123",
		},
	}

	var s1, s2 string
	for _, r := range tests {
		s1 = AsJSON(r.in)
		s2 = AsPrettyJSON(r.in)
		suite.Equalf(r.out, s1, "AsJSON(%v) got unexpected JSON string", r.in)
		suite.Equalf(r.prettyOut, s2, "AsPrettyJSON(%v) got unexpected JSON string", r.in)
	}
}

func (suite *JSONUtilTestSuite) TestToObject() {
	tests := []struct {
		in      string
		out     map[string]interface{}
		wantErr bool
	}{
		{
			in:      `{}`,
			out:     map[string]interface{}{},
			wantErr: false,
		},
		{
			in:      `{"k1": 1, "k2": 2}`,
			out:     map[string]interface{}{"k1": float64(1), "k2": float64(2)},
			wantErr: false,
		},
		{
			in:      `null`,
			out:     nil,
			wantErr: false,
		},
		{
			in:      ``,
			out:     nil,
			wantErr: true,
		},
		// invalid JSON encoding
		{
			in:      `{"a": "hello", "b": world"}`,
			out:     nil,
			wantErr: true,
		},
		// cannot unmarshal array into Go value of type map[string]interface {}
		{
			in:      `[1, 2, 3]`,
			out:     nil,
			wantErr: true,
		},
		// cannot unmarshal number into Go value of type map[string]interface {}
		{
			in:      `123`,
			out:     nil,
			wantErr: true,
		},
		// cannot unmarshal string into Go value of type map[string]interface {}
		{
			in:      `"123"`,
			out:     nil,
			wantErr: true,
		},
	}

	var m map[string]interface{}
	var err error
	for _, r := range tests {
		m, err = ToObject(r.in)
		if r.wantErr {
			suite.Errorf(err, "ToObject(%v) should have failed", r.in)
		} else {
			suite.NoErrorf(err, "ToObject(%v) got error %v", r.in, err)
			suite.Equalf(r.out, m, "ToObject(%v) got unexpected result", r.in)
		}
	}
}

func (suite *JSONUtilTestSuite) TestGetStringFromObject() {
	tests := []struct {
		obj       map[string]interface{}
		field     string
		wantValue string
		wantOK    bool
	}{
		{
			obj:       nil,
			field:     "f1",
			wantValue: "",
			wantOK:    false,
		},
		{
			obj:       map[string]interface{}{},
			field:     "f1",
			wantValue: "",
			wantOK:    false,
		},
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": "v2"},
			field:     "f1",
			wantValue: "v1",
			wantOK:    true,
		},
		// specified field does not exist
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": "v2"},
			field:     "f3",
			wantValue: "",
			wantOK:    false,
		},
		// specified field is not a top-level field
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": map[string]interface{}{"x1": "y1"}},
			field:     "x1",
			wantValue: "",
			wantOK:    false,
		},
		// the associated value is not a JSON string (Go value of type string)
		{
			obj:       map[string]interface{}{"f1": 123, "f2": 456},
			field:     "f1",
			wantValue: "",
			wantOK:    false,
		},
	}

	var s string
	var ok bool
	for _, r := range tests {
		s, ok = GetStringFromObject(r.obj, r.field)
		if suite.Equalf(r.wantOK, ok, "GetStringFromObject(%v) got unexpected result", r.obj) {
			suite.Equalf(r.wantValue, s, "GetStringFromObject(%v) got unexpected result", r.obj)
		}
	}
}

func (suite *JSONUtilTestSuite) TestGetNumberFromObject() {
	tests := []struct {
		obj       map[string]interface{}
		field     string
		wantValue float64
		wantOK    bool
	}{
		{
			obj:       nil,
			field:     "f1",
			wantValue: 0,
			wantOK:    false,
		},
		{
			obj:       map[string]interface{}{},
			field:     "f1",
			wantValue: 0,
			wantOK:    false,
		},
		{
			obj:       map[string]interface{}{"f1": float64(123), "f2": float64(456)},
			field:     "f1",
			wantValue: 123,
			wantOK:    true,
		},
		// specified field does not exist
		{
			obj:       map[string]interface{}{"f1": float64(123), "f2": float64(456)},
			field:     "f3",
			wantValue: 0,
			wantOK:    false,
		},
		// specified field is not a top-level field
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": map[string]interface{}{"x1": float64(123)}},
			field:     "x1",
			wantValue: 0,
			wantOK:    false,
		},
		// the associated value is not a JSON Number (Go value of type float64)
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": "v2"},
			field:     "f1",
			wantValue: 0,
			wantOK:    false,
		},
	}

	var f float64
	var ok bool
	for _, r := range tests {
		f, ok = GetNumberFromObject(r.obj, r.field)
		if suite.Equalf(r.wantOK, ok, "GetNumberFromObject(%v) got unexpected result", r.obj) {
			suite.Equalf(r.wantValue, f, "GetNumberFromObject(%v) got unexpected result", r.obj)
		}
	}
}

func (suite *JSONUtilTestSuite) TestGetArrayFromObject() {
	tests := []struct {
		obj       map[string]interface{}
		field     string
		wantValue []interface{}
		wantOK    bool
	}{
		{
			obj:       nil,
			field:     "f1",
			wantValue: nil,
			wantOK:    false,
		},
		{
			obj:       map[string]interface{}{},
			field:     "f1",
			wantValue: nil,
			wantOK:    false,
		},
		{
			obj:       map[string]interface{}{"f1": []interface{}{1, 2, 3}, "f2": []interface{}{4, 5}},
			field:     "f1",
			wantValue: []interface{}{1, 2, 3},
			wantOK:    true,
		},
		// specified field does not exist
		{
			obj:       map[string]interface{}{"f1": []interface{}{1, 2, 3}, "f2": []interface{}{4, 5}},
			field:     "f3",
			wantValue: nil,
			wantOK:    false,
		},
		// specified field is not a top-level field
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": map[string]interface{}{"x1": []interface{}{4, 5}}},
			field:     "x1",
			wantValue: nil,
			wantOK:    false,
		},
		// the associated value is not a JSON Array (Go value of type []interface{})
		{
			obj:       map[string]interface{}{"f1": "v1", "f2": "v2"},
			field:     "f1",
			wantValue: nil,
			wantOK:    false,
		},
	}

	var a []interface{}
	var ok bool
	for _, r := range tests {
		a, ok = GetArrayFromObject(r.obj, r.field)
		if suite.Equalf(r.wantOK, ok, "GetArrayFromObject(%v) got unexpected result", r.obj) {
			suite.Equalf(r.wantValue, a, "GetArrayFromObject(%v) got unexpected result", r.obj)
		}
	}
}

func (suite *JSONUtilTestSuite) TestExpectObject() {
	tests := []struct {
		data      interface{}
		wantValue map[string]interface{}
		wantErr   bool
	}{
		{
			data:      map[string]interface{}{"k1": 1, "k2": 2},
			wantValue: map[string]interface{}{"k1": 1, "k2": 2},
			wantErr:   false,
		},
		{
			data:      nil,
			wantValue: nil,
			wantErr:   true,
		},
		{
			data:      map[string]int{"k1": 1, "k2": 2},
			wantValue: nil,
			wantErr:   true,
		},
		{
			data:      123,
			wantValue: nil,
			wantErr:   true,
		},
		{
			data:      "value",
			wantValue: nil,
			wantErr:   true,
		},
		{
			data:      []interface{}{1, 2, 3},
			wantValue: nil,
			wantErr:   true,
		},
	}

	var obj map[string]interface{}
	var err error
	for _, r := range tests {
		obj, err = ExpectObject(r.data)
		if r.wantErr {
			suite.Errorf(err, "ExpectObject(data=%v) should have failed", r.data)
		} else {
			suite.NoErrorf(err, "ExpectObject(data=%v) got error %v", r.data, err)
			suite.Equalf(r.wantValue, obj, "ExpectObject(data=%v) got unexpected result", r.data)
		}
	}
}

func (suite *JSONUtilTestSuite) TestExpectString() {
	tests := []struct {
		data      interface{}
		wantValue string
		wantErr   bool
	}{
		{
			data:      "",
			wantValue: "",
			wantErr:   false,
		},
		{
			data:      "value",
			wantValue: "value",
			wantErr:   false,
		},
		{
			data:      nil,
			wantValue: "",
			wantErr:   true,
		},
		{
			data:      map[string]interface{}{"k1": 1, "k2": 2},
			wantValue: "",
			wantErr:   true,
		},
		{
			data:      123,
			wantValue: "",
			wantErr:   true,
		},
		{
			data:      []interface{}{1, 2, 3},
			wantValue: "",
			wantErr:   true,
		},
	}

	var s string
	var err error
	for _, r := range tests {
		s, err = ExpectString(r.data)
		if r.wantErr {
			suite.Errorf(err, "ExpectString(data=%v) should have failed", r.data)
		} else {
			suite.NoErrorf(err, "ExpectString(data=%v) got error %v", r.data, err)
			suite.Equalf(r.wantValue, s, "ExpectString(data=%v) got unexpected result", r.data)
		}
	}
}

func TestJSONUtil(t *testing.T) {
	suite.Run(t, new(JSONUtilTestSuite))
}
