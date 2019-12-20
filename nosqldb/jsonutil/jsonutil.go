//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// Package jsonutil provides utility functions for manipulating JSON.
package jsonutil

import (
	"encoding/json"
	"fmt"
)

const emptyJSONObject = "{}"

// AsJSON encodes the specified value into a json string.
func AsJSON(v interface{}) string {
	return asJSONString(v, false)
}

// AsPrettyJSON encodes the specified value into a json string, adding
// appropriate indents in the returned string.
func AsPrettyJSON(v interface{}) string {
	return asJSONString(v, true)
}

func asJSONString(v interface{}, pretty bool) string {
	var b []byte
	var err error
	if pretty {
		b, err = json.MarshalIndent(v, "", "  ")
	} else {
		b, err = json.Marshal(v)
	}
	if err != nil {
		return emptyJSONObject
	}
	return string(b)
}

// ToObject decodes the jsonStr as a JSON object.
func ToObject(jsonStr string) (v map[string]interface{}, err error) {
	err = json.Unmarshal([]byte(jsonStr), &v)
	return v, err
}

// GetStringFromObject retrieves the string value associated with the specified
// top-level fields from a JSON object.
func GetStringFromObject(m map[string]interface{}, field string) (s string, ok bool) {
	if m == nil {
		return
	}
	var v interface{}
	if v, ok = m[field]; !ok {
		return
	}
	s, ok = v.(string)
	return
}

// GetNumberFromObject retrieves the float64 value associated with the specified
// top-level fields from a JSON object.
func GetNumberFromObject(m map[string]interface{}, field string) (f float64, ok bool) {
	if m == nil {
		return
	}
	var v interface{}
	if v, ok = m[field]; !ok {
		return
	}
	f, ok = v.(float64)
	return
}

// GetArrayFromObject retrieves an array value associated with the specified
// top-level fields from a JSON object.
func GetArrayFromObject(m map[string]interface{}, field string) (a []interface{}, ok bool) {
	if m == nil {
		return
	}
	var v interface{}
	if v, ok = m[field]; !ok {
		return
	}
	a, ok = v.([]interface{})
	return
}

// ExpectObject checks if the specified data represents a JSON object.
func ExpectObject(data interface{}) (map[string]interface{}, error) {
	v, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expects a JSON (Go's map[string]interface{} type), got %T", data)
	}
	return v, nil
}

// ExpectString checks if the specified data represents a JSON string value.
func ExpectString(data interface{}) (string, error) {
	v, ok := data.(string)
	if !ok {
		return "", fmt.Errorf("expects a JSON String (Go's string type), got %T", data)
	}
	return v, nil
}
