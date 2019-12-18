//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package jsonutil

import (
	"encoding/json"
	"fmt"
)

const emptyJsonObject = "{}"

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
		return emptyJsonObject
	}
	return string(b)
}

func ToObject(jsonStr string) (v map[string]interface{}, err error) {
	err = json.Unmarshal([]byte(jsonStr), &v)
	return v, err
}

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

// GetNumberFromObject parses the JSON-encoded data, looks for the specified
// field in top level JSON object and returns the value of the field if it
// exists and its value is a JSON Number.
func GetNumber(data []byte, field string) (float64, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return 0, err
	}

	s, ok := v[field]
	if !ok {
		return 0, fmt.Errorf("cannot find the %q field from JSON %q", field, string(data))
	}

	if s, ok := s.(float64); ok {
		return s, nil
	}
	return 0, fmt.Errorf("the value of %q field is not a float64", field)
}

// GetString parses the JSON-encoded data, looks for the specified
// field in top level JSON object and returns the value of the field if it
// exists and its value is a JSON string.
func GetString(data []byte, field string) (string, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return "", err
	}

	s, ok := v[field]
	if !ok {
		return "", fmt.Errorf("cannot find the %q field from JSON %q", field, string(data))
	}

	if s, ok := s.(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("the value of %q field is not a string", field)
}

func GetStringValues(data []byte, fieldNames ...string) (map[string]string, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}

	m := make(map[string]string, len(fieldNames))
	for _, key := range fieldNames {
		value, ok := v[key]
		if !ok {
			return nil, fmt.Errorf("cannot find the %q field from json string %s", key, string(data))
		}

		if value, ok := value.(string); ok {
			m[key] = value
		}
		return nil, fmt.Errorf("the value of %q field is not a string", key)
	}

	return m, nil
}

func GetStringArrayValues(data []byte, name string) ([]string, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}

	value, ok := v[name]
	if !ok {
		return nil, fmt.Errorf("cannot find the %q field from json string %s", name, string(data))
	}

	if array, ok := value.([]interface{}); ok {
		arraySize := len(array)
		strValues := make([]string, 0, arraySize)
		for _, s := range array {
			if s, ok := s.(string); ok {
				strValues = append(strValues, s)
			}
		}

		return strValues, nil
	}
	return nil, fmt.Errorf("the value of %q field is not an array of string", name)

}

func ExpectObject(data interface{}) (map[string]interface{}, error) {
	v, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expects a JSON (Go's map[string]interface{} type), got %T", data)
	}
	return v, nil
}

func ExpectArray(data interface{}) ([]interface{}, error) {
	v, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expects a JSON Array (Go's []interface{} type), got %T", data)
	}
	return v, nil
}

func ExpectString(data interface{}) (string, error) {
	v, ok := data.(string)
	if !ok {
		return "", fmt.Errorf("expects a JSON String (Go's string type), got %T", data)
	}
	return v, nil
}

func ExpectNumber(data interface{}) (float64, error) {
	v, ok := data.(float64)
	if !ok {
		return 0, fmt.Errorf("expects a JSON Number (Go's float64 type), got %T", data)
	}
	return v, nil
}
