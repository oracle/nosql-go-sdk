//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package types defines types and values used to represent and manipulate data
// in the Oracle NoSQL Database. A table in Oracle NoSQL database is defined
// using a fixed schema that describes the data that table will hold. Application
// developers should know the data types for each field in the table. The values
// provided to the table field in an application must conform to the table schema,
// Go driver makes best effort to map the provided Go values to table field values,
// the values are validated against the table schema on the backend Oracle NoSQL
// Database and if they do not conform an error is reported.
//
// On input, the mappings between Go driver types and database types:
//
//   Go Driver Types                              Database Types
//   =========================================    ===============
//   byte, int8, uint8, int16, uint16, int32      INTEGER
//   uint32 (0 <= x <= math.MaxInt32)
//   int (math.MinInt32 <= x <= math.MaxInt32)
//   -----------------------------------------    ---------------
//   int64                                        LONG
//   uint64 (0 <= x <= math.MaxInt64)
//   uint32 (math.MaxInt32 < x <= math.MaxInt64)
//   uint (math.MaxInt32 < x <= math.MaxInt64)
//   int (math.MaxInt32 < x <= math.MaxInt64 or
//        math.MinInt64 <= x < math.MinInt32)
//   -----------------------------------------    ---------------
//   *big.Rat                                     NUMBER
//   uint64 (x > math.MaxInt64)
//   uint (x > math.MaxInt64)
//   -----------------------------------------    ---------------
//   float32, float64                             DOUBLE
//   -----------------------------------------    ---------------
//   *string                                      STRING
//   string
//   -----------------------------------------    ---------------
//   []byte                                       BINARY
//   -----------------------------------------    ---------------
//   bool                                         BOOLEAN
//   -----------------------------------------    ---------------
//   *MapValue                                    MAP
//   map[string]interface{}
//   -----------------------------------------    ---------------
//   []FieldValue                                 ARRAY
//   []interface{}
//   -----------------------------------------    ---------------
//   time.Time                                    TIMESTAMP
//   string representation of time.Time in the
//   form "2006-01-02T15:04:05.999999999"
//
// On output, the mappings between database types and Go driver types:
//
//   Database Types      Go Driver Types
//   ==============      ================
//   INTEGER             int
//   LONG                int64
//   NUMBER              *big.Rat
//   DOUBLE              float64
//   STRING              *string
//   BINARY              []byte
//   BOOLEAN             bool
//   MAP                 *MapValue
//   ARRAY               []FieldValue
//   TIMESTAMP           time.Time
//
// Note that there are several database types that do not have direct equivalents.
//
// 1. ENUM. Enumerations require a schema. When an application fetches a row with
// an enumeration its value will be mapped to a string value. On input, a string
// value must be created to represent an ENUM.
//
// 2. FIXED_BINARY. This is a specialization of BINARY that uses a fixed number
// of bytes. It is mapped to []byte that is validated on input.
//
// 3. RECORD. This is a fixed-schema map. It is represented as a MapValue,
// which is more flexible, but is validated on input.
//
// The database types of MAP and ARRAY are fixed-type in that they contain a
// single type, for example a MAP of INTEGER, an ARRAY of LONG. Although the
// corresponding Go driver types map[string]interface{} and []interface{} are
// not fixed-type, on input the types of the elements of these collections must
// match the table schema.
//
// JSON Mappings
//
// JSON is commonly used as a format for data and there are also well-defined
// mappings between JSON types and Go driver types. It is a common pattern to
// construct a row (MapValue) from JSON and generate JSON from a row. Methods on
// the MapValue type make this pattern easy to use. The following table defines
// the mappings from JSON types to Go driver types.
//
//   JSON Types          Go Driver Types
//   ============        ==============
//   ARRAY               []FieldValue
//   BOOLEAN             bool
//   NUMBER              int, int64, float64 or *big.Rat
//   OBJECT              *MapValue
//   STRING              string
//
// JSON has only a single numeric type. By default JSON numbers will be mapped to
// the most appropriate numeric type (int, int64, float64 or *big.Rat) depending
// on the value.
//
// The special values JSONNullValueInstance, NullValueInstance and EmptyValueInstance
// are served as output of the queries for the Oracle NoSQL database, they should not
// be used as input values.
package types

import (
	"encoding/json"
	"strings"
)

// FieldValue represents a field value of NoSQL database tables.
// This is an empty interface.
type FieldValue interface{}

// JSONNullValue represents an explicit JSON null value in a JSON object or array.
// On input this type can only be used in a table field of type JSON.
//
// This should be used as an immutable singleton object.
type JSONNullValue struct{}

// MarshalJSON returns the JSON encoding of JSONNullValue.
//
// This implements the json.Marshaler interface.
func (jn *JSONNullValue) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

// NullValue represents a null value or missing value in a fully-typed schema.
//
// This type only exists in index keys on a fully-typed field and never inside indexed JSON.
type NullValue struct{}

// MarshalJSON returns the JSON encoding of NullValue.
//
// This implements the json.Marshaler interface.
func (n *NullValue) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

// EmptyValue represents an empty value.
//
// This type is only relevant inside a table field of type JSON and only when
// that field is indexed.
//
// It is used in index keys to represent missing value for the indexed field.
//
// It is different from a JSONNullValue value, which is a concrete value.
//
// It is also different from NullValue which represents a null or missing field
// in a fully-typed field in the table schema as opposed to a JSON field.
type EmptyValue struct{}

// MarshalJSON returns the JSON encoding of EmptyValue.
//
// This implements the json.Marshaler interface.
func (e *EmptyValue) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

var (
	// JSONNullValueInstance represents an instance of JSONNullValue.
	// This should be used as an immutable singleton object.
	JSONNullValueInstance = &JSONNullValue{}

	// NullValueInstance represents an instance of NullValue.
	// This should be used as an immutable singleton object.
	NullValueInstance = &NullValue{}

	// EmptyValueInstance represents an instance of EmptyValue.
	// This should be used as an immutable singleton object.
	EmptyValueInstance = &EmptyValue{}
)

// MapValue represents a row in a NoSQL Database table. A top-level row is
// always a MapValue instance that contains FieldValue objects which may be
// instances of atomic types, embedded MapValue or an array of aforementioned
// types, creating a structured row.
//
// MapValue is also used to represent key values used in get operations as well
// as nested maps or records within a row.
//
// Field names in a MapValue are case-sensitive string values with no duplicates.
// On input, MapValues of any structure can be created, but when put into a
// table using a method such as Client.Put() they must conform to the schema of
// the target table or an error will be returned.
// Note that in the context of a RECORD field in a table schema, field names are
// treated as case-insensitive. If a MapValue represents JSON, field names are
// case-sensitive.
//
// When a MapValue is received on output, such as from Client.Get() and
// Client.Query(), the value will always conform to the schema of the table from
// which the value was received or the implied schema of a query projection.
type MapValue struct {
	// m represents a map that stores key/value pairs.
	m map[string]interface{}

	// keepInsertionOrder specifies whether to keep insertion order for the map.
	keepInsertionOrder bool

	// keys is a slice of string that contains keys in insertion order.
	keys []string
}

// NewMapValue creates a MapValue with the specified map m.
func NewMapValue(m map[string]interface{}) *MapValue {
	return &MapValue{
		m:                  m,
		keepInsertionOrder: false,
	}
}

// NewMapValueFromJSON creates a MapValue from the specified JSON string.
// It returns an error if jsonStr is not a valid JSON encoding.
//
// It unmarshals a number specified in jsonStr as a json.Number instead of as a float64.
// Non-numeric numbers such as NaN, -Inf, +Inf are not allowed.
func NewMapValueFromJSON(jsonStr string) (*MapValue, error) {
	var m map[string]interface{}
	d := json.NewDecoder(strings.NewReader(jsonStr))
	d.UseNumber()
	if err := d.Decode(&m); err != nil {
		return nil, err
	}

	return NewMapValue(m), nil
}

// NewOrderedMapValue creates an ordered MapValue which keeps insertion orders
// when key/value pairs are inserted into MapValue over the Put() method.
// An ordered MapValue guarantees that key/value pairs can be retrieved over the
// GetByIndex(i int) method by specifying the insertion order.
func NewOrderedMapValue() *MapValue {
	return &MapValue{
		m:                  make(map[string]interface{}),
		keepInsertionOrder: true,
		keys:               make([]string, 0, 16),
	}
}

// Len returns the number of key/value pairs stored in MapValue.
func (m *MapValue) Len() int {
	return len(m.m)
}

// IsOrdered reports whether the MapValue keeps insertion order.
func (m *MapValue) IsOrdered() bool {
	return m.keepInsertionOrder
}

// Map returns the underlying map of MapValue.
func (m *MapValue) Map() map[string]interface{} {
	return m.m
}

// MarshalJSON returns MapValue m as the JSON encoding of m.
//
// This implements the json.Marshaler interface.
func (m *MapValue) MarshalJSON() ([]byte, error) {
	if m == nil || m.m == nil {
		return []byte("null"), nil
	}

	return json.Marshal(m.m)
}

// Put inserts a value v indexed by key k into MapValue.
// If MapValue is ordered, it keeps track of the insertion order.
func (m *MapValue) Put(k string, v interface{}) *MapValue {
	if m.m == nil {
		m.m = make(map[string]interface{})
	}

	if m.keepInsertionOrder {
		if _, ok := m.Get(k); !ok {
			m.keys = append(m.keys, k)
		}
	}

	m.m[k] = v
	return m
}

// Get looks for a value v with specified key k. If it finds the value, it
// returns that value and sets ok to true. Otherwise, it returns nil and sets ok
// to false.
func (m *MapValue) Get(k string) (v interface{}, ok bool) {
	v, ok = m.m[k]
	return
}

// GetByIndex only applies to an ordered MapValue. It looks for key k and value
// v with specified index idx, which is 1-based index representing the insertion
// order. For example, the index of first key/value pairs inserted into MapValue
// is 1, the index of second key/value pairs inserted is 2, and so on.
// If the method finds k and v, it returns them and sets ok to true. Otherwise,
// it returns zero value for k and v, and sets ok to false.
func (m *MapValue) GetByIndex(idx int) (k string, v interface{}, ok bool) {
	if !m.keepInsertionOrder {
		return
	}

	if idx < 1 || idx > len(m.keys) {
		return
	}

	k = m.keys[idx-1]
	v, ok = m.Get(k)
	return
}

// Delete removes the value indexed by k from MapValue.
// If MapValue is ordered and the desired value is removed, the insertion order
// is adjusted accordingly to reflect the changes. The insertion order of
// key/value pairs that were inserted after the removed one is advanced by 1.
//
// It is not recommended to use Delete on an ordered MapValue frequently as it
// is inefficient to keep track of and adjust insertion order after a value is
// deleted.
func (m *MapValue) Delete(k string) {
	var ok bool
	if m.keepInsertionOrder {
		_, ok = m.Get(k)
	}

	delete(m.m, k)

	if !ok {
		return
	}

	n := len(m.keys)
	for i := 0; i < n; i++ {
		if k == m.keys[i] {
			switch i {
			case n - 1:
				m.keys = m.keys[:i]
			default:
				m.keys = append(m.keys[:i], m.keys[i+1:]...)
			}
			break
		}
	}
}

// GetString returns the string value s associated with the specified key k.
// If the value does not exist, or is not a string value, this method returns
// an empty string and sets ok to false.
func (m *MapValue) GetString(k string) (s string, ok bool) {
	v, ok := m.Get(k)
	if !ok {
		return
	}

	s, ok = v.(string)
	return
}

// GetInt returns the int value i associated with the specified key k.
// If the value does not exist, or is not an int value, this method returns 0
// and sets ok to false.
func (m *MapValue) GetInt(k string) (i int, ok bool) {
	v, ok := m.Get(k)
	if !ok {
		return
	}

	i, ok = v.(int)
	if ok {
		return
	}

	// If the MapValue is created from a JSON, v may be a json.Number.
	number, ok := v.(json.Number)
	if !ok {
		return
	}

	i64, err := number.Int64()
	if err != nil {
		return 0, false
	}
	return int(i64), true
}

// GetInt64 returns the int64 value i64 associated with the specified key k.
// If the value does not exist, or is not an int64 value, this method returns 0
// and sets ok to false.
func (m *MapValue) GetInt64(k string) (i64 int64, ok bool) {
	v, ok := m.Get(k)
	if !ok {
		return
	}

	i64, ok = v.(int64)
	if ok {
		return
	}

	// If the MapValue is created from a JSON, v may be a json.Number.
	number, ok := v.(json.Number)
	if !ok {
		return
	}

	i64, err := number.Int64()
	if err != nil {
		return 0, false
	}
	return i64, true
}

// GetFloat64 returns the float64 value f64 associated with the specified key k.
// If the value does not exist, or is not a float64 value, this method returns 0
// and sets ok to false.
func (m *MapValue) GetFloat64(k string) (f64 float64, ok bool) {
	v, ok := m.Get(k)
	if !ok {
		return
	}

	f64, ok = v.(float64)
	if ok {
		return
	}

	// If the MapValue is created from a JSON, v may be a json.Number.
	number, ok := v.(json.Number)
	if !ok {
		return
	}

	f64, err := number.Float64()
	if err != nil {
		return 0, false
	}
	return f64, true
}

// ToMapValue is a convenience function that converts a key/value pair into a MapValue.
func ToMapValue(k string, v interface{}) *MapValue {
	m := map[string]interface{}{
		k: v,
	}
	return NewMapValue(m)
}
