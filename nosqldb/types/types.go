//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

//go:generate stringer -type=Consistency,TimeUnit,TableState,OperationState,DbType,PutOption -output types_string.go

package types

import (
	"time"
)

// Consistency is used to provide consistency guarantees for read operations.
//
// There are two consistency values available: Eventual and Absolute.
//
// 1. Eventual consistency means that the values read may be very slightly out of date.
//
// 2. Absolute consistency may be specified to guarantee that current values are read.
// Absolute consistency results in higher cost, consuming twice the number of
// read units for the same data relative to Eventual consistency, and should
// only be used when required.
//
// It is possible to set a default Consistency for a nosqldb.Client instance by
// using nosqldb.Config.Consistency. If no Consistency is specified in an operation
// and there is no default value, Eventual consistency is used.
//
// Consistency can be specified as an optional argument to all read operations.
//
type Consistency int

const (
	// Absolute consistency.
	Absolute Consistency = iota + 1 // 1

	// Eventual consistency.
	Eventual // 2
)

// TableState represents current state of a table.
//
// The available table states are:
//
//   Active
//   Creating
//   Dropped
//   Dropping
//   Updating
//
type TableState int

const (
	// Active represents the table is ready to be used.
	// This is the steady state after table creation or modification.
	Active TableState = iota // 0

	// Creating represents the table is being created and cannot yet be used.
	Creating // 1

	// Dropped represents the table has been dropped or does not exist.
	Dropped // 2

	// Dropping represents the table is being dropped and cannot be used.
	Dropping // 3

	// Updating represents the table is being updated.
	// It is available for normal use, but additional table modification
	// operations are not permitted while the table is in this state.
	Updating // 4
)

// IsTerminal checks if current table state is a terminal state.
// This returns true if current state is either Active or Dropped, returns false otherwise.
func (st TableState) IsTerminal() bool {
	return st == Active || st == Dropped
}

// GoString defines the Go syntax for the TableState value.
//
// This implements the fmt.GoStringer interface.
func (st TableState) GoString() string {
	return "\"" + st.String() + "\""
}

// OperationState represents the current state of the operation.
//
// This is used for on-premise only.
type OperationState int

const (
	// UnknownOpState represents the operation state is unknown.
	UnknownOpState OperationState = iota // 0

	// Complete represents the operation is complete and was successful.
	Complete // 1

	// Working represents the operation is in progress.
	Working // 2
)

// GoString defines the Go syntax for the OperationState value.
//
// This implements the fmt.GoStringer interface.
func (st OperationState) GoString() string {
	return "\"" + st.String() + "\""
}

// Version represents the version of a row in the database.
// This is an opaque object from an application perspective.
//
// It is returned by successful Client.Put() or Client.Get() operations and
// can be used in PutRequest.MatchVersion and DeleteRequest.MatchVersion to
// conditionally perform those operations to ensure an atomic read-modify-write cycle.
// Use of Version in this way adds cost to operations so it should be done only
// if necessary.
type Version []byte

// PutOption represents an option for the put operation. It is used by PutRequest.
// The available put options are:
//
//   PutIfAbsent
//   PutIfPresent
//   PutIfVersion
//
type PutOption int

const (
	// PutIfAbsent means put operation should only succeed if the row does not exist.
	PutIfAbsent PutOption = 4 // 4

	// PutIfPresent means put operation should only succeed if the row exists.
	PutIfPresent PutOption = 5 // 5

	// PutIfVersion means put operation should succeed only if the row exists
	// and its Version matches the specified version.
	PutIfVersion PutOption = 6 // 6
)

// GoString defines the Go syntax for the PutOption value.
//
// This implements the fmt.GoStringer interface.
func (opt PutOption) GoString() string {
	return "\"" + opt.String() + "\""
}

// TimeUnit represents time durations at a given unit.
type TimeUnit int

const (
	// Hours represents time durations in hours.
	Hours TimeUnit = iota + 1 // 1

	// Days represents time durations in days.
	Days // 2
)

// GoString defines the Go syntax for the TimeUnit value.
//
// This implements the fmt.GoStringer interface.
func (tu TimeUnit) GoString() string {
	return "\"" + tu.String() + "\""
}

// TimeToLive represents a period of time, specialized to the needs of this driver.
//
// This is restricted to durations of days and hours. It is only used as input
// related to time to live (TTL) for row instances.
//
// Construction of TimeToLive values allows only day and hour durations for
// efficiency reasons. Durations of days are recommended as they result in the
// least amount of storage overhead.
//
// Only positive durations are allowed on input, although negative durations
// can be returned if the expiration time of a row is in the past relative to
// the reference time.
type TimeToLive struct {
	// Value represents number of time units.
	Value int64

	// Unit represents the time unit that is either Hours or Days.
	Unit TimeUnit
}

// ToDuration converts the TimeToLive value into a time.Duration value.
func (ttl TimeToLive) ToDuration() time.Duration {
	var numOfHours int64
	switch ttl.Unit {
	case Hours:
		numOfHours = ttl.Value
	case Days:
		fallthrough
	default:
		numOfHours = ttl.Value * 24
	}

	return time.Duration(numOfHours) * time.Hour
}

// ISO8601Layout represents the ISO 8601 format of Go's reference time.
const ISO8601Layout = "2006-01-02T15:04:05.999999999"

// FieldRange defines a range of values to be used in a Client.MultiDelete()
// operation, as specified in MultiDeleteRequest.FieldRange.
//
// FieldRange is used as the least significant component in a partially
// specified key value in order to create a value range for an operation that
// returns multiple rows or keys. The data types supported by FieldRange are
// limited to the atomic types which are valid for primary keys.
//
// The least significant component of a key is the first component of the key
// that is not fully specified. For example, if the primary key for a table is
// defined as the tuple:
//
//   <a, b, c>
//
// A FieldRange can be specified for:
//
//   "a" if the primary key supplied is empty.
//   "b" if the primary key supplied to the operation has a concrete value for "a" but not for "b" or "c".
//
// This object is used to scope a Client.MultiDelete() operation.
// The FieldPath specified must name a field in a table's primary key.
// The Start and End values used must be of the same type and that type must
// match the type of the field specified.
//
// Validation of this object is performed when it is used in an operation.
// Validation includes verifying that the field is in the required key and,
// in the case of a composite key, that the field is in the proper order
// relative to the key used in the operation.
type FieldRange struct {
	// FieldPath specifies the path to the field used in the range.
	FieldPath string

	// Start specifies the start value of the range.
	Start interface{}

	// End specifies the end value of the range.
	End interface{}

	// StartInclusive specifies whether Start value is included in the range,
	// i.e., Start value is less than or equal to the first FieldValue in the range.
	//
	// This value is valid only if the Start value is specified.
	StartInclusive bool

	// EndInclusive specifies whether End value is included in the range,
	// i.e., End value is greater than or equal to the last FieldValue in the range.
	//
	// This value is valid only if the End value is specified.
	EndInclusive bool
}

// DbType represents the Oracle NoSQL database types used for field values.
type DbType int

const (
	// Array represents the Array data type.
	// An array is an ordered collection of zero or more elements,
	// all elements of an array have the same type.
	Array DbType = iota // 0

	// Binary represents the Binary data type.
	// A binary is an uninterpreted sequence of zero or more bytes.
	Binary // 1

	// Boolean data type has only two possible values: true and false.
	Boolean // 2

	// Double data type represents the set of all IEEE-754 64-bit floating-point numbers.
	Double // 3

	// Integer data type represents the set of all signed 32-bit integers (-2147483648 to 2147483647).
	Integer // 4

	// Long data type represents the set of all signed 64-bit
	// integers (-9223372036854775808 to 9223372036854775807).
	Long // 5

	// Map represents the Map data type.
	// A map is an unordered collection of zero or more key-value pairs,
	// where all keys are strings and all the values have the same type.
	Map // 6

	// String represents the set of string values.
	String // 7

	// Timestamp represents a point in time as a date or a time.
	Timestamp // 8

	// Number represents arbitrary precision numbers.
	Number // 9

	// JSONNull represents a special value that indicates the absence of
	// an actual value within a JSON data type.
	JSONNull // 10

	// Null represents a special value that indicates the absence of
	// an actual value, or the fact that a value is unknown or inapplicable.
	Null // 11

	// Empty represents the Empty data type.
	// It is used to describe the result of a query expression is empty.
	Empty // 12
)
