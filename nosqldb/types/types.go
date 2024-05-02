//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

//go:generate stringer -type=Consistency,TimeUnit,TableState,OperationState,DbType,PutOption -output types_string.go

package types

import (
	"fmt"
	"time"
)

// Consistency is used to provide consistency guarantees for read operations.
//
// There are two consistency values available: Eventual and Absolute.
//
// 1. Eventual consistency means that the values read may be very slightly out of date.
//
// 2. Absolute consistency may be specified to guarantee that current values are read.
//
// Absolute consistency results in higher cost, consuming twice the number of
// read units for the same data relative to Eventual consistency, and should
// only be used when required.
//
// It is possible to set a default Consistency for a nosqldb.Client instance by
// using nosqldb.Config.Consistency. If no Consistency is specified in an operation
// and there is no default value, Eventual consistency is used.
//
// Consistency can be specified as an optional argument to all read operations.
type Consistency int

const (
	// Absolute consistency.
	Absolute Consistency = iota + 1 // 1

	// Eventual consistency.
	Eventual // 2
)

// SyncPolicy represents policies to be used when committing a
// transaction. High levels of synchronization offer a greater guarantee
// that the transaction is persistent to disk, but trade that off for
// lower performance.
type SyncPolicy int

const (
	// SyncPolicySync writes and synchronously flushes the log on transaction commit.
	// Transactions exhibit all the ACID (atomicity, consistency,
	// isolation, and durability) properties.
	SyncPolicySync = iota + 1 // 1

	// SyncPolicyNoSync does not write or synchronously flush the log on transaction commit.
	// Transactions exhibit the ACI (atomicity, consistency, and isolation)
	// properties, but not D (durability); that is, database integrity will
	// be maintained, but if the application or system fails, it is
	// possible some number of the most recently committed transactions may
	// be undone during recovery. The number of transactions at risk is
	// governed by how many log updates can fit into the log buffer, how
	// often the operating system flushes dirty buffers to disk, and how
	// often log checkpoints occur.
	SyncPolicyNoSync // 2

	// SyncPolicyWriteNoSync writes but does not synchronously flush the log on transaction commit.
	// Transactions exhibit the ACI (atomicity, consistency, and isolation)
	// properties, but not D (durability); that is, database integrity will
	// be maintained, but if the operating system fails, it is possible
	// some number of the most recently committed transactions may be
	// undone during recovery. The number of transactions at risk is
	// governed by how often the operating system flushes dirty buffers to
	// disk, and how often log checkpoints occur.
	SyncPolicyWriteNoSync // 3
)

// ReplicaAckPolicy defines the policy for how replicated commits are handled.
// A replicated environment makes it possible to increase an application's
// transaction commit guarantees by committing changes to its replicas on
// the network.
type ReplicaAckPolicy int

const (
	// ReplicaAckPolicyAll defines that all replicas must acknowledge that they
	// have committed the transaction. This policy should be selected only if
	// your replication group has a small number of replicas, and those replicas are on
	// extremely reliable networks and servers.
	ReplicaAckPolicyAll = iota + 1 // 1

	// ReplicaAckPolicyNone defines that no transaction commit acknowledgments
	// are required and the master will never wait for replica acknowledgments.
	// In this case, transaction durability is determined entirely by the type of commit
	// that is being performed on the master.
	ReplicaAckPolicyNone // 2

	// ReplicaAckPolicySimpleMajority defines that a simple majority of replicas
	// must acknowledge that they have committed the transaction. This
	// acknowledgment policy, in conjunction with an election policy which
	// requires at least a simple majority, ensures that the changes made by
	// the transaction remains durable if a new election is held.
	ReplicaAckPolicySimpleMajority // 3
)

// Durability defines the durability characteristics associated with a standalone write
// (put or update) operation.
//
// # Added in SDK Version 1.3.0
//
// This is currently only supported in On-Prem installations. It is ignored
// in the cloud service.
//
// The overall durability is a function of the SyncPolicy and
// ReplicaAckPolicy in effect for the Master, and the SyncPolicy in
// effect for each Replica.
type Durability struct {
	// The sync policy in effect on the Master node.
	MasterSync SyncPolicy `json:"masterSync"`

	// The sync policy in effect on a replica.
	ReplicaSync SyncPolicy `json:"replicaSync"`

	// The replica acknowledgment policy to be used.
	ReplicaAck ReplicaAckPolicy `json:"replicaAck"`
}

// IsSet returns true if any durability values are nonzero
func (d *Durability) IsSet() bool {
	return d.MasterSync != 0 || d.ReplicaSync != 0 || d.ReplicaAck != 0
}

// DefinedTags encapsulates defined tags which are returned
// from calls to Client.GetTable(). They can also be set during
// table creation operations as well as alter table operations.
// Cloud service only.
// Added in SDK Version 1.4.0
type DefinedTags struct {
	Tags *MapValue
}

// AddTag adds a key/value string pair to the DefinedTags in the given
// namespace.
func (dt *DefinedTags) AddTag(namespace, key, value string) (err error) {
	if dt.Tags == nil {
		dt.Tags = NewEmptyMapValue()
	}
	if !dt.Tags.Contains(namespace) {
		dt.Tags.Put(namespace, NewEmptyMapValue())
	}
	nsMap, ok := dt.Tags.GetMapValue(namespace)
	if !ok {
		return fmt.Errorf("invalid internal value for namespace %s", namespace)
	}
	nsMap.Put(key, value)
	return nil
}

// SetValuesFromJSON sets the DefinedTags values from a JSON string.
// Any previous values will be lost.
func (dt *DefinedTags) SetValuesFromJSON(jsonStr string) (err error) {
	dt.Tags, err = NewMapValueFromJSON(jsonStr)
	return err
}

// GetTag returns the DefinedTag for the given namespace and key, if present.
// If not present, empty string is returned.
func (dt *DefinedTags) GetTag(namespace, key string) string {
	if dt.Tags == nil {
		return ""
	}
	nsMap, ok := dt.Tags.GetMapValue(namespace)
	if !ok {
		return ""
	}
	str, _ := nsMap.GetString(key)
	return str
}

// IsEmpty returns true of there are no key/value pairs in any namespace in the
// DefinedTags. Otherwise it returns false.
func (dt *DefinedTags) IsEmpty() bool {
	if dt.Tags == nil || dt.Tags.Len() == 0 {
		return true
	}
	// walk each namespace looking for any key/value pair
	tagMap := dt.Tags.Map()
	for k := range tagMap {
		if mv, ok := dt.Tags.GetMapValue(k); ok {
			if mv.Len() > 0 {
				return false
			}
		}
	}
	return true
}

// FreeFormTags encapsulates free-form tags which are returned
// from calls to Client.GetTable(). They can also be set during
// table creation operations as well as alter table operations.
// Cloud service only.
// Added in SDK Version 1.4.0
type FreeFormTags struct {
	Tags *MapValue
}

// SetValuesFromMap sets the FreeFormTags values from a string/interface map.
// Any previous values will be lost.
func (fft *FreeFormTags) SetValuesFromMap(m map[string]interface{}) {
	fft.Tags = NewMapValue(m)
}

// SetValuesFromJSON sets the FreeFormTags values from a JSON string.
// Any previous values will be lost.
func (fft *FreeFormTags) SetValuesFromJSON(jsonStr string) (err error) {
	fft.Tags, err = NewMapValueFromJSON(jsonStr)
	return err
}

// AddTag adds a key/value string pair to the FreeFormTags.
func (fft *FreeFormTags) AddTag(key, value string) (err error) {
	if fft.Tags == nil {
		fft.Tags = NewEmptyMapValue()
	}
	fft.Tags.Put(key, value)
	return nil
}

// GetTag returns the tag for the given key, if present.
// If not present, empty string is returned.
func (fft *FreeFormTags) GetTag(key string) string {
	if fft.Tags == nil {
		return ""
	}
	str, _ := fft.Tags.GetString(key)
	return str
}

// Contains returns true if the FreeFormTags contain a key/value pair with
// the specified key, false otherwise.
func (fft *FreeFormTags) Contains(k string) (ok bool) {
	if fft.Tags == nil {
		return false
	}
	return fft.Tags.Contains(k)
}

// Size returns the number of key/value pairs in the FreeFormTags.
func (fft *FreeFormTags) Size() int {
	if fft.Tags == nil {
		return 0
	}
	return fft.Tags.Len()
}

// GetMap returns the underlying string/interface map in the FreeFormTags.
func (fft *FreeFormTags) GetMap() (m *map[string]interface{}) {
	if fft.Tags == nil {
		return nil
	}
	v := fft.Tags.Map()
	return &v
}

// CapacityMode defines the type of limits for a table.
type CapacityMode int

const (
	// Provisioned table: fixed maximum of read/write units.
	// Note this value is purposefully zero as it is the default and
	// this enables existing app code to work properly
	Provisioned CapacityMode = iota // 0

	// OnDemand table: table scales with usage
	// Added in SDK Version 1.3.0
	OnDemand // 1
)

// TableState represents current state of a table.
//
// The available table states are:
//
//	Active
//	Creating
//	Dropped
//	Dropping
//	Updating
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
//	PutIfAbsent
//	PutIfPresent
//	PutIfVersion
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

// NOTE: the date values in these are not random! They reference specific
// fields based on their numerical strings. For example, "2006" is the
// definition for stdLongYear. These do not follow C-style formatting.
// See the consts in https://golang.org/src/time/format.go for more detail.

// ISO8601Layout represents the ISO 8601 format of Go's reference time.
const ISO8601Layout = "2006-01-02T15:04:05.999999999"

// ISO8601ZLayout includes literal "Z"
const ISO8601ZLayout = "2006-01-02T15:04:05.999999999Z"

// ISO8601NoTLayout is the same as above woth a space instead of a 'T'
const ISO8601NoTLayout = "2006-01-02 15:04:05.999999999"

// ISO8601ZNoTLayout is the same as above woth a space instead of a 'T'
const ISO8601ZNoTLayout = "2006-01-02 15:04:05.999999999Z"

func ParseDateTime(datestr string) (time.Time, error) {
	if v, err := time.Parse(ISO8601Layout, datestr); err == nil {
		return v, nil
	}
	if v, err := time.Parse(ISO8601ZLayout, datestr); err == nil {
		return v, nil
	}
	if v, err := time.Parse(ISO8601NoTLayout, datestr); err == nil {
		return v, nil
	}
	return time.Parse(ISO8601ZNoTLayout, datestr)
}

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
//	<a, b, c>
//
// A FieldRange can be specified for:
//
//	"a" if the primary key supplied is empty.
//	"b" if the primary key supplied to the operation has a concrete value for "a" but not for "b" or "c".
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
