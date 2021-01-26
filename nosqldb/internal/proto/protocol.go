//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package proto

import (
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

const (
	// SerialVersion represents the protocol version used to serialize requests
	// and deserialize results between client and server.
	SerialVersion int16 = 2

	// QueryVersion represents the version used for query requests.
	QueryVersion int16 = 3
)

const (
	// BatchRequestSizeLimit represents the limit on content length of a batch request.
	// It is 25 MB.
	BatchRequestSizeLimit = 25 * 1024 * 1024

	// RequestSizeLimit represents the limit on content length of a single request.
	// It is 2 MB.
	RequestSizeLimit = 2 * 1024 * 1024

	// DefaultBatchQueryNumberLimit represents the default limit on number of
	// results returned for a batch query request.
	// It is 100.
	DefaultBatchQueryNumberLimit = 100
)

// OpCode represents the operation code.
type OpCode int

const (
	// Delete is used for the operation that deletes a row from table.
	Delete OpCode = iota // 0

	// DeleteIfVersion is used for the operation that deletes a row from table
	// if the row matches the specified version.
	DeleteIfVersion // 1

	// Get is used for the operation that retrieves a row from table.
	Get // 2

	// Put is used for the operation that unconditionally puts a row to table.
	Put // 3

	// PutIfAbsent is used for the operation that puts a row to table if the row
	// is absent.
	PutIfAbsent // 4

	// PutIfPresent is used for the operation that puts a row to table if the row
	// is present.
	PutIfPresent // 5

	// PutIfVersion is used for the operation that puts a row to table if the row
	// matches the specified version.
	PutIfVersion // 6

	// Query is used for the query operation.
	// A query operation can perform select, insert, update and delete operations
	// over an SQL statement.
	Query // 7

	// Prepare is used for the operation that compiles/prepares an SQL statement
	// before execution.
	Prepare // 8

	// WriteMultiple is used to perform multiple write operations associated
	// with a table in a single transaction.
	WriteMultiple // 9

	// MultiDelete is used for the operation that deletes multiple rows from a
	// table in a single transaction.
	MultiDelete // 10

	// GetTable is used for the operation that retrieves static information about a table.
	GetTable // 11

	// GetIndexes is used for the operation that retrieves information about an index.
	GetIndexes // 12

	// GetTableUsage is used for the operation that retrieves usage information on a table.
	GetTableUsage // 13

	// ListTables is used for the operation that lists all available table names.
	ListTables // 14

	// TableRequest is used for the operation that manages table schema or
	// changes table limits.
	TableRequest // 15

	// Scan is reserved for internal use.
	Scan // 16

	// IndexScan is reserved for internal use.
	IndexScan // 17

	// CreateTable represents the operation that creates a table.
	CreateTable // 18

	// AlterTable represents the operation that modifies the table schema.
	AlterTable // 19

	// DropTable represents the operation that drops a table.
	DropTable // 20

	// CreateIndex represents the operation that creates an index on a table.
	CreateIndex // 21

	// DropIndex represents the operation that drops an index on a table.
	DropIndex // 22

	// SystemRequest is used to perform system operations such as
	// administrative operations that do not affect a specific table.
	SystemRequest // 23

	// SystemStatusRequest is used to retrieve the operation status of a SystemRequest.
	SystemStatusRequest // 24
)

// Reader is a protocol reader used to read and decode byte sequences.
type Reader interface {
	// ReadInt16 reads an int16 value.
	ReadInt16() (int16, error)

	// ReadInt reads an int32 value.
	ReadInt() (int, error)

	// ReadPackedInt reads a packed int32 value.
	ReadPackedInt() (int, error)

	// ReadPackedLong reads a packed int64 value.
	ReadPackedLong() (int64, error)

	// ReadDouble reads a double value.
	ReadDouble() (float64, error)

	// ReadString reads a string value.
	ReadString() (*string, error)

	// ReadBoolean reads a boolean value.
	ReadBoolean() (bool, error)

	// ReadByte reads a single byte.
	ReadByte() (byte, error)

	// ReadVersion reads a Version value.
	ReadVersion() (types.Version, error)

	// ReadFieldValue reads a field value.
	ReadFieldValue() (types.FieldValue, error)

	// ReadByteArray reads an array of bytes.
	// The returned bytes may be nil.
	ReadByteArray() ([]byte, error)

	// ReadByteArrayWithInt reads an array of bytes.
	// The returned bytes is always non-nil.
	ReadByteArrayWithInt() ([]byte, error)
}

// Writer is a protocol writer used to encode data to byte sequences and write to the output.
type Writer interface {
	// Write writes the bytes.
	Write(p []byte) (int, error)

	// WriteByte writes a single byte.
	WriteByte(b byte) error

	// WriteInt16 writes an int16 value.
	WriteInt16(value int16) (int, error)

	// WriteInt writes an int32 value.
	WriteInt(value int) (int, error)

	// WritePackedInt writes a packed int32 value.
	WritePackedInt(value int) (int, error)

	// WritePackedLong writes a packed int64 value.
	WritePackedLong(value int64) (int, error)

	// WriteDouble writes a float64 value.
	WriteDouble(value float64) (int, error)

	// WriteString writes a string value.
	WriteString(value *string) (int, error)

	// WriteBoolean writes a boolean value.
	WriteBoolean(value bool) (int, error)

	// WriteMap writes a map value.
	WriteMap(value *types.MapValue) (int, error)

	// WriteArray writes an array of field values.
	WriteArray(value []types.FieldValue) (int, error)

	// WriteByteArray writes an array of bytes that may be nil.
	WriteByteArray(value []byte) (int, error)

	// WriteByteArrayWithInt writes an array of bytes that are non-nil.
	WriteByteArrayWithInt(value []byte) (int, error)

	// WriteFieldValue writes a field value.
	WriteFieldValue(value types.FieldValue) (int, error)

	// WriteFieldRange writes a field range value.
	WriteFieldRange(fieldRange *types.FieldRange) (int, error)

	// WriteOpCode writes an opcode.
	WriteOpCode(op OpCode) (int, error)

	// WriteTimeout writes a timeout value.
	WriteTimeout(timeout time.Duration) (int, error)

	// WriteConsistency writes a Consistency value.
	WriteConsistency(c types.Consistency) (int, error)

	// WriteTTL writes a TimeToLive value.
	WriteTTL(ttl *types.TimeToLive) (int, error)

	// WriteVersion writes a Version value.
	WriteVersion(version types.Version) (int, error)

	// WriteSerialVersion writes a serial version.
	WriteSerialVersion(serialVersion int16) (int, error)

	// Size reports the number of bytes written by the writer.
	Size() int

	// Reset resets the writer.
	Reset()

	// Bytes returns the bytes written in the buffer of the writer.
	Bytes() []byte
}
