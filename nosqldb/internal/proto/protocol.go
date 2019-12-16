//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
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
	QueryVersion int16 = 2
)

const (
	BatchRequestSizeLimit = 25 * 1024 * 1024
	RequestSizeLimit      = 2 * 1024 * 1024
	BatchOpNumberLimit    = 50
	// The limit on the max write KB during a operation
	WriteKBLimit        = 2 * 1024
	BatchQuerySizeLimit = 100
)

type OpCode int

const (
	Delete              OpCode = iota // 0
	DeleteIfVersion                   // 1
	Get                               // 2
	Put                               // 3
	PutIfAbsent                       // 4
	PutIfPresent                      // 5
	PutIfVersion                      // 6
	Query                             // 7
	Prepare                           // 8
	WriteMultiple                     // 9
	MultiDelete                       // 10
	GetTable                          // 11
	GetIndexes                        // 12
	GetTableUsage                     // 13
	ListTables                        // 14
	TableRequest                      // 15
	Scan                              // 16
	IndexScan                         // 17
	CreateTable                       // 18
	AlterTable                        // 19
	DropTable                         // 20
	CreateIndex                       // 21
	DropIndex                         // 22
	SystemRequest                     // 23
	SystemStatusRequest               // 24
)

type Reader interface {
	ReadInt16() (int16, error)
	ReadInt() (int, error)
	ReadPackedInt() (int, error)
	ReadPackedLong() (int64, error)
	ReadDouble() (float64, error)
	ReadString() (*string, error)
	ReadBoolean() (bool, error)
	ReadByte() (byte, error)
	ReadVersion() (types.Version, error)
	ReadFieldValue() (types.FieldValue, error)
	ReadByteArray() ([]byte, error)
	ReadByteArrayWithInt() ([]byte, error)
}

type Writer interface {
	Write(p []byte) (int, error)
	WriteByte(b byte) error
	WriteInt16(value int16) (int, error)
	WriteInt(value int) (int, error)
	WritePackedInt(value int) (int, error)
	WritePackedLong(value int64) (int, error)
	WriteDouble(value float64) (int, error)
	WriteString(value *string) (int, error)
	WriteBoolean(value bool) (int, error)
	WriteMap(value *types.MapValue) (int, error)
	WriteArray(value []types.FieldValue) (int, error)
	WriteByteArray(value []byte) (int, error)
	WriteByteArrayWithInt(value []byte) (int, error)
	WriteFieldValue(value types.FieldValue) (int, error)
	WriteFieldRange(fieldRange *types.FieldRange) (int, error)
	WriteOpCode(op OpCode) (int, error)
	WriteTimeout(timeout time.Duration) (int, error)
	WriteConsistency(c types.Consistency) (int, error)
	WriteTTL(ttl *types.TimeToLive) (int, error)
	WriteVersion(version types.Version) (int, error)
	WriteSerialVersion(serialVersion int16) (int, error)
	Size() int
	Reset()
	Bytes() []byte
}
