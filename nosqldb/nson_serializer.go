//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// NSON versions of all serializers

const (
	ABORT_ON_FAIL              = "a"
	BIND_VARIABLES             = "bv"
	COMPARTMENT_OCID           = "cc"
	CONSISTENCY                = "co"
	CONSUMED                   = "c"
	CONTINUATION_KEY           = "ck"
	DATA                       = "d"
	DEFINED_TAGS               = "dt"
	DRIVER_QUERY_PLAN          = "dq"
	DURABILITY                 = "du"
	END                        = "en"
	ERROR_CODE                 = "e"
	ETAG                       = "et"
	EXACT_MATCH                = "ec"
	EXCEPTION                  = "x"
	EXISTING_MOD_TIME          = "em"
	EXISTING_VALUE             = "el"
	EXISTING_VERSION           = "ev"
	EXPIRATION                 = "xp"
	FIELDS                     = "f"
	FREE_FORM_TAGS             = "ff"
	GENERATED                  = "gn"
	GET_QUERY_PLAN             = "gq"
	GET_QUERY_SCHEMA           = "gs"
	HEADER                     = "h"
	IDEMPOTENT                 = "ip"
	IDENTITY_CACHE_SIZE        = "ic"
	INCLUSIVE                  = "in"
	INDEX                      = "i"
	INDEXES                    = "ix"
	INITIALIZED                = "it"
	IS_JSON                    = "j"
	IS_PREPARED                = "is"
	IS_SIMPLE_QUERY            = "iq"
	KEY                        = "k"
	KV_VERSION                 = "kv"
	LAST_INDEX                 = "li"
	LIMITS                     = "lm"
	LIMITS_MODE                = "mo"
	LIST_MAX_TO_READ           = "lx"
	LIST_START_INDEX           = "ls"
	MATCH_VERSION              = "mv"
	MATH_CONTEXT_CODE          = "mc"
	MATH_CONTEXT_PRECISION     = "cp"
	MATH_CONTEXT_ROUNDING_MODE = "rm"
	MAX_READ_KB                = "mr"
	MAX_WRITE_KB               = "mw"
	MAX_SHARD_USAGE_PERCENT    = "ms"
	MODIFIED                   = "md"
	NAME                       = "m"
	NAMESPACE                  = "ns"
	NEXT_START_TIME            = "ni"
	NOT_TARGET_TABLES          = "nt"
	NUMBER_LIMIT               = "nl"
	NUM_DELETIONS              = "nd"
	NUM_OPERATIONS             = "no"
	NUM_RESULTS                = "nr"
	OP_CODE                    = "o"
	OPERATIONS                 = "os"
	OPERATION_ID               = "od"
	PATH                       = "pt"
	PAYLOAD                    = "p"
	PREPARE                    = "pp"
	PREPARED_QUERY             = "pq"
	PREPARED_STATEMENT         = "ps"
	PROXY_TOPO_SEQNUM          = "pn"
	QUERY                      = "q"
	QUERY_NAME                 = "qn"
	QUERY_OPERATION            = "qo"
	QUERY_PLAN_STRING          = "qs"
	QUERY_RESULTS              = "qr"
	QUERY_RESULT_SCHEMA        = "qc"
	QUERY_VERSION              = "qv"
	RANGE                      = "rg"
	RANGE_PATH                 = "rp"
	REACHED_LIMIT              = "re"
	READ_KB                    = "rk"
	READ_THROTTLE_COUNT        = "rt"
	READ_UNITS                 = "ru"
	REGION                     = "rn"
	REPLICAS                   = "rc"
	REPLICA_LAG                = "rl"
	REPLICA_STATS              = "ra"
	RETRY_HINT                 = "rh"
	RETURN_INFO                = "ri"
	RETURN_ROW                 = "rr"
	ROW                        = "r"
	ROW_VERSION                = "rv"
	SCHEMA_FROZEN              = "sf"
	SERVER_MEMORY_CONSUMPTION  = "sm"
	SHARD_ID                   = "si"
	SHARD_IDS                  = "sa"
	SORT_PHASE1_RESULTS        = "p1"
	START                      = "sr"
	STATEMENT                  = "st"
	STORAGE_GB                 = "sg"
	STORAGE_THROTTLE_COUNT     = "sl"
	SUCCESS                    = "ss"
	SYSOP_RESULT               = "rs"
	SYSOP_STATE                = "ta"
	TABLES                     = "tb"
	TABLE_ACCESS_INFO          = "ai"
	TABLE_DDL                  = "td"
	TABLE_NAME                 = "n"
	TABLE_OCID                 = "to"
	TABLE_SCHEMA               = "ac"
	TABLE_STATE                = "as"
	TABLE_USAGE                = "u"
	TABLE_USAGE_PERIOD         = "pd"
	TIME                       = "tm"
	TIMEOUT                    = "t"
	TOPOLOGY_INFO              = "tp"
	TOPO_SEQ_NUM               = "ts"
	TRACE_LEVEL                = "tl"
	TRACE_AT_LOG_FILES         = "tf"
	TTL                        = "tt"
	TYPE                       = "y"
	UPDATE_TTL                 = "ut"
	VALUE                      = "l"
	VERSION                    = "v"
	VSCAN                      = "vs"
	VSCANS                     = "vssa"
	VSCAN_JOIN_DESC_RESUME_KEY = "vsjdrk"
	VSCAN_JOIN_PATH_TABLES     = "vsjpt"
	VSCAN_JOIN_PATH_KEY        = "vsjpk"
	VSCAN_JOIN_PATH_SEC_KEY    = "vsjpsk"
	VSCAN_JOIN_PATH_MATCHED    = "vsjpm"
	VSCAN_MOVE_AFTER           = "vsma"
	VSCAN_PID                  = "vspid"
	VSCAN_PRIM_KEY             = "vspk"
	VSCAN_SEC_KEY              = "vssk"
	VSCAN_SID                  = "vssid"
	WM_FAILURE                 = "wf"
	WM_FAIL_INDEX              = "wi"
	WM_FAIL_RESULT             = "wr"
	WM_SUCCESS                 = "ws"
	WRITE_KB                   = "wk"
	WRITE_MULTIPLE             = "wm"
	WRITE_THROTTLE_COUNT       = "wt"
	WRITE_UNITS                = "wu"
)

// Constants to make code cleaner
const (
	BadProtocol         = int(nosqlerr.BadProtocolMessage)
	UnsupportedProtocol = int(nosqlerr.UnsupportedProtocol)
)

var nsondebug bool = false

// SetNsonDebug sets the debug level. It is typically only used during testing.
func SetNsonDebug(d bool) {
	nsondebug = d
}

// serialize writes the GetRequest to data stream using the specified protocol writer.
//
// The fields of GetRequest are written in the following order:
//
//	OpCode: Get
//	Timeout
//	TableName
//	Consistency
//	Key
func (req *GetRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.Get, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	// payload
	ns.startPayload()
	if err = ns.writeConsistency(req.Consistency); err != nil {
		return
	}
	if err = ns.writeField(KEY, req.Key); err != nil {
		return
	}
	ns.endPayload()

	endRequest(ns)
	return nil
}

func (req *GetRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &GetResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err = walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case CONSUMED:
			res.Capacity, err = readNsonConsumedCapacity(r)
		case ROW:
			err = readNsonRow(r, res)
		case TOPOLOGY_INFO:
			err = res.SetTopologyOrErr(readNsonTopologyInfo(r))
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the GetTableRequest to data stream using the specified protocol writer.
//
// The fields of GetTableRequest are written in the following order:
//
//	OpCode: GetTable
//	Timeout
//	TableName
//	OperationID
func (req *GetTableRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.GetTable, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if req.OperationID != "" {
		if err = ns.writeField(OPERATION_ID, req.OperationID); err != nil {
			return
		}
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *GetTableRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	return deserializeTableResult(r, serialVersion)
}

func (req *SystemRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.SystemRequest, req.Timeout, "", req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if err = ns.writeField(STATEMENT, []byte(req.Statement)); err != nil {
		return
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *SystemRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	return deserializeSystemResult(r)
}

func (req *SystemStatusRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	ns.startHeader()
	if err = ns.writeHeader(proto.SystemStatusRequest, req.Timeout, "", req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if err = ns.writeField(STATEMENT, req.Statement); err != nil {
		return
	}
	if err = ns.writeField(OPERATION_ID, req.OperationID); err != nil {
		return
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *SystemStatusRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	return deserializeSystemResult(r)
}

// serialize writes the TableRequest to data stream using the specified protocol writer.
//
// The fields of TableRequest are written in the following order:
//
//	OpCode: TableRequest
//	Timeout
//	Statement: if it is set.
//	A bool flag: indicates if table limits is set.
//	TableLimits: skip if it is not set.
//	A bool flag: indicates if table name is set.
//	TableName: skip if it is not set.
func (req *TableRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.TableRequest, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if req.Statement != "" {
		if err = ns.writeField(STATEMENT, req.Statement); err != nil {
			return
		}
	}
	if req.TableLimits != nil {
		ns.startMap(LIMITS)
		limits := req.TableLimits
		if err = ns.writeField(READ_UNITS, int(limits.ReadUnits)); err != nil {
			return
		}
		if err = ns.writeField(WRITE_UNITS, int(limits.WriteUnits)); err != nil {
			return
		}
		if err = ns.writeField(STORAGE_GB, int(limits.StorageGB)); err != nil {
			return
		}
		mode := 1 // Provisioned in protocol
		if limits.CapacityMode == types.OnDemand {
			mode = 2 // OnDemand in protocol
		}
		if err = ns.writeField(LIMITS_MODE, mode); err != nil {
			return
		}
		ns.endMap(LIMITS)
	}
	if req.DefinedTags != nil && !req.DefinedTags.IsEmpty() {
		var a []byte
		if a, err = req.DefinedTags.Tags.MarshalJSON(); err != nil {
			return
		}
		if err = ns.writeField(DEFINED_TAGS, string(a)); err != nil {
			return
		}
	}
	if req.FreeFormTags != nil && req.FreeFormTags.Size() > 0 {
		var a []byte
		if a, err = req.FreeFormTags.Tags.MarshalJSON(); err != nil {
			return
		}
		if err = ns.writeField(FREE_FORM_TAGS, string(a)); err != nil {
			return
		}
	}
	if req.MatchETag != "" {
		if err = ns.writeField(ETAG, req.MatchETag); err != nil {
			return
		}
	}

	ns.endPayload()

	endRequest(ns)
	return
}

func (req *TableRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	return deserializeTableResult(r, serialVersion)
}

// serialize writes the AddReplicaRequest to data stream using the specified protocol writer.
//
// The fields of AddReplicaRequest are written:
//
//	OpCode: AddReplicaRequest
//	Timeout
//	Tablename
//	Replica name
//	Read/Write units
//	ETag
func (req *AddReplicaRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.AddReplica, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if err = ns.writeField(REGION, req.ReplicaName); err != nil {
		return
	}
	if req.MatchETag != "" {
		if err = ns.writeField(ETAG, req.MatchETag); err != nil {
			return
		}
	}
	if req.ReadUnits > 0 {
		if err = ns.writeField(READ_UNITS, int(req.ReadUnits)); err != nil {
			return
		}
	}
	if req.WriteUnits > 0 {
		if err = ns.writeField(WRITE_UNITS, int(req.WriteUnits)); err != nil {
			return
		}
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *AddReplicaRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	return deserializeTableResult(r, serialVersion)
}

// serialize writes the DropReplicaRequest to data stream using the specified protocol writer.
//
// The fields of DropReplicaRequest are written:
//
//	OpCode: DropReplica
//	Timeout
//	Tablename
//	Replica name
//	ETag
func (req *DropReplicaRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.DropReplica, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if err = ns.writeField(REGION, req.ReplicaName); err != nil {
		return
	}
	if req.MatchETag != "" {
		if err = ns.writeField(ETAG, req.MatchETag); err != nil {
			return
		}
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *DropReplicaRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	return deserializeTableResult(r, serialVersion)
}

// serialize writes the ReplicaStatsRequest to data stream using the specified protocol writer.
//
// The fields of ReplicaStatsRequest are written:
//
//	OpCode: ReplicaStats
//	Timeout
//	Tablename
//	Region
//	StartTime
//	Limit
func (req *ReplicaStatsRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.GetReplicaStats, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if err = ns.writeField(REGION, req.ReplicaName); err != nil {
		return
	}
	if req.StartTime != nil {
		if err = ns.writeField(START, req.StartTime.Format(types.ISO8601ZLayout)); err != nil {
			return
		}
	}
	if req.Limit > 0 {
		if err = ns.writeField(LIST_MAX_TO_READ, req.Limit); err != nil {
			return
		}
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *ReplicaStatsRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &ReplicaStatsResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case TABLE_NAME:
			res.TableName, err = readNsonString(r)
		case NEXT_START_TIME:
			ms, err := readNsonLong(r)
			if err == nil {
				t := time.UnixMilli(ms)
				res.NextStartTime = &t
			}
		case REPLICA_STATS:
			err = res.readReplicaStats(r)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

func (res *ReplicaStatsResult) readReplicaStats(r proto.Reader) error {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return err
	}
	res.StatsRecords = make(map[string][]*ReplicaStats)
	for err == nil && walker.hasNext() {
		walker.next()
		// each NSON field is a replica_name:values map field
		replicaName := walker.getCurrentName()
		// value is an array of ReplicaStats records
		if err = readNsonType(r, types.Array); err != nil {
			return err
		}
		// length in bytes: ignored
		if _, err = r.ReadInt(); err != nil {
			return err
		}
		numElements, err := r.ReadInt()
		if err != nil {
			return err
		}
		records := make([]*ReplicaStats, numElements)
		for i := 0; i < numElements; i++ {
			walker, code, err := newMapWalker(r)
			if err != nil || code != 0 {
				return err
			}
			rs := &ReplicaStats{}
			for err == nil && walker.hasNext() {
				walker.next()
				switch name := walker.getCurrentName(); name {
				case TIME:
					ms, err := readNsonLong(r)
					if err == nil {
						rs.CollectionTime = time.UnixMilli(ms)
					}
				case REPLICA_LAG:
					ms, err := readNsonInt(r, name)
					if err == nil {
						rs.Lag = time.Duration(ms) * time.Millisecond
					}
				default:
					err = skipNsonField(r, name)
				}
			}
			if err != nil {
				return err
			}
			records[i] = rs
		}
		res.StatsRecords[replicaName] = records
	}
	return nil
}

// serialize writes the ListTablesRequest to data stream using the specified protocol writer.
//
// The fields of ListTablesRequest are written in the following order:
//
//	OpCode: ListTables
//	Timeout
//	StartIndex
//	Limit
//	Namespace
func (req *ListTablesRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.ListTables, req.Timeout, "", req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()

	if err = ns.writeField(LIST_START_INDEX, req.StartIndex); err != nil {
		return
	}
	if err = ns.writeField(LIST_MAX_TO_READ, req.Limit); err != nil {
		return
	}
	if req.Namespace != "" {
		if err = ns.writeField(NAMESPACE, req.Namespace); err != nil {
			return
		}
	}

	ns.endPayload()
	endRequest(ns)
	return
}

func (req *ListTablesRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &ListTablesResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case TABLES:
			// array of table names (strings)
			if err = readNsonType(r, types.Array); err != nil {
				return nil, BadProtocol, err
			}
			// length in bytes: ignored
			if _, err = r.ReadInt(); err != nil {
				return nil, BadProtocol, err
			}
			numElements, err := r.ReadInt()
			if err != nil {
				return nil, BadProtocol, err
			}
			res.Tables = make([]string, 0, numElements)
			var str string
			for i := 0; i < numElements; i++ {
				str, err = readNsonString(r)
				if err != nil {
					return nil, BadProtocol, err
				}
				res.Tables = append(res.Tables, str)
			}
		case LAST_INDEX:
			res.LastIndexReturned, err = readNsonUInt(r, name)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the GetIndexesRequest to data stream using the specified protocol writer.
//
// The fields of GetIndexesRequest are written in the following order:
//
//	OpCode: GetIndexes
//	Timeout
//	TableName
//	A bool flag: indicates if index name is set.
//	IndexName: skip if index name is not set.
func (req *GetIndexesRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.GetIndexes, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if req.IndexName != "" {
		if err = ns.writeField(INDEX, req.IndexName); err != nil {
			return
		}
	}
	ns.endPayload()
	endRequest(ns)
	return
}

func (req *GetIndexesRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &GetIndexesResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case INDEXES:
			// array of index info
			if err = readNsonType(r, types.Array); err != nil {
				return nil, BadProtocol, err
			}
			// length in bytes: ignored
			if _, err = r.ReadInt(); err != nil {
				return nil, BadProtocol, err
			}
			numElements, err := r.ReadInt()
			if err != nil {
				return nil, BadProtocol, err
			}
			res.Indexes = make([]IndexInfo, 0, numElements)
			for i := 0; i < numElements; i++ {
				idxInfo, err := readNsonIndexInfo(r)
				if err != nil {
					return nil, BadProtocol, err
				}
				res.Indexes = append(res.Indexes, *idxInfo)
			}
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the DeleteRequest to data stream using the specified protocol writer.
//
// The fields of DeleteRequest are written in the following order:
//
//	OpCode: Delete or DeleteIfVersion
//	Timeout: skip if the request is a a sub request.
//	TableName: skip if the request is a sub request.
//	ReturnRow
//	Key
//	MatchVersion: skip if it is nil
func (req *DeleteRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	return req.serializeInternal(w, serialVersion, true)
}

func (req *DeleteRequest) serializeInternal(w proto.Writer, _ int16, addTableName bool) (err error) {
	ns := startRequest(w)

	var op proto.OpCode
	if req.MatchVersion != nil {
		op = proto.DeleteIfVersion
	} else {
		op = proto.Delete
	}

	// If it is a sub request, do not write timeout.
	// Write tableName if addTableName==true
	if req.isSubRequest {
		if addTableName {
			if err = ns.writeField(TABLE_NAME, req.TableName); err != nil {
				return
			}
		}
		if err = ns.writeField(OP_CODE, int(op)); err != nil {
			return
		}
		if req.abortOnFail {
			if err = ns.writeField(ABORT_ON_FAIL, true); err != nil {
				return
			}
		}
	} else {
		// header
		ns.startHeader()
		if err = ns.writeHeader(op, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
			return
		}
		ns.endHeader()
	}

	if !req.isSubRequest {
		ns.startPayload()
	}

	if req.ReturnRow {
		if err = ns.writeField(RETURN_ROW, req.ReturnRow); err != nil {
			return
		}
	}

	if !req.isSubRequest {
		if err = ns.writeDurability(req.Durability); err != nil {
			return
		}
	}

	if err = ns.writeField(KEY, req.Key); err != nil {
		return
	}

	// Write match version if it is not nil.
	if req.MatchVersion != nil {
		ns.startField(ROW_VERSION)
		ns.writer.WriteByte(byte(types.Binary))
		if _, err = w.WriteVersion(req.MatchVersion); err != nil {
			return
		}
		ns.endField(ROW_VERSION)
	}

	if !req.isSubRequest {
		ns.endPayload()
	}

	endRequest(ns)
	return
}

func (req *DeleteRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &DeleteResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case CONSUMED:
			res.Capacity, err = readNsonConsumedCapacity(r)
		case SUCCESS:
			res.Success, err = readNsonBoolean(r)
		case RETURN_INFO:
			res.WriteResult, err = readNsonWriteResult(r)
		case TOPOLOGY_INFO:
			err = res.SetTopologyOrErr(readNsonTopologyInfo(r))
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the PutRequest to data stream using the specified protocol writer.
//
// The fields of PutRequest are written in the following order:
//
//	OpCode: either Put, PutIfAbsent, PutIfPresent or PutIfVersion.
//	Timeout: skip if the request is a a sub request.
//	TableName: skip if the request is a sub request.
//	ReturnRow
//	ExactMatch
//	IdentityCacheSize
//	Value
//	UpdateTTL: this is true if UseTableTTL or TTL is set.
//	TTL
//	MatchVersion: skip if it is nil.
func (req *PutRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	return req.serializeInternal(w, serialVersion, true)
}

func (req *PutRequest) serializeInternal(w proto.Writer, _ int16, addTableName bool) (err error) {

	ns := startRequest(w)

	var op proto.OpCode
	switch req.PutOption {
	case types.PutIfAbsent, types.PutIfPresent, types.PutIfVersion:
		op = proto.OpCode(req.PutOption)
	default:
		op = proto.Put
	}

	// If it is a sub request, do not write timeout.
	// Write tableName if addTableName==true
	if req.isSubRequest {
		if addTableName {
			if err = ns.writeField(TABLE_NAME, req.TableName); err != nil {
				return
			}
		}
		if err = ns.writeField(OP_CODE, int(op)); err != nil {
			return
		}
		if req.abortOnFail {
			if err = ns.writeField(ABORT_ON_FAIL, true); err != nil {
				return
			}
		}
	} else {
		// header
		ns.startHeader()
		if err = ns.writeHeader(op, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
			return
		}
		ns.endHeader()
	}

	if !req.isSubRequest {
		ns.startPayload()
	}

	if req.ReturnRow {
		if err = ns.writeField(RETURN_ROW, req.ReturnRow); err != nil {
			return
		}
	}

	if !req.isSubRequest {
		if err = ns.writeDurability(req.Durability); err != nil {
			return
		}
	}

	if req.ExactMatch {
		if err = ns.writeField(EXACT_MATCH, req.ExactMatch); err != nil {
			return
		}
	}

	// TODO: write only if non-default
	if err = ns.writeField(IDENTITY_CACHE_SIZE, req.IdentityCacheSize); err != nil {
		return
	}

	if req.Value != nil {
		if err = ns.writeField(VALUE, req.Value); err != nil {
			return
		}
	} else if req.StructValue != nil {
		if err = ns.writeStructField(VALUE, req.StructValue); err != nil {
			return
		}
	} else {
		return fmt.Errorf("missing Value in PutRequest")
	}

	if req.updateTTL() {
		if err = ns.writeField(UPDATE_TTL, req.updateTTL()); err != nil {
			return
		}
	}

	if err = ns.writeTTL(req.TTL); err != nil {
		return
	}

	// Write match version if it is not nil.
	if req.MatchVersion != nil {
		ns.startField(ROW_VERSION)
		ns.writer.WriteByte(byte(types.Binary))
		if _, err = w.WriteVersion(req.MatchVersion); err != nil {
			return
		}
		ns.endField(ROW_VERSION)
	}

	if !req.isSubRequest {
		ns.endPayload()
	}

	endRequest(ns)
	return
}

func (req *PutRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &PutResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case CONSUMED:
			res.Capacity, err = readNsonConsumedCapacity(r)
		case TOPOLOGY_INFO:
			err = res.SetTopologyOrErr(readNsonTopologyInfo(r))
		case ROW_VERSION:
			res.Version, err = readNsonVersion(r)
		case RETURN_INFO:
			res.WriteResult, err = readNsonWriteResult(r)
		case GENERATED:
			res.GeneratedValue, err = r.ReadFieldValue()
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the TableUsageRequest to data stream using the specified protocol writer.
//
// The fields of TableUsageRequest are written in the following order:
//
//	OpCode: GetTableUsage
//	Timeout
//	TableName
//	StartTime
//	EndTime
//	Limit
func (req *TableUsageRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	ns.startHeader()
	if err = ns.writeHeader(proto.GetTableUsage, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	ns.startPayload()
	if err = ns.writeField(START, req.StartTime.Format(types.ISO8601ZLayout)); err != nil {
		return
	}
	if err = ns.writeField(END, req.EndTime.Format(types.ISO8601ZLayout)); err != nil {
		return
	}
	if err = ns.writeField(LIST_MAX_TO_READ, req.Limit); err != nil {
		return
	}
	if err = ns.writeField(LIST_START_INDEX, req.StartIndex); err != nil {
		return
	}
	ns.endPayload()

	endRequest(ns)
	return
}

func (req *TableUsageRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &TableUsageResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case TABLE_NAME:
			res.TableName, err = readNsonString(r)
		case TABLE_USAGE:
			// array of usage records
			if err = readNsonType(r, types.Array); err != nil {
				return nil, BadProtocol, err
			}
			// length in bytes: ignored
			if _, err = r.ReadInt(); err != nil {
				return nil, BadProtocol, err
			}
			numElements, err := r.ReadInt()
			if err != nil {
				return nil, BadProtocol, err
			}
			res.UsageRecords = make([]TableUsage, 0, numElements)
			for i := 0; i < numElements; i++ {
				usage, err := readNsonUsageRecord(r)
				if err != nil {
					return nil, BadProtocol, err
				}
				res.UsageRecords = append(res.UsageRecords, *usage)
			}
		case LAST_INDEX:
			res.LastIndexReturned, err = readNsonInt(r, name)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the MultiDeleteRequest to data stream using the specified protocol writer.
//
// The fields of MultiDeleteRequest are written in the following order:
//
//	OpCode: MultiDelete
//	Timeout
//	TableName
//	Key
//	FieldRange
//	MaxWriteKB
//	ContinuationKey
func (req *MultiDeleteRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.MultiDelete, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	// payload
	ns.startPayload()

	if err = ns.writeDurability(req.Durability); err != nil {
		return
	}
	if err = ns.writeField(MAX_WRITE_KB, req.MaxWriteKB); err != nil {
		return
	}
	if err = ns.writeField(KEY, req.Key); err != nil {
		return
	}
	if req.ContinuationKey != nil {
		if err = ns.writeField(CONTINUATION_KEY, req.ContinuationKey); err != nil {
			return
		}
	}

	if err = ns.writeFieldRange(req.FieldRange); err != nil {
		return
	}

	ns.endPayload()

	endRequest(ns)
	return nil
}

func (req *MultiDeleteRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &MultiDeleteResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case CONSUMED:
			res.Capacity, err = readNsonConsumedCapacity(r)
		case TOPOLOGY_INFO:
			err = res.SetTopologyOrErr(readNsonTopologyInfo(r))
		case NUM_DELETIONS:
			res.NumDeleted, err = readNsonInt(r, name)
		case CONTINUATION_KEY:
			res.ContinuationKey, err = readNsonBinary(r)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the WriteMultipleRequest to data stream using the specified protocol writer.
//
// The fields of WriteMultipleRequest are written in the following order:
//
//	OpCode: WriteMultiple
//	Timeout
//	TableName
//	Number of operations
//	All sub operations: either put or delete operation
func (req *WriteMultipleRequest) serialize(w proto.Writer, serialVersion int16, _ int16) (err error) {
	ns := startRequest(w)

	// if all sub-operations are on the same table, place that table name in the header.
	// otherwise, put table names in each sub-operation.
	tableName := ""
	isSingleTable := true
	for _, operation := range req.Operations {
		if tableName == "" {
			tableName = operation.tableName()
		} else {
			if !strings.EqualFold(tableName, operation.tableName()) {
				isSingleTable = false
				break
			}
		}
	}

	if !isSingleTable {
		tableName = ""
	}

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.WriteMultiple, req.Timeout, tableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	// payload
	ns.startPayload()

	if err = ns.writeDurability(req.Durability); err != nil {
		return
	}
	numOps := len(req.Operations)
	if err = ns.writeField(NUM_OPERATIONS, numOps); err != nil {
		return
	}

	ns.startArray(OPERATIONS)

	var subReq Request
	for _, operation := range req.Operations {
		ns.startArrayField(0)
		n1 := w.Size()

		if operation.DeleteRequest != nil {
			operation.DeleteRequest.abortOnFail = operation.AbortOnFail
			subReq = operation.DeleteRequest
			err = operation.DeleteRequest.serializeInternal(w, serialVersion, !isSingleTable)
		} else if operation.PutRequest != nil {
			operation.PutRequest.abortOnFail = operation.AbortOnFail
			subReq = operation.PutRequest
			err = operation.PutRequest.serializeInternal(w, serialVersion, !isSingleTable)
		}

		if err != nil {
			return
		}

		if req.checkSubReqSize {
			n := w.Size() - n1
			// Check size limit for each sub request.
			if err = checkRequestSizeLimit(subReq, n); err != nil {
				return
			}
		}
		ns.endArrayField(0)
	}
	ns.endArray(OPERATIONS)

	ns.endPayload()

	endRequest(ns)
	return nil
}

func (req *WriteMultipleRequest) deserialize(r proto.Reader, serialVersion int16, _ int16) (Result, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &WriteMultipleResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		name := walker.getCurrentName()
		switch name {
		//switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case CONSUMED:
			res.Capacity, err = readNsonConsumedCapacity(r)
		case TOPOLOGY_INFO:
			err = res.SetTopologyOrErr(readNsonTopologyInfo(r))
		case WM_SUCCESS:
			// success is an array of map
			res.FailedOperationIndex = -1
			if err = readNsonType(r, types.Array); err != nil {
				return nil, BadProtocol, err
			}
			// length in bytes: ignored
			if _, err = r.ReadInt(); err != nil {
				return nil, BadProtocol, err
			}
			numElements, err := r.ReadInt()
			if err != nil {
				return nil, BadProtocol, err
			}
			res.ResultSet = make([]OperationResult, 0, numElements)
			for i := 0; i < numElements; i++ {
				opRes, err := readNsonOperationResult(r)
				if err != nil {
					return nil, BadProtocol, err
				}
				res.ResultSet = append(res.ResultSet, *opRes)
			}
		case WM_FAILURE:
			// failure is a map
			var lw *mapWalker
			lw, _, err = newMapWalker(r)
			for err == nil && lw.hasNext() {
				lw.next()
				switch name := lw.getCurrentName(); name {
				case WM_FAIL_INDEX:
					res.FailedOperationIndex, err = readNsonInt(r, name)
				case WM_FAIL_RESULT:
					var opRes *OperationResult
					opRes, err = readNsonOperationResult(r)
					if err == nil {
						res.ResultSet = make([]OperationResult, 1)
						res.ResultSet[0] = *opRes
					}
				default:
					err = skipNsonField(r, name)
				}
			}
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

// serialize writes the PrepareRequest to data stream using the specified protocol writer.
//
// The fields of PrepareRequest are written in the following order:
//
//	OpCode: Prepare
//	Timeout
//	Statement
//	QueryVersion
//	GetQueryPlan
func (req *PrepareRequest) serialize(w proto.Writer, serialVersion int16, queryVersion int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.Prepare, req.Timeout, "", req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	// payload
	ns.startPayload()

	if err = ns.writeField(QUERY_VERSION, queryVersion); err != nil {
		return
	}
	if err = ns.writeField(STATEMENT, req.Statement); err != nil {
		return
	}
	if req.GetQueryPlan {
		if err = ns.writeField(GET_QUERY_PLAN, true); err != nil {
			return
		}
	}
	if req.GetQuerySchema {
		if err = ns.writeField(GET_QUERY_SCHEMA, true); err != nil {
			return
		}
	}

	ns.endPayload()

	endRequest(ns)
	return nil
}

func (req *PrepareRequest) deserialize(r proto.Reader, serialVersion int16, queryVersion int16) (res Result, code int, err error) {
	pres := &PrepareResult{}
	code, err = readNsonPrepareOrQuery(nil, nil, req, pres, r, serialVersion, queryVersion)
	return pres, code, err
}

func (req *QueryRequest) serialize(w proto.Writer, serialVersion int16, queryVersion int16) (err error) {
	ns := startRequest(w)

	// header
	ns.startHeader()
	if err = ns.writeHeader(proto.Query, req.Timeout, req.TableName, req.GetTopologyInfo()); err != nil {
		return
	}
	ns.endHeader()

	// payload
	ns.startPayload()

	if err = ns.writeConsistency(req.Consistency); err != nil {
		return
	}
	if err = ns.writeDurability(req.Durability); err != nil {
		return
	}

	// only write if nonzero
	if err = ns.writeNZField(MAX_READ_KB, int(req.MaxReadKB)); err != nil {
		return
	}
	if err = ns.writeNZField(MAX_WRITE_KB, int(req.MaxWriteKB)); err != nil {
		return
	}
	if err = ns.writeNZField(NUMBER_LIMIT, int(req.Limit)); err != nil {
		return
	}
	if err = ns.writeNZField(TRACE_LEVEL, req.traceLevel); err != nil {
		return
	}

	if err = ns.writeField(QUERY_VERSION, queryVersion); err != nil {
		return
	}

	if req.isPrepared() {
		if req.PreparedStatement == nil {
			return fmt.Errorf("request isPrepared, but has no PreparedStatement")
		}
		if err = ns.writeField(IS_PREPARED, true); err != nil {
			return
		}
		if err = ns.writeField(IS_SIMPLE_QUERY, req.isSimpleQuery()); err != nil {
			return
		}
		pstmt := req.PreparedStatement
		if err = ns.writeField(PREPARED_QUERY, pstmt.statement); err != nil {
			return
		}
		if err = ns.writeBindVariables(pstmt.bindVariables); err != nil {
			return
		}
	} else {
		if err = ns.writeField(STATEMENT, req.Statement); err != nil {
			return
		}
	}

	if req.continuationKey != nil {
		if err = ns.writeField(CONTINUATION_KEY, req.continuationKey); err != nil {
			return
		}
	}

	if err = ns.writeNZLongField(SERVER_MEMORY_CONSUMPTION, req.MaxServerMemoryConsumption); err != nil {
		return
	}

	if err = ns.writeMathContext(req.getFPArithSpec()); err != nil {
		return
	}

	if req.getShardID() != -1 { // default
		if err = ns.writeField(SHARD_ID, req.getShardID()); err != nil {
			return
		}
	}
	if queryVersion >= 4 {
		// TODO req.queryName
		if err = ns.writeVirtualScan(req.getVirtualScan()); err != nil {
			return
		}
	} else if req.GetTopologyInfo() != nil {
		if err = ns.writeField(TOPO_SEQ_NUM, req.GetTopologyInfo().SeqNum); err != nil {
			return
		}
	}

	ns.endPayload()

	endRequest(ns)
	return nil
}

func (ns *NsonSerializer) writeVirtualScan(vs *virtualScan) (err error) {
	if vs == nil {
		return nil
	}
	ns.startMap(VSCAN)
	ns.writeField(VSCAN_SID, vs.shardID)
	ns.writeField(VSCAN_PID, vs.partitionID)
	if vs.firstBatch {
		ns.writeField(VSCAN_PRIM_KEY, vs.primResumeKey)
		ns.writeField(VSCAN_SEC_KEY, vs.secResumeKey)
		ns.writeField(VSCAN_MOVE_AFTER, vs.moveAfterResumeKey)
		ns.writeField(VSCAN_JOIN_DESC_RESUME_KEY, vs.descResumeKey)
		ns.writeField(VSCAN_JOIN_PATH_KEY, vs.joinPathKey)
		ns.writeField(VSCAN_JOIN_PATH_TABLES, vs.joinPathTables)
		ns.writeField(VSCAN_JOIN_PATH_SEC_KEY, vs.joinPathSecKey)
		ns.writeField(VSCAN_JOIN_PATH_MATCHED, vs.joinPathMatched)
	}
	ns.endMap(VSCAN)
	return nil
}

func (ns *NsonSerializer) writeMathContext(mathCtx *FPArithSpec) (err error) {
	if mathCtx == nil {
		return nil
	}

	var val int
	switch *mathCtx {
	case Decimal32:
		// default: no need to write
		return nil
	case Decimal64:
		val = 2
	case Decimal128:
		val = 3
	case Unlimited:
		val = 4
	default:
		val = 5
		if err = ns.writeField(MATH_CONTEXT_PRECISION, int(mathCtx.Precision)); err != nil {
			return
		}
		var mode int
		switch mathCtx.RoundingMode {
		case big.AwayFromZero:
			// This equals to RoundingMode.UP in Java.
			mode = 0
		case big.ToZero:
			// This equals to RoundingMode.DOWN in Java.
			mode = 1
		case big.ToPositiveInf:
			// This equals to RoundingMode.CEILING in Java.
			mode = 2
		case big.ToNegativeInf:
			// This equals to RoundingMode.FLOOR in Java.
			mode = 3
		case big.ToNearestAway:
			// This equals to RoundingMode.HALF_UP in Java.
			mode = 4
		case big.ToNearestEven:
			// This equals to RoundingMode.HALF_EVEN in Java.
			mode = 6
		default:
			mode = 6
		}
		if err = ns.writeField(MATH_CONTEXT_ROUNDING_MODE, mode); err != nil {
			return
		}
	}
	return ns.writeField(MATH_CONTEXT_CODE, val)
}

func (req *QueryRequest) deserialize(r proto.Reader, serialVersion int16, queryVersion int16) (res Result, code int, err error) {
	qres := newQueryResult(req, true)
	code, err = readNsonPrepareOrQuery(req, qres, nil, nil, r, serialVersion, queryVersion)
	return qres, code, err
}

func deserializeSystemResult(r proto.Reader) (*SystemResult, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &SystemResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case SYSOP_STATE:
			var state int
			state, err = readNsonInt(r, name)
			if err == nil {
				res.State = toOperationState(byte(state))
			}
		case OPERATION_ID:
			res.OperationID, err = readNsonString(r)
		case STATEMENT:
			res.Statement, err = readNsonString(r)
		case SYSOP_RESULT:
			res.ResultString, err = readNsonString(r)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}

	return res, 0, nil
}

func deserializeTableResult(r proto.Reader, _ int16) (*TableResult, int, error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, code, err
	}
	res := &TableResult{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			code, err := walker.handleErrorCode()
			if err != nil || code != 0 {
				return nil, code, err
			}
		case TABLE_NAME:
			res.TableName, err = readNsonString(r)
		case COMPARTMENT_OCID:
			res.CompartmentOrNamespace, err = readNsonString(r)
		case NAMESPACE:
			res.CompartmentOrNamespace, err = readNsonString(r)
		case TABLE_OCID:
			res.TableOcid, err = readNsonString(r)
		case TABLE_SCHEMA:
			res.Schema, err = readNsonString(r)
		case TABLE_DDL:
			res.DDL, err = readNsonString(r)
		case TABLE_STATE:
			var state int
			if state, err = readNsonInt(r, name); err == nil {
				res.State = types.TableState(state)
			}
		case OPERATION_ID:
			res.OperationID, err = readNsonString(r)
		case FREE_FORM_TAGS:
			var fft string
			if fft, err = readNsonString(r); err == nil {
				var mv *types.MapValue
				if mv, err = types.NewMapValueFromJSON(fft); err == nil {
					res.FreeFormTags = &types.FreeFormTags{Tags: mv}
				}
			}
		case DEFINED_TAGS:
			var dt string
			if dt, err = readNsonString(r); err == nil {
				var mv *types.MapValue
				if mv, err = types.NewMapValueFromJSON(dt); err == nil {
					res.DefinedTags = &types.DefinedTags{Tags: mv}
				}
			}
		case ETAG:
			res.MatchETag, err = readNsonString(r)
		case SCHEMA_FROZEN:
			res.SchemaFrozen, err = readNsonBoolean(r)
		case INITIALIZED:
			res.IsLocalReplicaInitialized, err = readNsonBoolean(r)
		case REPLICAS:
			res.Replicas, err = readNsonReplicas(r)
		case LIMITS:
			var lw *mapWalker
			lw, _, err = newMapWalker(r)
			limits := &TableLimits{0, 0, 0, types.Provisioned}
			for err == nil && lw.hasNext() {
				lw.next()
				switch name = lw.getCurrentName(); name {
				case READ_UNITS:
					limits.ReadUnits, err = readNsonUInt(r, name)
				case WRITE_UNITS:
					limits.WriteUnits, err = readNsonUInt(r, name)
				case STORAGE_GB:
					limits.StorageGB, err = readNsonUInt(r, name)
				case LIMITS_MODE:
					limits.CapacityMode, err = readNsonCapacityMode(r)
				default:
					err = skipNsonField(r, name)
				}
			}
			if err == nil {
				res.Limits = *limits
			}
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, BadProtocol, err
	}
	return res, 0, nil
}

func readNsonReplicas(r proto.Reader) (replicas []*Replica, err error) {
	// array of replicas
	if err = readNsonType(r, types.Array); err != nil {
		return nil, err
	}
	// length in bytes: ignored
	if _, err = r.ReadInt(); err != nil {
		return nil, err
	}
	// number of array elements
	numElements, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	replicas = make([]*Replica, numElements)
	for i := 0; i < numElements; i++ {
		rep, err := readNsonReplica(r)
		if err != nil {
			return nil, err
		}
		replicas[i] = rep
	}
	return replicas, nil
}

func readNsonReplica(r proto.Reader) (rep *Replica, err error) {
	walker, code, err := newMapWalker(r)
	if err != nil || code != 0 {
		return nil, err
	}
	rep = &Replica{}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case REGION:
			rep.Name, err = readNsonString(r)
		case TABLE_OCID:
			rep.TableOcid, err = readNsonString(r)
		case WRITE_UNITS:
			rep.WriteUnits, err = readNsonInt(r, name)
		case LIMITS_MODE:
			rep.CapacityMode, err = readNsonCapacityMode(r)
		case TABLE_STATE:
			var state int
			if state, err = readNsonInt(r, name); err == nil {
				rep.State = types.TableState(state)
			}
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, err
	}
	return rep, nil
}

// NsonSerializer is the base struct used for all serialization.
type NsonSerializer struct {

	// writer is used to do all the actual writing.
	writer proto.Writer

	// offsetStack represents the number of bytes used for maps or arrays
	offsetStack []int

	// sizeStack represents the number of elements in maps or arrays
	sizeStack []int
}

/*
 * Maps and Arrays. These objects start with their total length,
 * allowing them to be optionally skipped on deserialization.
 *  1. start:
 *    make a 4-byte space for the ultimate length of the serialized
 *    object.
 *  2. save the offset on a stack
 *  3. start counting elements on a stack
 *  4. ... entries are written
 *  5. end:
 *    a. pop the offset stack to get the original length offset
 *    write the real length into the spot that was held
 *    b. pop the size stack to get the number of elements
 *    write the real number of elements the spot that was held
 * NOTE: a full 4-byte integer is used to avoid the variable-length
 * encoding used by compressed integers.
 *
 * It would be more efficient and avoid an extra stack with pop/push
 * for each map/array element to rely on the size from the caller
 * but counting elements here is safer and doesn't rely on the caller
 * having access to the size information. For example, a caller may be
 * turning a List (via iterator) into an array. That is less likely
 * for a Map but it's simplest to keep them the same. Alternatively
 * the caller could track number of elements and send it correctly in
 * the end* calls but again, that relies on the caller.
 */

func (ns *NsonSerializer) startMap(field string) {
	if field != "" {
		ns.startField(field)
	}
	ns.writer.WriteByte(byte(types.Map))
	off := ns.writer.Size()
	ns.writer.WriteInt(0) // size in bytes
	ns.writer.WriteInt(0) // number of elements
	ns.offsetStack = append(ns.offsetStack, off)
	ns.sizeStack = append(ns.sizeStack, 0)
}

func (ns *NsonSerializer) startArray(field string) {
	if field != "" {
		ns.startField(field)
	}
	ns.writer.WriteByte(byte(types.Array))
	off := ns.writer.Size()
	ns.writer.WriteInt(0) // size in bytes
	ns.writer.WriteInt(0) // number of elements
	ns.offsetStack = append(ns.offsetStack, off)
	ns.sizeStack = append(ns.sizeStack, 0)
}

func (ns *NsonSerializer) endMap(field string) {
	olen := len(ns.offsetStack)
	lengthOffset := ns.offsetStack[olen-1]
	ns.offsetStack = ns.offsetStack[:olen-1]
	slen := len(ns.sizeStack)
	numElems := ns.sizeStack[slen-1]
	ns.sizeStack = ns.sizeStack[:slen-1]
	start := lengthOffset + 4
	// write size in bytes, then number of elements into the space reserved
	ns.writer.WriteIntAtOffset(ns.writer.Size()-start, lengthOffset)
	ns.writer.WriteIntAtOffset(numElems, lengthOffset+4)
	if field != "" {
		ns.endField(field)
	}
}

func (ns *NsonSerializer) endArray(field string) {
	ns.endMap(field)
}

func (ns *NsonSerializer) startField(key string) {
	ns.writer.WriteString(&key)
}

func (ns *NsonSerializer) endField(_ string) {
	// add one to number of elements
	ns.incrSize(1)
}

func (ns *NsonSerializer) startArrayField(index int) {
	// nothing to do
}

func (ns *NsonSerializer) endArrayField(_ int) {
	// add one to number of elements
	ns.incrSize(1)
}

func (ns *NsonSerializer) writeField(key string, value types.FieldValue) (err error) {
	ns.startField(key)
	if _, err = ns.writer.WriteFieldValue(value); err != nil {
		return
	}
	ns.endField(key)
	return nil
}

func (ns *NsonSerializer) writeStructField(key string, value any) (err error) {
	ns.startField(key)
	if _, err = ns.writer.WriteStructValue(value); err != nil {
		return
	}
	ns.endField(key)
	return nil
}

func (ns *NsonSerializer) writeNZField(key string, value int) (err error) {
	if value <= 0 {
		return nil
	}
	ns.startField(key)
	if _, err = ns.writer.WriteFieldValue(value); err != nil {
		return
	}
	ns.endField(key)
	return nil
}

func (ns *NsonSerializer) writeNZLongField(key string, value int64) (err error) {
	if value <= 0 {
		return nil
	}
	ns.startField(key)
	if _, err = ns.writer.WriteFieldValue(value); err != nil {
		return
	}
	ns.endField(key)
	return nil
}

func (ns *NsonSerializer) writeTTL(value *types.TimeToLive) (err error) {
	if value == nil {
		return nil
	}
	// write value as a string like "10 DAYS"
	var unitStr string
	if value.Unit == types.Days {
		unitStr = "DAYS"
	} else if value.Unit == types.Hours {
		unitStr = "HOURS"
	} else {
		return errors.New("nson.Writer: invalid TTL unit")
	}
	ttl := fmt.Sprintf("%d %s", value.Value, unitStr)
	return ns.writeField(TTL, ttl)
}

func (ns *NsonSerializer) writeDurability(value types.Durability) (err error) {
	// write nothing if default
	if !value.IsSet() {
		return nil
	}
	// the durability value accepted by server, which is
	// a single byte (cast to an int in V4) with 3 2-bit fields.
	var dur int = int(value.MasterSync)
	dur |= int(value.ReplicaSync << 2)
	dur |= int(value.ReplicaAck << 4)
	return ns.writeField(DURABILITY, dur)
}

func (ns *NsonSerializer) writeConsistency(value types.Consistency) (err error) {
	ns.startMap(CONSISTENCY)

	// NSON uses an integer for consistency, whereas v3 uses a byte
	ns.startField(TYPE)
	ns.writer.WriteByte(byte(types.Integer))
	var c int
	if value == types.Absolute || value == types.Eventual {
		c = int(value) - 1
	} else {
		c = int(value)
	}
	if _, err = ns.writer.WritePackedInt(c); err != nil {
		return
	}
	ns.endField(TYPE)
	ns.endMap(CONSISTENCY)
	return nil
}

func (ns *NsonSerializer) writeFieldRange(value *types.FieldRange) (err error) {
	if value == nil {
		return nil
	}
	ns.startMap(RANGE)

	if err = ns.writeField(RANGE_PATH, value.FieldPath); err != nil {
		return
	}

	if value.Start != nil {
		ns.startMap(START)
		if err = ns.writeField(VALUE, value.Start); err != nil {
			return
		}
		if err = ns.writeField(INCLUSIVE, value.StartInclusive); err != nil {
			return
		}
		ns.endMap(START)
	}

	if value.End != nil {
		ns.startMap(END)
		if err = ns.writeField(VALUE, value.End); err != nil {
			return
		}
		if err = ns.writeField(INCLUSIVE, value.EndInclusive); err != nil {
			return
		}
		ns.endMap(END)
	}

	ns.endMap(RANGE)
	return nil
}

func (ns *NsonSerializer) incrSize(delta int) {
	off := len(ns.sizeStack)
	value := ns.sizeStack[off-1] + delta
	ns.sizeStack[off-1] = value
}

func (ns *NsonSerializer) writeHeader(op proto.OpCode, timeout time.Duration, tableName string, topo *common.TopologyInfo) (err error) {
	if err = ns.writeField(VERSION, 4); err != nil {
		return
	}
	if err = ns.writeField(OP_CODE, int(op)); err != nil {
		return
	}

	timeoutMs := int(timeout.Nanoseconds() / 1e6)
	if err = ns.writeField(TIMEOUT, timeoutMs); err != nil {
		return
	}

	if tableName != "" {
		if err = ns.writeField(TABLE_NAME, tableName); err != nil {
			return
		}
	}

	seqNum := -1
	if topo != nil {
		seqNum = topo.SeqNum
	}
	if err = ns.writeField(TOPO_SEQ_NUM, seqNum); err != nil {
		return
	}

	return
}

func (ns *NsonSerializer) startHeader() {
	ns.startMap(HEADER)
}

func (ns *NsonSerializer) endHeader() {
	ns.endMap(HEADER)
}

func (ns *NsonSerializer) startPayload() {
	ns.startMap(PAYLOAD)
}

func (ns *NsonSerializer) endPayload() {
	ns.endMap(PAYLOAD)
}

func startRequest(w proto.Writer) *NsonSerializer {
	ns := &NsonSerializer{w, nil, nil}
	// top-level object
	ns.startMap("")
	return ns
}

func endRequest(ns *NsonSerializer) {
	ns.endMap("")
}

// to prevent an infinite loop for bad serialization
const maxNumElements int = 1000000000

// mapWalker is the base struct used for walking all NSON results
type mapWalker struct {
	reader       proto.Reader
	numElements  int
	currentName  string
	currentIndex int
}

// newMapWalker returns a MapWalker based on the reader.
// If the client is connected to a pre-V4 server, and the client tries
// to deserialize using V4, the mapWalker constructor will throw an
// IllegalArgumentException, because the following codes will be
// returned from previous servers:
//
//	V3: UNSUPPORTED_PROTOCOL (24)
//	V2: BAD_PROTOCOL_MESSAGE (17)
//
// Neither of these maps to any valid Nson field.
// Convert the error to an UnsupportedProtocolException so the client's
// serial version negotiation logic will detect it and decrement
// the serial version accordingly.
func newMapWalker(r proto.Reader) (*mapWalker, int, error) {
	t, err := r.ReadByte()
	if err != nil {
		return nil, BadProtocol, err
	}
	if types.DbType(t) != types.Map {
		if t == byte(UnsupportedProtocol) || t == byte(BadProtocol) {
			return nil, UnsupportedProtocol, fmt.Errorf("unsupported protocol")
		}
		return nil, BadProtocol, fmt.Errorf("stream must point to a MAP, it points to %v of type %[1]T", t)
	}
	_, err = r.ReadInt() // total length of map in bytes
	if err != nil {
		return nil, BadProtocol, err
	}
	numElements, err := r.ReadInt()
	if err != nil {
		return nil, BadProtocol, err
	}
	if numElements < 0 || numElements > maxNumElements {
		return nil, BadProtocol, fmt.Errorf("invalid number of map elements: %d", numElements)
	}
	return &mapWalker{r, numElements, "", 0}, 0, nil
}

func (mw *mapWalker) hasNext() bool {
	return mw.currentIndex < mw.numElements
}

func (mw *mapWalker) next() error {
	if !mw.hasNext() {
		return fmt.Errorf("cannot call next with no elements remaining")
	}
	var err error
	mw.currentName, err = mw.reader.ReadNonNilString()
	if err != nil {
		return err
	}
	mw.currentIndex++
	return nil
}

func (mw *mapWalker) getCurrentName() string {
	return mw.currentName
}

func (mw *mapWalker) GetReader() *proto.Reader {
	return &mw.reader
}

func (mw *mapWalker) handleErrorCode() (int, error) {
	code, err := readNsonInt(mw.reader, ERROR_CODE)
	if err != nil {
		return BadProtocol, err
	}
	if code == 0 {
		return 0, nil
	}

	var msg string
	// get EXCEPTION message
	for mw.hasNext() {
		mw.next()
		switch name := mw.getCurrentName(); name {
		case EXCEPTION:
			msg, err = readNsonString(mw.reader)
			if nsondebug {
				fmt.Fprintf(os.Stderr, "Got error code %d from server: %s\n", code, msg)
			}
		// TODO: CONSUMED
		default:
			err = skipNsonField(mw.reader, name)
		}
	}
	if err != nil {
		msg = err.Error()
	}
	if msg != "" {
		return code, fmt.Errorf("%s", msg)
	}

	return code, fmt.Errorf("unknown error: code=%d", code)
}

func skipNsonField(r proto.Reader, name string) (err error) {
	if nsondebug {
		fmt.Fprintf(os.Stderr, " Skipping field '%s'\n", nsonReadable(name))
	}
	t, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch types.DbType(t) {
	case types.Array, types.Map:
		length, err := r.ReadInt()
		if err != nil {
			return err
		}
		r.GetBuffer().Next(length)
		return nil
	case types.Binary:
		// would be nice to skip allocating a []byte here...
		_, err = r.ReadByteArray()
		return err
	case types.Boolean:
		_, err = r.ReadBoolean()
		return err
	case types.Double:
		_, err = r.ReadDouble()
		return err
	case types.Integer:
		_, err = r.ReadPackedInt()
		return err
	case types.Long:
		_, err = r.ReadPackedLong()
		return err
	case types.String, types.Timestamp, types.Number:
		_, err = r.ReadString()
		return err
	case types.JSONNull, types.Null, types.Empty:
		return nil
	default:
		return fmt.Errorf("nson.Reader: unsupported field value %v of type %[1]T", t)
	}
}

func readNsonType(r proto.Reader, t types.DbType) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	if types.DbType(b) != t {
		return fmt.Errorf("expected type %v, got type %v", t, types.DbType(b))
	}
	return nil
}

func readNsonString(r proto.Reader) (string, error) {
	if err := readNsonType(r, types.String); err != nil {
		return "", err
	}
	return r.ReadNonNilString()
}

func readNsonBoolean(r proto.Reader) (bool, error) {
	if err := readNsonType(r, types.Boolean); err != nil {
		return false, err
	}
	return r.ReadBoolean()
}

// nson ints are always packed
func readNsonInt(r proto.Reader, field string) (int, error) {
	if err := readNsonType(r, types.Integer); err != nil {
		return 0, fmt.Errorf("error reading field %s: %s", nsonReadable(field), err.Error())
	}
	val, err := r.ReadPackedInt()
	if err != nil {
		return 0, fmt.Errorf("error reading field %s: %s", nsonReadable(field), err.Error())
	}
	return val, nil
}

// nson longs are always packed
func readNsonLong(r proto.Reader) (int64, error) {
	if err := readNsonType(r, types.Long); err != nil {
		return 0, err
	}
	return r.ReadPackedLong()
}

func readNsonUInt(r proto.Reader, field string) (uint, error) {
	val, err := readNsonInt(r, field)
	if err != nil {
		return 0, err
	}
	return uint(val), nil
}

func readNsonVersion(r proto.Reader) (types.Version, error) {
	if err := readNsonType(r, types.Binary); err != nil {
		return nil, err
	}
	return r.ReadByteArray()
}

func readNsonBinary(r proto.Reader) ([]byte, error) {
	if err := readNsonType(r, types.Binary); err != nil {
		return nil, err
	}
	return r.ReadByteArray()
}

func readNsonCapacityMode(r proto.Reader) (types.CapacityMode, error) {
	m, err := readNsonInt(r, LIMITS_MODE)
	if err != nil {
		return types.Provisioned, err
	}
	if m == 2 {
		return types.OnDemand, nil
	}
	return types.Provisioned, nil
}

func readNsonConsumedCapacity(r proto.Reader) (Capacity, error) {
	c := &Capacity{0, 0, 0}
	lw, _, err := newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case READ_UNITS:
			c.ReadUnits, err = readNsonInt(r, READ_UNITS)
		case READ_KB:
			c.ReadKB, err = readNsonInt(r, READ_KB)
		case WRITE_KB:
			c.WriteKB, err = readNsonInt(r, WRITE_KB)
		default:
			err = skipNsonField(r, name)
		}
	}
	return *c, err
}

func readNsonVirtualScan(r proto.Reader) (*virtualScan, error) {
	vs := &virtualScan{}
	lw, _, err := newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case VSCAN_SID:
			vs.shardID, err = readNsonInt(r, name)
		case VSCAN_PID:
			vs.partitionID, err = readNsonInt(r, name)
		case VSCAN_PRIM_KEY:
			vs.primResumeKey, err = readNsonBinary(r)
		case VSCAN_SEC_KEY:
			vs.secResumeKey, err = readNsonBinary(r)
		case VSCAN_MOVE_AFTER:
			vs.moveAfterResumeKey, err = readNsonBoolean(r)
		case VSCAN_JOIN_DESC_RESUME_KEY:
			vs.descResumeKey, err = readNsonBinary(r)
		case VSCAN_JOIN_PATH_TABLES:
			vs.joinPathTables, err = readNsonIntArray(r)
		case VSCAN_JOIN_PATH_KEY:
			vs.joinPathKey, err = readNsonBinary(r)
		case VSCAN_JOIN_PATH_SEC_KEY:
			vs.joinPathSecKey, err = readNsonBinary(r)
		case VSCAN_JOIN_PATH_MATCHED:
			vs.joinPathMatched, err = readNsonBoolean(r)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, err
	}
	return vs, nil
}

func readNsonTopologyInfo(r proto.Reader) (*common.TopologyInfo, error) {
	ti := &common.TopologyInfo{SeqNum: -1, ShardIDs: nil}
	lw, _, err := newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case PROXY_TOPO_SEQNUM:
			ti.SeqNum, err = readNsonInt(r, name)
		case SHARD_IDS:
			ti.ShardIDs, err = readNsonIntArray(r)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, err
	}
	if ti.SeqNum < 0 || ti.ShardIDs == nil {
		return nil, errors.New("topology info missing seqNum or shardIDs")
	}
	return ti, nil
}

func readNsonWriteResult(r proto.Reader) (WriteResult, error) {
	res := &WriteResult{}
	lw, _, err := newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case EXISTING_MOD_TIME:
			res.ExistingModificationTime, err = readNsonLong(r)
		case EXISTING_VERSION:
			res.ExistingVersion, err = readNsonVersion(r)
		case EXISTING_VALUE:
			v, err := r.ReadFieldValue()
			if err == nil {
				if v, ok := v.(*types.MapValue); ok {
					res.ExistingValue = v
				} else {
					return *res, errors.New("returned field value is not a MapValue")
				}
			}
		//case EXISTING_EXPIRATION:
		// TODO
		default:
			err = skipNsonField(r, name)
		}
	}
	return *res, err
}

func readNsonRow(r proto.Reader, res *GetResult) error {
	lw, _, err := newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case MODIFIED:
			res.ModificationTime, err = readNsonLong(r)
		case EXPIRATION:
			var timeMs int64
			timeMs, err = readNsonLong(r)
			if err == nil {
				if timeMs <= 0 {
					// Zero value of time.Time means the row does not expire.
					res.ExpirationTime = time.Time{}
				} else {
					res.ExpirationTime = toUnixTime(timeMs)
				}
			}
		case ROW_VERSION:
			res.Version, err = readNsonVersion(r)
		case VALUE:
			res.Value, err = readNsonRowValue(r)
		default:
			err = skipNsonField(r, name)
		}
	}
	return err
}

func readNsonRowValue(r proto.Reader) (*types.MapValue, error) {
	v, err := r.ReadFieldValue()
	if err != nil {
		return nil, err
	}
	if v, ok := v.(*types.MapValue); ok {
		return v, nil
	}
	return nil, fmt.Errorf("row value is not of type MapValue")
}

func readNsonTimestampFromString(r proto.Reader) (time.Time, error) {
	var val time.Time
	str, err := readNsonString(r)
	if err != nil {
		return val, err
	}
	return types.ParseDateTime(str)
}

func readNsonOperationResult(r proto.Reader) (res *OperationResult, err error) {
	res = &OperationResult{}

	var lw *mapWalker
	lw, _, err = newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case SUCCESS:
			res.Success, err = readNsonBoolean(r)
		case ROW_VERSION:
			res.Version, err = readNsonVersion(r)
		case RETURN_INFO:
			res.WriteResult, err = readNsonWriteResult(r)
		case GENERATED:
			res.GeneratedValue, err = r.ReadFieldValue()
		default:
			err = skipNsonField(r, name)
		}
	}
	return res, err
}

func readNsonUsageRecord(r proto.Reader) (res *TableUsage, err error) {
	res = &TableUsage{}
	var secondsInPeriod int
	var lw *mapWalker
	lw, _, err = newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case START:
			res.StartTime, err = readNsonTimestampFromString(r)
		case TABLE_USAGE_PERIOD:
			secondsInPeriod, err = readNsonInt(r, name)
		case READ_UNITS:
			res.ReadUnits, err = readNsonInt(r, name)
		case WRITE_UNITS:
			res.WriteUnits, err = readNsonInt(r, name)
		case STORAGE_GB:
			res.StorageGB, err = readNsonInt(r, name)
		case READ_THROTTLE_COUNT:
			res.ReadThrottleCount, err = readNsonInt(r, name)
		case WRITE_THROTTLE_COUNT:
			res.WriteThrottleCount, err = readNsonInt(r, name)
		case STORAGE_THROTTLE_COUNT:
			res.StorageThrottleCount, err = readNsonInt(r, name)
		case MAX_SHARD_USAGE_PERCENT:
			res.MaxShardUsagePercent, err = readNsonInt(r, name)
		default:
			err = skipNsonField(r, name)
		}
	}
	if err == nil {
		dur := time.Duration(time.Duration(secondsInPeriod) * time.Second)
		res.EndTime = res.StartTime.Add(dur)
	}

	return res, err
}

func readNsonIndexInfo(r proto.Reader) (res *IndexInfo, err error) {
	res = &IndexInfo{}
	var lw *mapWalker
	lw, _, err = newMapWalker(r)
	for err == nil && lw.hasNext() {
		lw.next()
		switch name := lw.getCurrentName(); name {
		case NAME:
			res.IndexName, err = readNsonString(r)
		case FIELDS:
			// array of map with PATH, TYPE elements
			if err = readNsonType(r, types.Array); err != nil {
				return nil, err
			}
			// length in bytes: ignored
			if _, err = r.ReadInt(); err != nil {
				return nil, err
			}
			numElements, err := r.ReadInt()
			if err != nil {
				return nil, err
			}
			res.FieldNames = make([]string, 0, numElements)
			res.FieldTypes = make([]string, 0, numElements)
			for i := 0; i < numElements; i++ {
				var str string
				var infoWalker *mapWalker
				infoWalker, _, err = newMapWalker(r)
				for err == nil && infoWalker.hasNext() {
					infoWalker.next()
					switch name = infoWalker.getCurrentName(); name {
					case PATH:
						str, err = readNsonString(r)
						if err == nil {
							res.FieldNames = append(res.FieldNames, str)
						}
					case TYPE:
						str, err = readNsonString(r)
						if err == nil {
							res.FieldTypes = append(res.FieldTypes, str)
						}
					default:
						err = skipNsonField(r, name)
					}
				}
			}
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

type driverPlanInfo struct {
	driverQueryPlan planIter
	numIterators    int
	numRegisters    int
	externalVars    map[string]int
}

// readNsonPrepareOrQuery deserializes either a QueryResult or a PrepareResult.
// Either qreq/qres are given, or preq/pres are given.
func readNsonPrepareOrQuery(qreq *QueryRequest, qres *QueryResult,
	preq *PrepareRequest, pres *PrepareResult,
	r proto.Reader, _ int16, _ int16) (code int, err error) {

	isPreparedRequest := false
	if qreq != nil && qreq.PreparedStatement != nil {
		isPreparedRequest = true
	}

	var proxyPreparedQuery []byte
	var contKey []byte
	var queryPlan string
	var proxyTopoSeqNum int = -1
	var shardIDs []int
	var dpi *driverPlanInfo
	var capacity Capacity
	var topo *common.TopologyInfo
	var virtualScans []*virtualScan

	// TODO
	var tableName string
	var namespace string
	var operation byte
	var querySchema string

	var walker *mapWalker
	if walker, code, err = newMapWalker(r); err != nil || code != 0 {
		return
	}
	for err == nil && walker.hasNext() {
		walker.next()
		switch name := walker.getCurrentName(); name {
		case ERROR_CODE:
			if code, err = walker.handleErrorCode(); err != nil || code != 0 {
				return
			}
		case CONSUMED:
			capacity, err = readNsonConsumedCapacity(r)
		case TOPOLOGY_INFO:
			topo, err = readNsonTopologyInfo(r)
			if topo != nil {
				proxyTopoSeqNum = topo.SeqNum
				shardIDs = topo.ShardIDs
			}
		case QUERY_RESULTS:
			if qres != nil {
				err = readNsonQueryResults(r, qres)
			} else {
				err = skipNsonField(r, name)
			}
		case CONTINUATION_KEY:
			contKey, err = readNsonBinary(r)
		case SORT_PHASE1_RESULTS:
			if qres != nil {
				var arr []byte
				arr, err = readNsonBinary(r)
				if err == nil {
					err = readNsonPhase1Results(arr, qres)
				}
			} else {
				err = skipNsonField(r, name)
			}
		case PREPARED_QUERY:
			proxyPreparedQuery, err = readNsonBinary(r)
		case DRIVER_QUERY_PLAN:
			var arr []byte
			arr, err = readNsonBinary(r)
			if err == nil {
				dpi, err = readDriverPlanInfo(arr)
			}
		case REACHED_LIMIT:
			if qres != nil {
				qres.reachedLimit, err = readNsonBoolean(r)
			} else {
				err = skipNsonField(r, name)
			}
		case PROXY_TOPO_SEQNUM:
			proxyTopoSeqNum, err = readNsonInt(r, name)
		case SHARD_IDS:
			shardIDs, err = readNsonIntArray(r)
		// added in query V4
		case VSCANS:
			virtualScans, err = readNsonVirtualScans(r)
		case TABLE_NAME:
			tableName, err = readNsonString(r)
		case NAMESPACE:
			namespace, err = readNsonString(r)
		case QUERY_PLAN_STRING:
			queryPlan, err = readNsonString(r)
		case QUERY_RESULT_SCHEMA:
			querySchema, err = readNsonString(r)
		case QUERY_OPERATION:
			var val int
			val, err = readNsonInt(r, name)
			if err == nil {
				operation = byte(val)
			}
		default:
			err = skipNsonField(r, name)
		}
	}
	if err != nil {
		return BadProtocol, err
	}

	if qres != nil {
		qres.continuationKey = contKey
		qreq.setContKey(qres.continuationKey)
		qres.Capacity = capacity
		qres.virtualScans = virtualScans
	}

	if isPreparedRequest {
		//if qreq != nil && qreq.driver != nil {
		// TODO update topo info
		//}
		return 0, nil
	}

	var ti *common.TopologyInfo
	if proxyTopoSeqNum >= 0 {
		ti = &common.TopologyInfo{
			SeqNum:   proxyTopoSeqNum,
			ShardIDs: shardIDs,
		}
	}

	var sqlText string
	if qreq != nil {
		sqlText = qreq.Statement
	} else {
		sqlText = preq.Statement
	}

	prep := &PreparedStatement{
		sqlText:     sqlText,
		queryPlan:   queryPlan,
		querySchema: querySchema,
		statement:   proxyPreparedQuery,
		tableName:   tableName,
		namespace:   namespace,
		operation:   operation,
	}

	if dpi != nil {
		prep.driverQueryPlan = dpi.driverQueryPlan
		prep.numIterators = dpi.numIterators
		prep.numRegisters = dpi.numRegisters
		prep.variableToIDs = dpi.externalVars
	}

	// TODO: namespace, tableName, operation??

	if pres != nil {
		pres.PreparedStatement = *prep
		pres.Capacity = capacity
		if ti != nil {
			pres.SetTopology(ti)
		}
	} else if qreq != nil {
		qreq.PreparedStatement = prep
		if !prep.isSimpleQuery() {
			driver := newQueryDriver(qreq)
			driver.prepareCost = qres.Capacity.ReadKB
			qres.isComputed = false
		}
		if ti != nil {
			qres.SetTopology(ti)
		}
	}

	return 0, nil
}

func readDriverPlanInfo(arr []byte) (dpi *driverPlanInfo, err error) {
	if len(arr) == 0 {
		return nil, nil
	}

	// create a new io.Reader from buffer
	r := binary.NewReader(bytes.NewBuffer(arr))

	dpi = &driverPlanInfo{}

	dpi.driverQueryPlan, err = deserializePlanIter(r)
	if err != nil || dpi.driverQueryPlan == nil {
		return nil, err
	}

	dpi.numIterators, err = r.ReadInt()
	if err != nil {
		return nil, err
	}

	dpi.numRegisters, err = r.ReadInt()
	if err != nil {
		return nil, err
	}

	var numVars int
	numVars, err = r.ReadInt()
	if err != nil {
		return nil, err
	}

	if numVars > 0 {
		dpi.externalVars = make(map[string]int, numVars)
		var name *string
		var id int
		for i := 0; i < numVars; i++ {
			name, err = r.ReadString()
			if err != nil {
				return nil, err
			}

			id, err = r.ReadInt()
			if err != nil {
				return nil, err
			}

			if name != nil {
				dpi.externalVars[*name] = id
			}
		}
	}

	return dpi, nil
}

func readNsonIntArray(r proto.Reader) (arr []int, err error) {
	// array of integers
	if err = readNsonType(r, types.Array); err != nil {
		return nil, err
	}
	// length in bytes: ignored
	if _, err = r.ReadInt(); err != nil {
		return nil, err
	}
	numElements, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	arr = make([]int, 0, numElements)
	for i := 0; i < numElements; i++ {
		val, err := readNsonInt(r, "<array element>")
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
	}
	return
}

func readNsonVirtualScans(r proto.Reader) (scans []*virtualScan, err error) {
	// array of virtual scans
	if err = readNsonType(r, types.Array); err != nil {
		return nil, err
	}
	// length in bytes: ignored
	if _, err = r.ReadInt(); err != nil {
		return nil, err
	}
	numElements, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	scans = make([]*virtualScan, 0, numElements)
	for i := 0; i < numElements; i++ {
		val, err := readNsonVirtualScan(r)
		if err != nil {
			return nil, err
		}
		scans = append(scans, val)
	}
	return
}

func readNsonQueryResults(r proto.Reader, qres *QueryResult) (err error) {
	// array of integers
	if err = readNsonType(r, types.Array); err != nil {
		return
	}
	// length in bytes: ignored
	if _, err = r.ReadInt(); err != nil {
		return
	}
	var numElements int
	numElements, err = r.ReadInt()
	if err != nil {
		return
	}
	arr := make([]*types.MapValue, 0, numElements)
	var val *types.MapValue
	for i := 0; i < numElements; i++ {
		val, err = readNsonRowValue(r)
		if err != nil {
			return
		}
		arr = append(arr, val)
	}
	qres.results = arr
	return nil
}

func readNsonPhase1Results(arr []byte, res *QueryResult) (err error) {
	if len(arr) == 0 {
		return nil
	}
	// create a new io.Reader from buffer
	r := binary.NewReader(bytes.NewBuffer(arr))

	if res.isInPhase1, err = r.ReadBoolean(); err != nil {
		return
	}

	if res.partitionIDs, err = readPackedIntArray(r); err != nil {
		return
	}

	if res.partitionIDs != nil {
		if res.numResultsPerPart, err = readPackedIntArray(r); err != nil {
			return
		}
		n := len(res.partitionIDs)
		res.contKeysPerPart = make([][]byte, n)
		for i := 0; i < n; i++ {
			res.contKeysPerPart[i], err = r.ReadByteArray()
			if err != nil {
				return
			}
		}
	}

	return nil
}

// Bind variables:
// "variables": [
//
//	 { "name": "foo", "value": {...}}
//	.....
//
// ]
func (ns *NsonSerializer) writeBindVariables(bindVars map[string]interface{}) (err error) {

	n := len(bindVars)
	if n <= 0 {
		return nil
	}

	ns.startArray(BIND_VARIABLES)

	for k, v := range bindVars {
		ns.startArrayField(0)
		ns.startMap("")
		if err = ns.writeField(NAME, &k); err != nil {
			return
		}
		if err = ns.writeField(VALUE, v); err != nil {
			return
		}
		ns.endMap("")
		ns.endArrayField(0)
	}

	ns.endArray(BIND_VARIABLES)
	return nil
}

func nsonReadable(field string) string {
	val, ok := NsonFieldsMap[field]
	if !ok {
		return field
	}
	return val
}
