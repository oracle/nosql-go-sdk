//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

type serializer interface {
	serialize(w proto.Writer) error
	deserialize(r proto.Reader) (Result, error)
}

type Request interface {
	serializer
	validate() error
	setDefaults(cfg *RequestConfig)
	shouldRetry() bool
	timeout() time.Duration
}

// serialize writes the GetRequest to data stream using the specified protocol writer.
//
// The fields of GetRequest are written in the following order:
//
//   OpCode: Get
//   Timeout
//   TableName
//   Consistency
//   Key
//
func (req *GetRequest) serialize(w proto.Writer) (err error) {
	if err = serializeTableOp(w, proto.Get, req.Timeout, req.TableName); err != nil {
		return
	}

	if _, err = w.WriteConsistency(req.Consistency); err != nil {
		return
	}

	if _, err = w.WriteFieldValue(req.Key); err != nil {
		return
	}

	return
}

func (req *GetRequest) deserialize(r proto.Reader) (Result, error) {
	c, err := deserializeConsumedCapacity(r)
	if err != nil {
		return nil, err
	}

	hasRow, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	res := &GetResult{Capacity: *c}
	if !hasRow {
		return res, nil
	}

	v, err := r.ReadFieldValue()
	if err != nil {
		return nil, err
	}

	if v, ok := v.(*types.MapValue); ok {
		res.Value = v
	}

	timeMs, err := r.ReadPackedLong()
	if err != nil {
		return nil, err
	}

	if timeMs <= 0 {
		// Zero value of time.Time means the row does not expire.
		res.ExpirationTime = time.Time{}
	} else {
		res.ExpirationTime = toUnixTime(timeMs)
	}

	if res.Version, err = r.ReadVersion(); err != nil {
		return nil, err
	}

	return res, nil
}

// serialize writes the GetTableRequest to data stream using the specified protocol writer.
//
// The fields of GetTableRequest are written in the following order:
//
//   OpCode: GetTable
//   Timeout
//   TableName
//   OperationID
//
func (req *GetTableRequest) serialize(w proto.Writer) (err error) {
	if err = serializeTableOp(w, proto.GetTable, req.Timeout, req.TableName); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.OperationID); err != nil {
		return
	}

	return
}

func (req *GetTableRequest) deserialize(r proto.Reader) (Result, error) {
	return deserializeTableResult(r)
}

func (req *SystemRequest) serialize(w proto.Writer) (err error) {
	if err = serializeOp(w, proto.SystemRequest, req.Timeout); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.Statement); err != nil {
		return
	}

	return
}

func (req *SystemRequest) deserialize(r proto.Reader) (Result, error) {
	return deserializeSystemResult(r)
}

func (req *SystemStatusRequest) serialize(w proto.Writer) (err error) {
	if err = serializeOp(w, proto.SystemStatusRequest, req.Timeout); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.OperationID); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.Statement); err != nil {
		return
	}

	return
}

func (req *SystemStatusRequest) deserialize(r proto.Reader) (Result, error) {
	return deserializeSystemResult(r)
}

// serialize writes the TableRequest to data stream using the specified protocol writer.
//
// The fields of TableRequest are written in the following order:
//
//   OpCode: TableRequest
//   Timeout
//   Statement: if it is set.
//   A bool flag: indicates if table limits is set.
//   TableLimits: skip if it is not set.
//   A bool flag: indicates if table name is set.
//   TableName: skip if it is not set.
//
func (req *TableRequest) serialize(w proto.Writer) (err error) {
	if err = serializeOp(w, proto.TableRequest, req.Timeout); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.Statement); err != nil {
		return
	}

	if req.TableLimits == nil {
		_, err = w.WriteBoolean(false)
		return
	}

	// Write table limits if it is set.
	if _, err = w.WriteBoolean(true); err != nil {
		return
	}

	if _, err = w.WriteInt(int(req.TableLimits.ReadUnits)); err != nil {
		return
	}

	if _, err = w.WriteInt(int(req.TableLimits.WriteUnits)); err != nil {
		return
	}

	if _, err = w.WriteInt(int(req.TableLimits.StorageGB)); err != nil {
		return
	}

	// Table name is not set.
	if req.TableName == "" {
		_, err = w.WriteBoolean(false)
		return
	}

	// Write table name if it is set.
	if _, err = w.WriteBoolean(true); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.TableName); err != nil {
		return
	}

	return
}

func (req *TableRequest) deserialize(r proto.Reader) (Result, error) {
	return deserializeTableResult(r)
}

// serialize writes the ListTablesRequest to data stream using the specified protocol writer.
//
// The fields of ListTablesRequest are written in the following order:
//
//   OpCode: ListTables
//   Timeout
//   StartIndex
//   Limit
//   Namespace
//
func (req *ListTablesRequest) serialize(w proto.Writer) (err error) {
	if err = serializeOp(w, proto.ListTables, req.Timeout); err != nil {
		return
	}

	if _, err = w.WriteInt(int(req.StartIndex)); err != nil {
		return
	}

	if _, err = w.WriteInt(int(req.Limit)); err != nil {
		return
	}

	if err = writeNonEmptyString(w, req.Namespace); err != nil {
		return
	}

	return
}

func (req *ListTablesRequest) deserialize(r proto.Reader) (Result, error) {
	n, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	tableNames := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		if s != nil {
			tableNames = append(tableNames, *s)
		}
	}

	lastIdx, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	return &ListTablesResult{
		Tables:            tableNames,
		LastIndexReturned: uint(lastIdx),
	}, nil
}

// serialize writes the GetIndexesRequest to data stream using the specified protocol writer.
//
// The fields of GetIndexesRequest are written in the following order:
//
//   OpCode: GetIndexes
//   Timeout
//   TableName
//   A bool flag: indicates if index name is set.
//   IndexName: skip if index name is not set.
//
func (req *GetIndexesRequest) serialize(w proto.Writer) (err error) {
	if err = serializeTableOp(w, proto.GetIndexes, req.Timeout, req.TableName); err != nil {
		return
	}

	if req.IndexName == "" {
		_, err = w.WriteBoolean(false)
		return
	}

	// Write index name if it is set.
	if _, err = w.WriteBoolean(true); err != nil {
		return
	}

	if _, err = w.WriteString(&req.IndexName); err != nil {
		return
	}

	return
}

func (req *GetIndexesRequest) deserialize(r proto.Reader) (Result, error) {
	n, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	indexes := make([]IndexInfo, 0, n)
	for i := 0; i < n; i++ {
		idxInfo, err := deserializeIndexInfo(r)
		if err != nil {
			return nil, err
		}

		if idxInfo != nil {
			indexes = append(indexes, *idxInfo)
		}
	}

	return &GetIndexesResult{Indexes: indexes}, nil
}

// serialize writes the DeleteRequest to data stream using the specified protocol writer.
//
// The fields of DeleteRequest are written in the following order:
//
//   OpCode: Delete or DeleteIfVersion
//   Timeout: skip if the request is a a sub request.
//   TableName: skip if the request is a sub request.
//   ReturnRow
//   Key
//   MatchVersion: skip if it is nil
//
func (req *DeleteRequest) serialize(w proto.Writer) (err error) {
	op := proto.Delete
	hasVersion := req.MatchVersion != nil
	if hasVersion {
		op = proto.DeleteIfVersion
	}

	if req.isSubRequest {
		_, err = w.WriteOpCode(op)
	} else {
		err = serializeTableOp(w, op, req.Timeout, req.TableName)
	}

	if err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.ReturnRow); err != nil {
		return
	}

	if _, err = w.WriteFieldValue(req.Key); err != nil {
		return
	}

	// Write match version if it is not nil.
	if hasVersion {
		_, err = w.WriteVersion(req.MatchVersion)
	}

	return
}

func (req *DeleteRequest) deserialize(r proto.Reader) (Result, error) {
	c, err := deserializeConsumedCapacity(r)
	if err != nil {
		return nil, err
	}

	success, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	wrRes, err := deserializeWriteResult(r)
	if err != nil {
		return nil, err
	}

	return &DeleteResult{
		Capacity:    *c,
		WriteResult: *wrRes,
		Success:     success,
	}, nil
}

// serialize writes the PutRequest to data stream using the specified protocol writer.
//
// The fields of PutRequest are written in the following order:
//
//   OpCode: either Put, PutIfAbsent, PutIfPresent or PutIfVersion.
//   Timeout: skip if the request is a a sub request.
//   TableName: skip if the request is a sub request.
//   ReturnRow
//   ExactMatch
//   IdentityCacheSize
//   Value
//   UpdateTTL: this is true if UseTableTTL or TTL is set.
//   TTL
//   MatchVersion: skip if it is nil.
//
func (req *PutRequest) serialize(w proto.Writer) (err error) {
	var op proto.OpCode
	switch req.PutOption {
	case types.PutIfAbsent, types.PutIfPresent, types.PutIfVersion:
		op = proto.OpCode(req.PutOption)
	default:
		op = proto.Put
	}

	// If it is a sub request, do not write timeout and table name.
	if req.isSubRequest {
		_, err = w.WriteOpCode(op)
	} else {
		err = serializeTableOp(w, op, req.Timeout, req.TableName)
	}

	if err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.ReturnRow); err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.ExactMatch); err != nil {
		return
	}

	if _, err = w.WritePackedInt(req.IdentityCacheSize); err != nil {
		return
	}

	if _, err = w.WriteFieldValue(req.Value); err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.updateTTL()); err != nil {
		return
	}

	if _, err = w.WriteTTL(req.TTL); err != nil {
		return
	}

	// Write match version if it is not nil.
	if req.MatchVersion != nil {
		_, err = w.WriteVersion(req.MatchVersion)
	}

	return
}

func (req *PutRequest) deserialize(r proto.Reader) (Result, error) {
	c, err := deserializeConsumedCapacity(r)
	success, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	var version types.Version
	if success {
		version, err = r.ReadVersion()
		if err != nil {
			return nil, err
		}
	}

	wrRes, err := deserializeWriteResult(r)
	if err != nil {
		return nil, err
	}

	// Check if there is a generated identity column value.
	hasGeneratedValue, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	var generatedValue types.FieldValue
	if hasGeneratedValue {
		generatedValue, err = r.ReadFieldValue()
		if err != nil {
			return nil, err
		}
	}

	return &PutResult{
		Capacity:       *c,
		WriteResult:    *wrRes,
		Version:        version,
		GeneratedValue: generatedValue,
	}, nil
}

// serialize writes the TableUsageRequest to data stream using the specified protocol writer.
//
// The fields of TableUsageRequest are written in the following order:
//
//   OpCode: GetTableUsage
//   Timeout
//   TableName
//   StartTime
//   EndTime
//   Limit
//
func (req *TableUsageRequest) serialize(w proto.Writer) (err error) {
	if err = serializeTableOp(w, proto.GetTableUsage, req.Timeout, req.TableName); err != nil {
		return
	}

	if _, err = w.WritePackedLong(timeToMs(req.StartTime)); err != nil {
		return
	}

	if _, err = w.WritePackedLong(timeToMs(req.EndTime)); err != nil {
		return
	}

	if _, err = w.WritePackedInt(int(req.Limit)); err != nil {
		return
	}

	return
}

func (req *TableUsageRequest) deserialize(r proto.Reader) (Result, error) {
	res := &TableUsageResult{}
	// Read in the tenant id but discard it.
	_, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	tableName, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	if tableName != nil {
		res.TableName = *tableName
	}

	n, err := r.ReadPackedInt()
	res.UsageRecords = make([]TableUsage, 0, n)
	for i := 0; i < n; i++ {
		usageRecord, err := deserializeUsage(r)
		if err != nil {
			return nil, err
		}

		res.UsageRecords = append(res.UsageRecords, *usageRecord)
	}

	return res, nil
}

// serialize writes the MultiDeleteRequest to data stream using the specified protocol writer.
//
// The fields of MultiDeleteRequest are written in the following order:
//
//   OpCode: MultiDelete
//   Timeout
//   TableName
//   Key
//   FieldRange
//   MaxWriteKB
//   ContinuationKey
//
func (req *MultiDeleteRequest) serialize(w proto.Writer) (err error) {
	if err = serializeTableOp(w, proto.MultiDelete, req.Timeout, req.TableName); err != nil {
		return
	}

	if _, err = w.WriteFieldValue(req.Key); err != nil {
		return
	}

	if _, err = w.WriteFieldRange(req.FieldRange); err != nil {
		return
	}

	if _, err = w.WritePackedInt(int(req.MaxWriteKB)); err != nil {
		return
	}

	if _, err = w.WriteByteArray(req.ContinuationKey); err != nil {
		return
	}

	return
}

func (req *MultiDeleteRequest) deserialize(r proto.Reader) (Result, error) {
	c, err := deserializeConsumedCapacity(r)
	if err != nil {
		return nil, err
	}

	n, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	contKey, err := r.ReadByteArray()
	if err != nil {
		return nil, err
	}

	return &MultiDeleteResult{
		Capacity:        *c,
		NumDeleted:      n,
		ContinuationKey: contKey,
	}, nil
}

// serialize writes the WriteMultipleRequest to data stream using the specified protocol writer.
//
// The fields of WriteMultipleRequest are written in the following order:
//
//   OpCode: WriteMultiple
//   Timeout
//   TableName
//   Number of operations
//   All sub operations: either put or delete operation
//
func (req *WriteMultipleRequest) serialize(w proto.Writer) (err error) {
	if err = serializeTableOp(w, proto.WriteMultiple, req.Timeout, req.TableName); err != nil {
		return
	}

	numOps := len(req.Operations)
	if _, err = w.WritePackedInt(numOps); err != nil {
		return
	}

	var subReq Request
	for _, operation := range req.Operations {
		n1 := w.NumBytes()
		if _, err = w.WriteBoolean(operation.AbortOnFail); err != nil {
			return
		}

		if operation.DeleteRequest != nil {
			subReq = operation.DeleteRequest
			err = operation.DeleteRequest.serialize(w)
		} else if operation.PutRequest != nil {
			subReq = operation.PutRequest
			err = operation.PutRequest.serialize(w)
		}

		if err != nil {
			return
		}

		n := w.NumBytes() - n1
		// Check size limit for each sub request.
		if err = checkRequestSizeLimit(subReq, n); err != nil {
			return
		}
	}

	return
}

func (req *WriteMultipleRequest) deserialize(r proto.Reader) (Result, error) {
	succeed, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	c, err := deserializeConsumedCapacity(r)
	if err != nil {
		return nil, err
	}

	res := &WriteMultipleResult{Capacity: *c}

	// WriteMultiple operation succeeded.
	if succeed {
		res.FailedOperationIndex = -1
		n, err := r.ReadPackedInt()
		if err != nil {
			return nil, err
		}

		res.ResultSet = make([]OperationResult, 0, n)
		for i := 0; i < n; i++ {
			opRes, err := deserializeOperationResult(r)
			if err != nil {
				return nil, err
			}

			res.ResultSet = append(res.ResultSet, *opRes)
		}

		return res, nil
	}

	// WriteMultiple operation failed.
	idx, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	res.FailedOperationIndex = int(idx)
	opRes, err := deserializeOperationResult(r)
	res.ResultSet = make([]OperationResult, 1)
	res.ResultSet[0] = *opRes
	return res, nil
}

// serialize writes the PrepareRequest to data stream using the specified protocol writer.
//
// The fields of PrepareRequest are written in the following order:
//
//   OpCode: Prepare
//   Timeout
//   Statement
//   QueryVersion
//   GetQueryPlan
//
func (req *PrepareRequest) serialize(w proto.Writer) (err error) {
	if err = serializeOp(w, proto.Prepare, req.Timeout); err != nil {
		return
	}

	if _, err = w.WriteString(&req.Statement); err != nil {
		return
	}

	// Write query version.
	if _, err = w.WriteInt16(proto.QueryVersion); err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.GetQueryPlan); err != nil {
		return
	}

	return
}

func (req *PrepareRequest) deserialize(r proto.Reader) (res Result, err error) {
	c, err := deserializeConsumedCapacity(r)
	if err != nil {
		return
	}

	prepStmt, err := deserializePrepStmt(r, req.Statement, req.GetQueryPlan)
	if err != nil {
		return
	}

	res = &PrepareResult{
		Capacity:          *c,
		PreparedStatement: *prepStmt,
	}

	return
}

func deserializePrepStmt(r proto.Reader, sqlText string, getQueryPlan bool) (prepStmt *PreparedStatement, err error) {
	stmt, err := r.ReadByteArrayWithInt()
	if err != nil {
		return
	}

	var p *string
	var queryPlan string
	var numIterators, numRegisters int
	var extVariables map[string]int
	var topoInfo *topologyInfo

	if getQueryPlan {
		p, err = r.ReadString()
		if err != nil {
			return
		}

		if p != nil {
			queryPlan = *p
		}
	}

	driverPlanIter, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	if driverPlanIter != nil {
		numIterators, err = r.ReadInt()
		if err != nil {
			return
		}

		numRegisters, err = r.ReadInt()
		if err != nil {
			return
		}

		var numVars int
		numVars, err = r.ReadInt()
		if err != nil {
			return
		}

		if numVars > 0 {
			extVariables = make(map[string]int, numVars)
			var name *string
			var id int
			for i := 0; i < numVars; i++ {
				name, err = r.ReadString()
				if err != nil {
					return
				}

				id, err = r.ReadInt()
				if err != nil {
					return
				}

				if name != nil {
					extVariables[*name] = id
				}
			}
		}

		topoInfo, err = deserializeTopologyInfo(r)
		if err != nil {
			return
		}
	}

	return newPreparedStatement(sqlText, queryPlan, topoInfo, stmt, driverPlanIter,
		numIterators, numRegisters, extVariables)

}

func deserializeTopologyInfo(r proto.Reader) (topoInfo *topologyInfo, err error) {
	seqNum, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	if seqNum < -1 {
		return nil, nosqlerr.NewIllegalArgument("invalid topology sequence number: %d", seqNum)
	}

	// A sequence number of -1 indicates the server does not send topology information.
	if seqNum == -1 {
		return nil, nil
	}

	shardIDs, err := readPackedIntArray(r)
	if err != nil {
		return
	}

	return &topologyInfo{
		seqNum:   seqNum,
		shardIDs: shardIDs,
	}, nil
}

func readPackedIntArray(r proto.Reader) ([]int, error) {
	n, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	if n < -1 {
		return nil, nosqlerr.NewIllegalArgument("invalid length of int array: %d", n)
	}

	if n == -1 {
		return nil, nil
	}

	array := make([]int, n)
	for i := 0; i < n; i++ {
		array[i], err = r.ReadPackedInt()
		if err != nil {
			return nil, err
		}
	}

	return array, nil
}

func (req *QueryRequest) serialize(w proto.Writer) (err error) {
	if err = serializeOp(w, proto.Query, req.Timeout); err != nil {
		return
	}

	if _, err = w.WriteConsistency(req.Consistency); err != nil {
		return
	}

	if _, err = w.WritePackedInt(int(req.Limit)); err != nil {
		return
	}

	if _, err = w.WritePackedInt(int(req.MaxReadKB)); err != nil {
		return
	}

	if _, err = w.WriteByteArray(req.continuationKey); err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.isPrepared()); err != nil {
		return
	}

	if _, err = w.WriteInt16(proto.QueryVersion); err != nil {
		return
	}

	if err = w.WriteByte(byte(req.traceLevel)); err != nil {
		return
	}

	if _, err = w.WritePackedInt(int(req.MaxWriteKB)); err != nil {
		return
	}

	if _, err = serializeMathContext(w, req.getFPArithSpec()); err != nil {
		return
	}

	if _, err = w.WritePackedInt(req.topologySeqNum()); err != nil {
		return
	}

	if _, err = w.WritePackedInt(req.getShardID()); err != nil {
		return
	}

	if _, err = w.WriteBoolean(req.isPrepared() && req.isSimpleQuery()); err != nil {
		return
	}

	if !req.isPrepared() {
		_, err = w.WriteString(&req.Statement)
		return
	}

	if req.PreparedStatement == nil {
		return
	}

	pstmt := req.PreparedStatement
	// write prepared statement
	if _, err = w.WriteByteArrayWithInt(pstmt.statement); err != nil {
		return
	}

	n := len(pstmt.bindVariables)
	if n <= 0 {
		// bind variables is nil
		_, err = w.WritePackedInt(0)
		return
	}

	if _, err = w.WritePackedInt(n); err != nil {
		return
	}

	for k, v := range pstmt.bindVariables {
		if _, err = w.WriteString(&k); err != nil {
			return
		}

		if _, err = w.WriteFieldValue(v); err != nil {
			return
		}
	}

	return
}

func serializeMathContext(w proto.Writer, mathCtx *FPArithSpec) (n int, err error) {
	if mathCtx == nil {
		return w.Write([]byte{0})
	}

	switch *mathCtx {
	case Decimal32:
		return w.Write([]byte{1})
	case Decimal64:
		return w.Write([]byte{2})
	case Decimal128:
		return w.Write([]byte{3})
	case Unlimited:
		return w.Write([]byte{4})
	default:
		n, err = w.Write([]byte{5})
		if err != nil {
			return
		}

		var num int
		num, err = w.WriteInt(int(mathCtx.Precision))
		n += num
		if err != nil {
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

		num, err = w.WriteInt(mode)
		n += num
		return
	}
}

func (req *QueryRequest) deserialize(r proto.Reader) (Result, error) {
	n, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	isSortPhase1Result, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	res := newQueryResult(req, true)

	res.results = make([]*types.MapValue, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadFieldValue()
		if err != nil {
			return nil, err
		}

		if v, ok := v.(*types.MapValue); ok {
			res.results = append(res.results, v)
		}
	}

	if isSortPhase1Result {
		res.isInPhase1, err = r.ReadBoolean()
		if err != nil {
			return nil, err
		}

		res.partitionIDs, err = readPackedIntArray(r)
		if err != nil {
			return nil, err
		}

		if res.partitionIDs != nil {
			res.numResultsPerPart, err = readPackedIntArray(r)
			if err != nil {
				return nil, err
			}

			n = len(res.partitionIDs)
			res.contKeysPerPart = make([][]byte, n)
			for i := 0; i < n; i++ {
				res.contKeysPerPart[i], err = r.ReadByteArray()
				if err != nil {
					return nil, err
				}
			}
		}
	}

	c, err := deserializeConsumedCapacity(r)
	if err != nil {
		return nil, err
	}
	res.Capacity = *c

	res.continuationKey, err = r.ReadByteArray()
	if err != nil {
		return nil, err
	}
	req.setContKey(res.continuationKey)

	prepStmt := req.PreparedStatement
	isPrepared := prepStmt != nil

	if !isPrepared {
		prepStmt, err = deserializePrepStmt(r, req.Statement, false)
		if err != nil {
			return nil, err
		}

		req.PreparedStatement = prepStmt
	}

	if prepStmt != nil && !prepStmt.isSimpleQuery() {
		if !isPrepared {
			driver := newQueryDriver(req)
			driver.topologyInfo = prepStmt.topologyInfo
			c, err := res.ConsumedCapacity()
			if err != nil {
				return nil, err
			}
			driver.prepareCost = c.ReadKB
			res.isComputed = false

		} else {
			res.reachedLimit, err = r.ReadBoolean()
			if err != nil {
				return nil, err
			}

			topoInfo, err := deserializeTopologyInfo(r)
			if err != nil {
				return nil, err
			}

			if topoInfo != nil {
				prepStmt.topologyInfo = topoInfo
				req.driver.topologyInfo = topoInfo
			}
		}
	}

	return res, nil
}

func checkRequestSizeLimit(req Request, size int) error {
	limit := proto.RequestSizeLimit
	if _, ok := req.(*WriteMultipleRequest); ok {
		limit = proto.BatchRequestSizeLimit
	}

	if size > limit {
		return fmt.Errorf("the request size %d exceeds the limit %d", size, limit)
	}

	return nil
}

func serializeOp(w proto.Writer, op proto.OpCode, timeout time.Duration) (err error) {
	if _, err = w.WriteOpCode(op); err != nil {
		return
	}

	if _, err = w.WriteTimeout(timeout); err != nil {
		return
	}

	return
}

func serializeTableOp(w proto.Writer, op proto.OpCode, timeout time.Duration, tableName string) (err error) {
	if err = serializeOp(w, op, timeout); err != nil {
		return
	}

	if err = writeNonEmptyString(w, tableName); err != nil {
		return
	}

	return
}

func writeNonEmptyString(w proto.Writer, s string) (err error) {
	if s == "" {
		_, err = w.WriteString(nil)
	} else {
		_, err = w.WriteString(&s)
	}
	return
}

// timeToMs converts the specified time to the number of milliseconds since
// Unix Epoch.
func timeToMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	// Converts to milliseconds
	return t.UnixNano() / 1e6
}

func toUnixTime(timeMs int64) time.Time {
	return time.Unix(0, timeMs*int64(time.Millisecond))
}

func toDuration(seconds int64) time.Duration {
	return time.Duration(seconds * int64(time.Second))
}

// deserialize consumed capacity of the request
func deserializeConsumedCapacity(r proto.Reader) (c *Capacity, err error) {
	var ru, rkb, wkb int
	if ru, err = r.ReadPackedInt(); err != nil {
		return
	}

	if rkb, err = r.ReadPackedInt(); err != nil {
		return
	}

	if wkb, err = r.ReadPackedInt(); err != nil {
		return
	}

	c = &Capacity{
		ReadKB:    rkb,
		WriteKB:   wkb,
		ReadUnits: ru,
	}
	return c, nil
}

func deserializeWriteResult(r proto.Reader) (*WriteResult, error) {
	res := &WriteResult{}
	returnInfo, err := r.ReadBoolean()
	if !returnInfo || err != nil {
		return res, err
	}

	v, err := r.ReadFieldValue()
	if err != nil {
		return res, err
	}

	if v, ok := v.(*types.MapValue); ok {
		res.ExistingValue = v
	} else {
		return res, errors.New("returned field value is not a MapValue")
	}

	version, err := r.ReadVersion()
	if err != nil {
		return res, err
	}

	res.ExistingVersion = version
	return res, nil
}

func toOperationState(st byte) types.OperationState {
	return types.OperationState(st + 1)
}

func deserializeSystemResult(r proto.Reader) (*SystemResult, error) {
	res := &SystemResult{}
	// Operation state.
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	res.State = toOperationState(b)

	var p *string
	// Operation id.
	p, err = r.ReadString()
	if err != nil {
		return nil, err
	}
	if p != nil {
		res.OperationID = *p
	}

	// Statement.
	p, err = r.ReadString()
	if err != nil {
		return nil, err
	}
	if p != nil {
		res.Statement = *p
	}

	// Result string.
	p, err = r.ReadString()
	if err != nil {
		return nil, err
	}
	if p != nil {
		res.ResultString = *p
	}

	return res, nil
}

func deserializeTableResult(r proto.Reader) (*TableResult, error) {
	hasInfo, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	res := &TableResult{}
	if !hasInfo {
		return res, nil
	}

	// read in the tenant id but discard it
	if _, err := r.ReadString(); err != nil {
		return res, err
	}

	var p *string
	// TableName.
	p, err = r.ReadString()
	if err != nil {
		return res, err
	}
	if p != nil {
		res.TableName = *p
	}

	// TableState
	b, err := r.ReadByte()
	if err != nil {
		return res, err
	}
	res.State = types.TableState(b)

	hasStaticState, err := r.ReadBoolean()
	if err != nil {
		return res, err
	}

	if hasStaticState {
		readKB, err := r.ReadPackedInt()
		if err != nil {
			return res, err
		}

		writeKB, err := r.ReadPackedInt()
		if err != nil {
			return res, err
		}

		storageGB, err := r.ReadPackedInt()
		if err != nil {
			return res, err
		}

		res.Limits = TableLimits{
			ReadUnits:  uint(readKB),
			WriteUnits: uint(writeKB),
			StorageGB:  uint(storageGB),
		}

		p, err = r.ReadString()
		if err != nil {
			return res, err
		}
		if p != nil {
			res.Schema = *p
		}
	}

	p, err = r.ReadString()
	if err != nil {
		return res, err
	}
	if p != nil {
		res.OperationID = *p
	}

	return res, nil

}

func deserializeIndexInfo(r proto.Reader) (*IndexInfo, error) {
	indexName, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	if indexName == nil {
		return nil, errors.New("nil index name")
	}

	n, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	fieldNames := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		if s != nil {
			fieldNames = append(fieldNames, *s)
		}
	}

	return &IndexInfo{
		IndexName:  *indexName,
		FieldNames: fieldNames,
	}, nil
}

func deserializeUsage(r proto.Reader) (usage *TableUsage, err error) {
	startTimeMillis, err := r.ReadPackedLong()
	if err != nil {
		return
	}

	startTime := toUnixTime(startTimeMillis)
	secondsInPeriod, err := r.ReadPackedInt()
	if err != nil {
		return
	}
	endTime := startTime.Add(toDuration(int64(secondsInPeriod)))

	ru, err := r.ReadPackedInt()
	if err != nil {
		return
	}

	wu, err := r.ReadPackedInt()
	if err != nil {
		return
	}

	storage, err := r.ReadPackedInt()
	if err != nil {
		return
	}

	rtc, err := r.ReadPackedInt()
	if err != nil {
		return
	}

	wtc, err := r.ReadPackedInt()
	if err != nil {
		return
	}

	stc, err := r.ReadPackedInt()
	if err != nil {
		return
	}

	usage = &TableUsage{
		StartTime:            startTime,
		EndTime:              endTime,
		ReadUnits:            ru,
		WriteUnits:           wu,
		StorageGB:            storage,
		ReadThrottleCount:    rtc,
		WriteThrottleCount:   wtc,
		StorageThrottleCount: stc,
	}
	return
}

func deserializeOperationResult(r proto.Reader) (res *OperationResult, err error) {
	success, err := r.ReadBoolean()
	if err != nil {
		return
	}

	hasVersion, err := r.ReadBoolean()
	if err != nil {
		return
	}

	var version types.Version
	if hasVersion {
		version, err = r.ReadVersion()
		if err != nil {
			return
		}
	}

	wrRes, err := deserializeWriteResult(r)
	if err != nil {
		return
	}

	// Check if there is a generated value for identity column.
	hasGeneratedValue, err := r.ReadBoolean()
	if err != nil {
		return nil, err
	}

	var generatedValue types.FieldValue
	if hasGeneratedValue {
		generatedValue, err = r.ReadFieldValue()
		if err != nil {
			return nil, err
		}
	}

	res = &OperationResult{
		WriteResult:    *wrRes,
		Success:        success,
		Version:        version,
		GeneratedValue: generatedValue,
	}

	return res, nil
}
