//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// +build cloud onprem

package nosqldb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// WriteMultiple tests the WriteMultiple API.
type WriteMultipleTestSuite struct {
	*test.NoSQLTestSuite
	table    string
	tableTTL *types.TimeToLive
}

func (suite *WriteMultipleTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	suite.table = suite.GetTableName("WriteMultipleTable")
	// Create a test table.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sid INTEGER, id INTEGER, name STRING, longString STRING, "+
		"PRIMARY KEY(SHARD(sid), id)) "+
		"USING TTL 1 DAYS", suite.table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  5000,
		WriteUnits: 5000,
		StorageGB:  50,
	}
	suite.ReCreateTable(suite.table, stmt, limits)
	suite.tableTTL = &types.TimeToLive{
		Value: 1,
		Unit:  types.Days,
	}
	// a single child table, if supported by server
	// first supported in 21.2
	if suite.Config.Version >= "21.2" {
		childTable := suite.table + ".child"
		stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(" +
			"childid INTEGER, childname STRING, childdata STRING, " +
			"PRIMARY KEY(childid)) " +
			"USING TTL 1 DAYS", childTable)
		suite.ReCreateTable(childTable, stmt, nil)
	}
}

func (suite *WriteMultipleTestSuite) TestOpSucceed() {
	sid := 10
	recordKB := 2
	n := 10

	var err error
	var wmRes *nosqldb.WriteMultipleResult
	var wmReq *nosqldb.WriteMultipleRequest
	var putReq *nosqldb.PutRequest
	var delReq *nosqldb.DeleteRequest
	var key, value *types.MapValue
	var shouldSucceed, rowPresent []bool
	var versionId2, versionId7 types.Version

	shouldSucceed = make([]bool, 0, n)
	rowPresent = make([]bool, 0, n)
	// Put 10 rows.
	wmReq = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < n; i++ {
		value = suite.genRow(sid, i, recordKB, false)
		putReq = &nosqldb.PutRequest{
			TableName: suite.table,
			Value:     value,
		}
		wmReq.AddPutRequest(putReq, true)
		shouldSucceed = append(shouldSucceed, true)
		rowPresent = append(rowPresent, false)
	}

	wmRes, err = suite.Client.WriteMultiple(wmReq)
	if suite.NoErrorf(err, "WriteMultiple() failed, got error: %v", err) {
		suite.verifyResult(wmRes, wmReq, shouldSucceed, rowPresent, recordKB)
		versionId2 = wmRes.ResultSet[2].Version
		versionId7 = wmRes.ResultSet[7].Version
	}

	shouldSucceed = make([]bool, 0, n)
	rowPresent = make([]bool, 0, n)
	wmReq = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	// PutIfAbsent, ReturnRow = true
	value = suite.genRow(sid, 0, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)
	shouldSucceed = append(shouldSucceed, false)
	rowPresent = append(rowPresent, true)

	// PutIfPresent, ReturnRow = true
	value = suite.genRow(sid, 1, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfPresent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// PutIfVersion, ReturnRow = true
	value = suite.genRow(sid, 2, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName:    suite.table,
		Value:        value,
		PutOption:    types.PutIfVersion,
		ReturnRow:    true,
		MatchVersion: versionId2,
	}
	wmReq.AddPutRequest(putReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// PutIfAbsent, ReturnRow = false
	value = suite.genRow(sid, 10, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: false,
	}
	wmReq.AddPutRequest(putReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, false)

	// Put, ReturnRow = true
	value = suite.genRow(sid, 3, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// Put, ReturnRow = false
	value = suite.genRow(sid, 4, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		ReturnRow: false,
	}
	wmReq.AddPutRequest(putReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// Delete, ReturnRow = true
	key = suite.genKey(sid, 5)
	delReq = &nosqldb.DeleteRequest{
		TableName: suite.table,
		Key:       key,
		ReturnRow: true,
	}
	wmReq.AddDeleteRequest(delReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// Delete, ReturnRow = false
	key = suite.genKey(sid, 6)
	delReq = &nosqldb.DeleteRequest{
		TableName: suite.table,
		Key:       key,
		ReturnRow: false,
	}
	wmReq.AddDeleteRequest(delReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// DeleteIfVersion, ReturnRow = true
	key = suite.genKey(sid, 7)
	delReq = &nosqldb.DeleteRequest{
		TableName:    suite.table,
		Key:          key,
		ReturnRow:    true,
		MatchVersion: versionId7,
	}
	wmReq.AddDeleteRequest(delReq, false)
	shouldSucceed = append(shouldSucceed, true)
	rowPresent = append(rowPresent, true)

	// DeleteIfVersion, ReturnRow = true
	key = suite.genKey(sid, 8)
	delReq = &nosqldb.DeleteRequest{
		TableName:    suite.table,
		Key:          key,
		ReturnRow:    true,
		MatchVersion: versionId7,
	}
	wmReq.AddDeleteRequest(delReq, false)
	shouldSucceed = append(shouldSucceed, false)
	rowPresent = append(rowPresent, true)

	// Delete, ReturnRow = true
	key = suite.genKey(sid, 100)
	delReq = &nosqldb.DeleteRequest{
		TableName: suite.table,
		Key:       key,
		ReturnRow: true,
	}
	wmReq.AddDeleteRequest(delReq, false)
	shouldSucceed = append(shouldSucceed, false)
	rowPresent = append(rowPresent, false)

	wmRes, err = suite.Client.WriteMultiple(wmReq)
	if suite.NoErrorf(err, "WriteMultiple() failed, got error: %v", err) {
		suite.verifyResult(wmRes, wmReq, shouldSucceed, rowPresent, recordKB)
	}

	// test writemultiple with parent/child tables
	// first supported in 21.2.52
	if suite.Config.Version < "21.2" {
		return
	}

	shouldSucceed = make([]bool, 0, n)
	rowPresent = make([]bool, 0, n)
	wmReq = &nosqldb.WriteMultipleRequest{}
	sid = 20
	childTable := suite.table + ".child"
	for i := 0; i < n; i++ {
		// parent
		value = suite.genRow(sid, i, recordKB, false)
		putReq = &nosqldb.PutRequest{
			TableName: suite.table,
			Value:     value,
		}
		wmReq.AddPutRequest(putReq, false)
		shouldSucceed = append(shouldSucceed, true)
		rowPresent = append(rowPresent, false)

		// child
		value = suite.genChildRow(sid, i, i, recordKB)
		putReq = &nosqldb.PutRequest{
			TableName: childTable,
			Value:     value,
		}
		wmReq.AddPutRequest(putReq, false)
		shouldSucceed = append(shouldSucceed, true)
		rowPresent = append(rowPresent, false)
	}

	wmRes, err = suite.Client.WriteMultiple(wmReq)
	if err != nil {
		// expected in the following KV releases:
		// 21.2 <= .51
		// 22.1 <= .22
		// 22.2 <= .13
		// 22.3 <= .3
	    if suite.Config.Version <= "21.2.51" {
			return
		}
	    if suite.Config.Version >= "22.1" && suite.Config.Version <= "22.1.22" {
			return
		}
	    if suite.Config.Version >= "22.2" && suite.Config.Version <= "22.2.13" {
			return
		}
	    if suite.Config.Version >= "22.3" && suite.Config.Version <= "22.3.3" {
			return
		}
	}
	if suite.NoErrorf(err, "WriteMultiple() failed, got error: %v", err) {
		suite.verifyResult(wmRes, wmReq, shouldSucceed, rowPresent, recordKB)
	}
}

func (suite *WriteMultipleTestSuite) TestOpAborted() {
	sid := 20
	recordKB := 2

	var err error
	var requests []nosqldb.Request
	var putReq *nosqldb.PutRequest
	var putRes *nosqldb.PutResult
	var delReq *nosqldb.DeleteRequest
	var oldValue, newValue *types.MapValue
	var oldVersion, newVersion types.Version

	oldValue = suite.genRow(sid, 101, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     oldValue,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoErrorf(err, "put(req=%v) failed, got error: %v", putReq) {
		suite.NotNilf(putRes.Version, "put(req=%v) should have returned a non-nil version", putReq)
		oldVersion = putRes.Version
	}

	newValue = suite.genRow(sid, 101, recordKB, true)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     newValue,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoErrorf(err, "put(req=%v) failed, got error: %v", putReq) {
		suite.NotNilf(putRes.Version, "put(req=%v) should have returned a non-nil version", putReq)
		newVersion = putRes.Version
	}

	failedOpIdx := 1
	requests = make([]nosqldb.Request, 0, 3)
	// Add 3 put operations
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     suite.genRow(sid, 100, recordKB, false),
		PutOption: types.PutIfAbsent,
	}
	requests = append(requests, putReq)

	// The 2nd put operation is expected to fail.
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     suite.genRow(sid, 101, recordKB, false),
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	requests = append(requests, putReq)

	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     suite.genRow(sid, 200, recordKB, false),
		PutOption: types.PutIfAbsent,
	}
	requests = append(requests, putReq)

	rowPresents := []bool{false, true, false}
	suite.runOpAbortedTest(requests, failedOpIdx, recordKB, rowPresents, newVersion, newValue)

	// Add PutIfPresent as 2nd operation, expect to fail.
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     suite.genRow(sid, 102, recordKB, false),
		PutOption: types.PutIfPresent,
		ReturnRow: true,
	}
	requests[failedOpIdx] = putReq
	rowPresents[failedOpIdx] = false
	suite.runOpAbortedTest(requests, failedOpIdx, recordKB, rowPresents, nil, nil)

	// Add PutIfVersion as 2nd operation, expect to fail.
	putReq = &nosqldb.PutRequest{
		TableName:    suite.table,
		Value:        suite.genRow(sid, 101, recordKB, true),
		PutOption:    types.PutIfVersion,
		MatchVersion: oldVersion,
		ReturnRow:    true,
	}
	requests[failedOpIdx] = putReq
	rowPresents[failedOpIdx] = true
	suite.runOpAbortedTest(requests, failedOpIdx, recordKB, rowPresents, newVersion, newValue)

	// Add Delete as 2nd operation, expect to fail.
	delReq = &nosqldb.DeleteRequest{
		TableName: suite.table,
		Key:       suite.genKey(sid, 102),
		ReturnRow: true,
	}
	requests[failedOpIdx] = delReq
	rowPresents[failedOpIdx] = false
	suite.runOpAbortedTest(requests, failedOpIdx, recordKB, rowPresents, nil, nil)

	// Add DeleteIfVersion as 2nd operation, expect to fail.
	delReq = &nosqldb.DeleteRequest{
		TableName:    suite.table,
		Key:          suite.genKey(sid, 101),
		ReturnRow:    true,
		MatchVersion: oldVersion,
	}
	requests[failedOpIdx] = delReq
	rowPresents[failedOpIdx] = true
	suite.runOpAbortedTest(requests, failedOpIdx, recordKB, rowPresents, newVersion, newValue)
}

func (suite *WriteMultipleTestSuite) TestOpInvalid() {
	sid := 30
	recordKB := 2

	var err error
	var expErrCode nosqlerr.ErrorCode
	var wmReq *nosqldb.WriteMultipleRequest
	var putReq *nosqldb.PutRequest
	var delReq *nosqldb.DeleteRequest
	var key, value *types.MapValue
	var n int

	wmReq = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	// Testcase 1: Two operations have different shard key
	n++
	value = suite.genRow(sid, 0, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	value = suite.genRow(sid+1, 0, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	expErrCode = nosqlerr.IllegalArgument
	_, err = suite.Client.WriteMultiple(wmReq)
	suite.Truef(nosqlerr.Is(err, expErrCode),
		"Testcase %d: WriteMultiple() should have failed with error: %v, got error: %v",
		n, expErrCode, err)

	// Testcase 2: More than one operation has the same Key
	n++
	wmReq.Clear()
	wmReq.TableName = suite.table

	value = suite.genRow(sid, 0, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	key = suite.genKey(sid, 0)
	delReq = &nosqldb.DeleteRequest{
		TableName: suite.table,
		Key:       key,
	}
	wmReq.AddDeleteRequest(delReq, false)

	expErrCode = nosqlerr.IllegalArgument
	_, err = suite.Client.WriteMultiple(wmReq)
	suite.Truef(nosqlerr.Is(err, expErrCode),
		"Testcase %d: WriteMultiple() should have failed with error: %v, got error: %v",
		n, expErrCode, err)

	// Testcase 3: the target table of a operation is different from that of others.
	n++
	wmReq.Clear()
	wmReq.TableName = suite.table

	value = suite.genRow(sid, 0, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	value = suite.genRow(sid, 1, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: "anotherTableName",
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	expErrCode = nosqlerr.IllegalArgument
	_, err = suite.Client.WriteMultiple(wmReq)
	suite.Truef(nosqlerr.Is(err, expErrCode),
		"Testcase %d: WriteMultiple() should have failed with error: %v, got error: %v",
		n, expErrCode, err)

	// Testcase 4: the number of operations exceeds the limit
	// This should fail for cloud service, but succeed for on-premise
	n++
	wmReq.Clear()
	wmReq.TableName = suite.table

	for i := 0; i <= test.MaxBatchOpNumberLimit; i++ {
		value = suite.genRow(sid, i, recordKB, false)
		putReq = &nosqldb.PutRequest{
			TableName: suite.table,
			Value:     value,
			PutOption: types.PutIfAbsent,
			ReturnRow: true,
		}
		wmReq.AddPutRequest(putReq, false)
	}

	_, err = suite.Client.WriteMultiple(wmReq)
	if test.IsCloud() {
		expErrCode = nosqlerr.BatchOpNumberLimitExceeded
		suite.Truef(nosqlerr.Is(err, expErrCode),
			"Testcase %d: WriteMultiple() should have failed with error: %v, got error: %v",
			n, expErrCode, err)

	} else {
		suite.NoErrorf(err, "Testcase %d: WriteMultiple() should have succeeded, got error: %v",
			n, err)
	}

	// Testcase 5: the data size of 2nd operation exceeds the limit.
	// This should fail for cloud service, but succeed for on-premise
	n++
	wmReq.Clear()
	wmReq.TableName = suite.table

	value = suite.genRow(sid, 100, recordKB, false)
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	value = &types.MapValue{}
	value.Put("sid", sid).Put("id", 101)
	value.Put("name", test.GenString(test.MaxDataSizeLimit))
	putReq = &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     value,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}
	wmReq.AddPutRequest(putReq, false)

	_, err = suite.Client.WriteMultiple(wmReq)
	if test.IsCloud() {
		expErrCode = nosqlerr.RowSizeLimitExceeded
		suite.Truef(nosqlerr.Is(err, expErrCode),
			"Testcase %d: WriteMultiple() should have failed with error: %v, got error: %v",
			n, expErrCode, err)

	} else {
		suite.NoErrorf(err, "Testcase %d: WriteMultiple() should have succeeded, got error: %v",
			n, err)
	}
}

func (suite *WriteMultipleTestSuite) TestOpWithTTL() {
	sid := 11
	n := 10
	recordKB := 2

	var err error
	var wmRes *nosqldb.WriteMultipleResult
	var wmReq *nosqldb.WriteMultipleRequest
	var putReq *nosqldb.PutRequest
	var getReq *nosqldb.GetRequest
	var getRes *nosqldb.GetResult
	var key, value *types.MapValue
	var shouldSucceed []bool
	var ttl *types.TimeToLive
	var oldExpireTime time.Time

	shouldSucceed = make([]bool, 0, n)

	// Put rows.
	wmReq = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	ttl = &types.TimeToLive{
		Value: 0,
		Unit:  types.Days,
	}
	for i := 0; i < n; i++ {
		value = suite.genRow(sid, i, recordKB, false)
		putReq = &nosqldb.PutRequest{
			TableName: suite.table,
			Value:     value,
			ReturnRow: true,
			TTL:       ttl,
		}
		wmReq.AddPutRequest(putReq, false)
		shouldSucceed = append(shouldSucceed, true)
	}

	wmRes, err = suite.Client.WriteMultiple(wmReq)
	if suite.NoErrorf(err, "WriteMultiple() failed, got error: %v", err) {
		suite.verifyResult(wmRes, wmReq, shouldSucceed, nil, recordKB)
	}

	// Verify expiration time
	doNotExpire := time.Time{}
	setExpireTime := false
	for i := 0; i < n; i++ {
		key = suite.genKey(sid, i)
		getReq = &nosqldb.GetRequest{
			TableName: suite.table,
			Key:       key,
		}
		getRes, err = suite.Client.Get(getReq)
		if suite.NoErrorf(err, "Get() failed, got error: %v", err) {
			if !setExpireTime {
				oldExpireTime = getRes.ExpirationTime
				setExpireTime = true
			}
			suite.assertTimeToLive(ttl, getRes.ExpirationTime, doNotExpire)
		}
	}

	// Update rows with new TTL.
	shouldSucceed = make([]bool, 0, n)
	expTTLs := make([]*types.TimeToLive, 0, n)
	wmReq.Clear()
	wmReq.TableName = suite.table

	for i := 0; i < n; i++ {
		value = suite.genRow(sid, i, recordKB, true)
		putReq = &nosqldb.PutRequest{
			TableName: suite.table,
			Value:     value,
			ReturnRow: false,
		}
		ttl = suite.genTTL(i)
		expTTLs = append(expTTLs, ttl)
		if ttl == suite.tableTTL {
			putReq.UseTableTTL = true
		} else {
			putReq.TTL = ttl
		}

		wmReq.AddPutRequest(putReq, false)
		shouldSucceed = append(shouldSucceed, true)
	}

	wmRes, err = suite.Client.WriteMultiple(wmReq)
	if suite.NoErrorf(err, "WriteMultiple() failed, got error: %v", err) {
		suite.verifyResult(wmRes, wmReq, shouldSucceed, nil, recordKB)
	}

	// Verify expiration time
	for i := 0; i < n; i++ {
		key = suite.genKey(sid, i)
		getReq = &nosqldb.GetRequest{
			TableName: suite.table,
			Key:       key,
		}
		getRes, err = suite.Client.Get(getReq)
		if suite.NoErrorf(err, "Get() failed, got error: %v", err) {
			suite.assertTimeToLive(expTTLs[i], getRes.ExpirationTime, oldExpireTime)
		}
	}

}

func (suite *WriteMultipleTestSuite) assertTimeToLive(ttl *types.TimeToLive,
	actualExpireTime, origExpireTime time.Time) {

	if ttl == nil || ttl.Value == 0 {
		suite.Truef(actualExpireTime.Equal(origExpireTime),
			"unexpected expiration time, expect %v, got %v", origExpireTime, actualExpireTime)

	} else {
		suite.Falsef(actualExpireTime.IsZero(), "wrong expiration time, expect non-zero value of time.Time, got %v", actualExpireTime)
		expectExpireTime := time.Now().Add(ttl.ToDuration())
		delta := expectExpireTime.Sub(actualExpireTime)
		if delta < 0 {
			delta = -delta
		}

		var expectDelta time.Duration
		if ttl.Unit == types.Days {
			expectDelta = time.Duration(24*60*60) * time.Second
		} else {
			expectDelta = time.Duration(60*60) * time.Second
		}

		suite.Truef(delta <= expectDelta, "wrong expiration time, expect %v, got %v",
			expectExpireTime, actualExpireTime)
	}
}

func (suite *WriteMultipleTestSuite) genRow(sid, id, recordKB int, isUpdate bool) *types.MapValue {
	var name string
	if isUpdate {
		name = fmt.Sprintf("name_upd_%d_%d", sid, id)
	} else {
		name = fmt.Sprintf("name_%d_%d", sid, id)
	}
	m := &types.MapValue{}
	m.Put("sid", sid).Put("id", id).Put("name", name)
	m.Put("longString", test.GenString((recordKB-1)*1024))
	return m
}

func (suite *WriteMultipleTestSuite) genChildRow(sid, id, childid, recordKB int) *types.MapValue {
	m := &types.MapValue{}
	m.Put("sid", sid).Put("id", id)
	m.Put("childid", childid)
	m.Put("childname", fmt.Sprintf("name_%d_%d", sid, id))
	m.Put("childdata", test.GenString((recordKB-1)*1024))
	return m
}

func (suite *WriteMultipleTestSuite) genKey(sid, id int) *types.MapValue {
	m := &types.MapValue{}
	m.Put("sid", sid).Put("id", id)
	return m
}

func (suite *WriteMultipleTestSuite) runOpAbortedTest(requests []nosqldb.Request, failedOpIdx, recordKB int,
	rowPresents []bool, expFailOpPrevVersion types.Version, expFailOpPrevValue *types.MapValue) {

	wmReq := &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	var err error
	for _, req := range requests {
		switch r := req.(type) {
		case *nosqldb.PutRequest:
			err = wmReq.AddPutRequest(r, true)
			suite.NoErrorf(err, "WriteMultiple() failed to add PutRequest, got error: %v", err)

		case *nosqldb.DeleteRequest:
			err = wmReq.AddDeleteRequest(r, true)
			suite.NoErrorf(err, "WriteMultiple() failed to add DeleteRequest, got error: %v", err)

		default:
			suite.T().Errorf("unsupported request type for WriteMultipleRequest, "+
				"expect *PutRequest or *DeleteRequest, got %T", req)
		}
	}

	wmRes, err := suite.Client.WriteMultiple(wmReq)
	if suite.NoErrorf(err, "WriteMultiple() failed, got error: %v", err) {
		suite.verifyResultAborted(wmRes, wmReq, failedOpIdx, rowPresents,
			recordKB, expFailOpPrevVersion, expFailOpPrevValue)
	}

}

func (suite *WriteMultipleTestSuite) verifyResult(res *nosqldb.WriteMultipleResult, req *nosqldb.WriteMultipleRequest,
	shouldSucceedList, rowPresentList []bool, recordKB int) {

	if !suite.Truef(res.IsSuccess(), "WriteMultiple should have succeeded.") {
		return
	}

	expNum := len(req.Operations)
	actualNum := len(res.ResultSet)
	if !suite.Equalf(expNum, actualNum, "wrong number of results returned.") {
		return
	}

	var expReadKB, expWriteKB int
	var putReq *nosqldb.PutRequest
	var delReq *nosqldb.DeleteRequest
	var opRes nosqldb.OperationResult

	for i := 0; i < actualNum; i++ {
		putReq = req.Operations[i].PutRequest
		delReq = req.Operations[i].DeleteRequest
		opRes = res.ResultSet[i]

		shouldSucceed := shouldSucceedList[i]

		suite.Equalf(shouldSucceed, opRes.Success, "unexpected operation result")
		if shouldSucceed && putReq != nil {
			suite.NotNilf(opRes.Version, "the %s operation should have returnd a non-nil version", ordinal(i))
		} else {
			suite.Nilf(opRes.Version, "the %s operation should have returnd a nil version", ordinal(i))
		}

		if len(rowPresentList) == 0 {
			continue
		}

		rowPresent := rowPresentList[i]
		var shouldReturnRow bool
		if putReq != nil {
			shouldReturnRow = putReq.ReturnRow
			rdKB, wrKB := getPutReadWriteCost(putReq, shouldSucceed, rowPresent, recordKB)
			expReadKB += rdKB
			expWriteKB += wrKB

		} else if delReq != nil {
			shouldReturnRow = delReq.ReturnRow
			rdKB, wrKB := getDeleteReadWriteCost(delReq, shouldSucceed, rowPresent, recordKB)
			expReadKB += rdKB
			expWriteKB += wrKB
		}

		hasReturnRow := !opRes.Success && shouldReturnRow && rowPresentList[i]
		if hasReturnRow {
			suite.NotNilf(opRes.ExistingVersion,
				"the %s operation should have returned a non-nil version for existing row", ordinal(i))
			suite.NotNilf(opRes.ExistingValue,
				"the %s operation should have returned a non-nil value for existing row", ordinal(i))
		} else {
			suite.Nilf(opRes.ExistingVersion,
				"the %s operation should have returned a nil version for existing row", ordinal(i))
			suite.Nilf(opRes.ExistingValue,
				"the %s operation should have returned a nil value for existing row", ordinal(i))
		}
	}

	// Verify read/write cost.
	if test.IsOnPrem() {
		suite.AssertZeroReadWriteKB(res)

	} else if len(rowPresentList) > 0 {
		suite.AssertReadWriteKB(res, expReadKB, expWriteKB, 0, true)
	}
}

func (suite *WriteMultipleTestSuite) verifyResultAborted(res *nosqldb.WriteMultipleResult,
	req *nosqldb.WriteMultipleRequest, failedOpIndex int, rowPresentList []bool, recordKB int,
	expFailOpPrevVersion types.Version, expFailOpPrevValue *types.MapValue) {

	if !suite.Falsef(res.IsSuccess(), "WriteMultiple should have aborted.") {
		return
	}

	var expReadKB, expWriteKB int
	var opRes nosqldb.OperationResult

	if suite.Equalf(failedOpIndex, res.FailedOperationIndex, "wrong FailedOperationIndex") {
		return
	}

	opRes = res.ResultSet[res.FailedOperationIndex]
	suite.Nilf(opRes.Version, "version should have been nil")

	if expFailOpPrevVersion != nil {
		suite.Equalf(expFailOpPrevVersion, opRes.ExistingVersion, "wrong existing version")
	} else {
		suite.Nilf(opRes.ExistingVersion, "existing version should have been nil")
	}

	if expFailOpPrevValue != nil {
		suite.Truef(test.CompareMapValue(expFailOpPrevValue, opRes.ExistingValue, false),
			"wrong existing value")
	} else {
		suite.Nilf(opRes.ExistingValue, "existing value should have been nil")
	}

	if test.IsOnPrem() {
		suite.AssertZeroReadWriteKB(res)
		return
	}

	for i := 0; i <= failedOpIndex; i++ {
		r := req.Operations[i]
		shouldSucceed := i < failedOpIndex

		var rdKB, wrKB int
		if r.PutRequest != nil {
			rdKB, wrKB = getPutReadWriteCost(r.PutRequest, shouldSucceed, rowPresentList[i], recordKB)
		} else if r.DeleteRequest != nil {
			rdKB, wrKB = getDeleteReadWriteCost(r.DeleteRequest, shouldSucceed, rowPresentList[i], recordKB)
		}

		expReadKB += rdKB
		expWriteKB += wrKB
	}

	suite.AssertReadWriteKB(res, expReadKB, expWriteKB, 0, true)

}

func getPutReadWriteCost(req *nosqldb.PutRequest, shouldSucceed, rowPresent bool, recordKB int) (readKB, writeKB int) {
	minRead := test.MinReadKB
	readReturnRow := !shouldSucceed && rowPresent && req.ReturnRow

	switch req.PutOption {
	case types.PutIfAbsent:
		if readReturnRow {
			readKB = recordKB
		} else {
			readKB = minRead
		}

		if shouldSucceed {
			writeKB = recordKB
		}

	case types.PutIfPresent:
		// PutIfPresent never return previous row but cost MIN_READ for
		// searching existing row
		readKB = minRead
		if shouldSucceed {
			writeKB = recordKB + recordKB // old + new record size
		}

	case types.PutIfVersion:
		if readReturnRow {
			readKB = recordKB
		} else {
			readKB = minRead
		}

		if shouldSucceed {
			writeKB = recordKB + recordKB // old + new record size
		}

	default:
		// Put never return previous row.
		readKB = 0
		if shouldSucceed {
			writeKB = recordKB
			if rowPresent {
				writeKB += recordKB
			}
		}
	}

	return
}

func getDeleteReadWriteCost(req *nosqldb.DeleteRequest, shouldSucceed, rowPresent bool, recordKB int) (readKB, writeKB int) {
	readKB = test.MinReadKB
	// Delete never return previous row.
	if req.MatchVersion == nil {
		if shouldSucceed {
			writeKB = recordKB
		}

	} else {
		// The record is present but the version does not matched, read
		// cost is recordKB, otherwise MIN_READ.
		readReturnRow := !shouldSucceed && rowPresent && req.ReturnRow
		if readReturnRow {
			readKB = recordKB
		}

		if shouldSucceed {
			writeKB = recordKB
		}
	}
	return

}

func (suite *WriteMultipleTestSuite) genTTL(i int) *types.TimeToLive {
	switch i % 4 {
	case 1:
		return &types.TimeToLive{
			Value: int64(i + 1),
			Unit:  types.Days,
		}

	case 2:
		return &types.TimeToLive{
			Value: int64(i + 1),
			Unit:  types.Hours,
		}

	case 3:
		return suite.tableTTL

	default:
		return nil
	}
}

func ordinal(i int) string {
	if i < 0 {
		return ""
	}

	var sfx string
	n := i + 1
	switch n {
	case 11, 12, 13:
		sfx = "th"
	default:
		switch n % 10 {
		case 1:
			sfx = "st"
		case 2:
			sfx = "nd"
		case 3:
			sfx = "rd"
		default:
			sfx = "th"
		}
	}

	return fmt.Sprintf("%d%s", n, sfx)
}
func TestWriteMultiple(t *testing.T) {
	test := &WriteMultipleTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
