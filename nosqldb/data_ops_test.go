//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// +build cloud onprem

package nosqldb_test

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// DataOpsTestSuite contains tests for Put/Get/Delete APIs.
type DataOpsTestSuite struct {
	*test.NoSQLTestSuite
}

var (
	// Maximum timestamp value supported by server: 9999-12-31T23:59:59.999999999
	maxTimestamp = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)

	// Minimum timestamp value supported by server: -6383-01-01T00:00:00
	minTimestamp = time.Date(-6383, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// Test Put/Get/Delete APIs with different operation options.
func (suite *DataOpsTestSuite) TestPutGetDelete() {
	var stmt string
	var err error
	table := suite.GetTableName("TestUsers")
	// Drop and re-create test tables.
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"name STRING, "+
		"PRIMARY KEY(id))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  500,
		WriteUnits: 500,
		StorageGB:  50,
	}
	suite.ReCreateTable(table, stmt, limits)

	var putReq *nosqldb.PutRequest
	var putRes *nosqldb.PutResult
	var oldVersion, ifVersion, newVersion types.Version
	recordKB := 2
	name := test.GenString((recordKB - 1) * 1024)
	value := &types.MapValue{}
	value.Put("id", 10).Put("name", name)
	newValue := &types.MapValue{}
	newValue.Put("id", 11).Put("name", name)

	// Put a row.
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true,  // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// Put row again with ReturnRow=true, expect no existing row returned.
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
		oldVersion = putRes.Version
	}

	// PutIfAbsent an existing row, it should fail.
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
		PutOption: types.PutIfAbsent,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false, // shouldSucceed
			true,  // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfAbsent fails + ReturnRow=true, return existing value and version
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false,      // shouldSucceed
			true,       // rowPresent
			value,      // expPrevValue
			oldVersion, // expPrevVersion
			recordKB)
	}

	// PutIfPresent an existing row, it should succeed
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
		PutOption: types.PutIfPresent,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
	}

	// PutIfPresent succeed + ReturnRow=true, expect no existing row returned.
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
		ifVersion = putRes.Version
	}

	// PutIfPresent an new row, it should fail
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     newValue,
		PutOption: types.PutIfPresent,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false, // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfPresent fail + ReturnRow=true, expect no existing row returned.
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false, // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfAbsent an new row, it should succeed
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     newValue,
		PutOption: types.PutIfAbsent,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true,  // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfVersion an existing row with unmatched version, it should fail.
	putReq = &nosqldb.PutRequest{
		TableName:    table,
		Value:        value,
		PutOption:    types.PutIfVersion,
		MatchVersion: oldVersion,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false, // shouldSucceed
			true,  // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfVersion fails + ReturnRow=true, expect no existing row returned.
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false,     // shouldSucceed
			true,      // rowPresent
			value,     // expPrevValue
			ifVersion, // expPrevVersion
			recordKB)
	}

	// Put an existing row with matching version, it should succeed.
	putReq = &nosqldb.PutRequest{
		TableName:    table,
		Value:        value,
		PutOption:    types.PutIfVersion,
		MatchVersion: ifVersion,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
		ifVersion = putRes.Version
	}

	// PutIfVersion succeed + ReturnRow=true, expect no existing row returned.
	putReq.MatchVersion = ifVersion
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
		newVersion = putRes.Version
	}

	// Put with IfVersion but no matched version is specified, put should fail.
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
		PutOption: types.PutIfVersion,
	}
	putRes, err = suite.Client.Put(putReq)
	suite.Truef(nosqlerr.IsIllegalArgument(err),
		"PutIfVersion without a match version specified should have been failed with an IllegalArgument error")

	// Test Get operations
	var getReq *nosqldb.GetRequest
	var getRes *nosqldb.GetResult

	expOrderedValue := types.NewOrderedMapValue()
	expOrderedValue.Put("id", 10).Put("name", name)

	// Get a row.
	key := types.ToMapValue("id", 10)
	getReq = &nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	}
	getRes, err = suite.Client.Get(getReq)
	if suite.NoError(err) {
		suite.checkGetResult(getReq, getRes,
			true,            // rowPresent
			true,            // checkFieldOrder
			expOrderedValue, // expected value
			nil,             // Don't check version if use Eventual consistency
			recordKB)
	}

	// Get a row with Absolute consistency
	getReq.Consistency = types.Absolute
	getRes, err = suite.Client.Get(getReq)
	if suite.NoError(err) {
		suite.checkGetResult(getReq, getRes,
			true,            // rowPresent
			true,            // checkFieldOrder
			expOrderedValue, // expected value
			newVersion,      // expected version
			recordKB)
	}

	// Get non-existing row
	key.Put("id", 100)
	getReq = &nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	}
	getRes, err = suite.Client.Get(getReq)
	if suite.NoError(err) {
		suite.checkGetResult(getReq, getRes,
			false, // rowPresent
			false, // checkFieldOrder
			nil,   // expected value
			nil,   // expected version
			recordKB)
	}

	// Get non-existing row with Absolute consistency
	getReq.Consistency = types.Absolute
	getRes, err = suite.Client.Get(getReq)
	if suite.NoError(err) {
		suite.checkGetResult(getReq, getRes,
			false, // rowPresent
			false, // checkFieldOrder
			nil,   // expected value
			nil,   // expected version
			recordKB)
	}

	// Test Delete operations
	var delReq *nosqldb.DeleteRequest
	var delRes *nosqldb.DeleteResult

	// Delete a row
	key.Put("id", 10)
	delReq = &nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	}
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
	}

	// Put the row back to store
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	_, err = suite.Client.Put(putReq)
	suite.NoError(err)

	// Delete succeed + ReturnRow=true, no existing row returned.
	delReq.ReturnRow = true
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
	}

	// Delete fail + ReturnRow=true, no existing row returned.
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			false, // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// Put the row back to store
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		ifVersion = putRes.Version
	}

	// DeleteIfVersion with unmatched version, it should fail
	delReq = &nosqldb.DeleteRequest{
		TableName:    table,
		Key:          key,
		MatchVersion: oldVersion,
	}
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			false, // shouldSucceed
			true,  // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// DeleteIfVersion with unmatched version + ReturnRow=true, the existing row returned.
	delReq.ReturnRow = true
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			false,     // shouldSucceed
			true,      // rowPresent
			value,     // expPrevValue
			ifVersion, // expPrevVersion
			recordKB)
	}

	// DeleteIfVersion with matched version, it should succeed.
	delReq = &nosqldb.DeleteRequest{
		TableName:    table,
		Key:          key,
		MatchVersion: ifVersion,
	}
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
	}

	// Put the row back to store
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		ifVersion = putRes.Version
	}

	// DeleteIfVersion with matched version + ReturnRow=true,
	// it should succeed but no existing row returned.
	delReq = &nosqldb.DeleteRequest{
		TableName:    table,
		Key:          key,
		MatchVersion: ifVersion,
		ReturnRow:    true,
	}
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			true, // shouldSucceed
			true, // rowPresent
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
	}

	// DeleteIfVersion with a key not existed, it should fail.
	delReq = &nosqldb.DeleteRequest{
		TableName:    table,
		Key:          key,
		MatchVersion: ifVersion,
	}
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			false, // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// DeleteIfVersion with a key not existed + ReturnRow=true,
	// it should fail and no existing row returned.
	delReq.ReturnRow = true
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			false, // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}
}

// TestPutExactMatch tests put operations with and without "ExactMatch" set.
func (suite *DataOpsTestSuite) TestPutExactMatch() {
	var stmt string
	var err error

	table := suite.GetTableName("TestExactMatch")
	// Drop and re-create test tables.
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"str STRING, "+
		"PRIMARY KEY(id))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  100,
		WriteUnits: 100,
		StorageGB:  1,
	}
	suite.ReCreateTable(table, stmt, limits)

	var str string
	var value *types.MapValue
	var req *nosqldb.PutRequest
	var res *nosqldb.PutResult
	var currVer types.Version

	// Put an extra field value using the default value for ExactMatch.
	recordKB := 2
	id := 1
	str = test.GenString((recordKB - 1) * 1024)
	value = &types.MapValue{}
	value.Put("id", id).Put("str", str).Put("extra", "foo")

	req = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	res, err = suite.Client.Put(req)
	if suite.NoErrorf(err, "Put(): %v", err) {
		suite.checkPutResult(req, res,
			true,  // shouldSucceed
			false, // rowPresent
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)

		currVer = res.Version
	}

	// Put an extra field value with ExactMatch set to true.
	req = &nosqldb.PutRequest{
		TableName:  table,
		ExactMatch: true,
	}

	options := [...]types.PutOption{
		types.PutIfPresent, types.PutIfAbsent, types.PutIfVersion,
	}
	for i, opt := range options {
		str = opt.String() + "-str"
		value = &types.MapValue{}
		value.Put("str", str).Put("extra", "bar")
		switch opt {
		case types.PutIfAbsent:
			value.Put("id", 100+i)

		case types.PutIfPresent:
			value.Put("id", id)

		case types.PutIfVersion:
			value.Put("id", id)
			req.MatchVersion = currVer
		}

		req.PutOption = opt
		req.Value = value
		res, err = suite.Client.Put(req)
		suite.Truef(nosqlerr.IsIllegalArgument(err),
			"Put(ExactMatch=true, Option=%v) expect IllegalArgument error, got: %v", opt, err)
	}

	// Put with multiple field values for the "str" field where the names are in different cases.
	value = &types.MapValue{}
	value.Put("id", 99)
	value.Put("str", "str-a").Put("STR", "STR-b").Put("StR", "StR-c")
	req = &nosqldb.PutRequest{
		TableName:  table,
		Value:      value,
		ExactMatch: true,
	}
	_, err = suite.Client.Put(req)
	suite.NoErrorf(err, "Put(ExactMatch=true) got error: %v", err)
}

func (suite *DataOpsTestSuite) TestPutIdentity() {
	var stmt string
	var err error

	table := suite.GetTableName("TestIdentityValue")
	// Drop and re-create test tables.
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"pk INTEGER, "+
		"sk INTEGER, "+
		"str STRING, "+
		"id LONG GENERATED ALWAYS AS IDENTITY (CACHE 100), "+
		"PRIMARY KEY(SHARD(sk), pk))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  1000,
		WriteUnits: 1000,
		StorageGB:  1,
	}
	suite.ReCreateTable(table, stmt, limits)

	var putReq *nosqldb.PutRequest
	var putRes *nosqldb.PutResult
	var value *types.MapValue
	idCacheSize := 15

	putReq = &nosqldb.PutRequest{
		TableName:         table,
		IdentityCacheSize: idCacheSize,
	}

	var pk int
	var expectId int64
	n := idCacheSize + 3
	for i := 0; i < n; i++ {
		pk++
		expectId++
		value = &types.MapValue{}
		value.Put("pk", pk).Put("sk", pk%5).Put("str", "str-"+strconv.Itoa(i))
		putReq.Value = value
		putRes, err = suite.Client.Put(putReq)
		if suite.NoErrorf(err, "Put(pk=%d, IdentityCacheSize=%d) got error: %v", pk, idCacheSize, err) {
			id, ok := putRes.GeneratedValue.(int64)
			if suite.Truef(ok, "expect generated id value of int64, got %T", putRes.GeneratedValue) {
				suite.Equalf(expectId, id, "wrong id value returned")
			}
		}
	}

	var wmReq *nosqldb.WriteMultipleRequest
	var wmRes *nosqldb.WriteMultipleResult
	wmReq = &nosqldb.WriteMultipleRequest{
		TableName: table,
	}

	n = idCacheSize + 1
	for i := 0; i < n; i++ {
		pk++
		value = &types.MapValue{}
		value.Put("pk", pk).Put("sk", 1).Put("str", "str-"+strconv.Itoa(i))
		putReq = &nosqldb.PutRequest{
			TableName:         table,
			IdentityCacheSize: idCacheSize,
			Value:             value,
		}
		wmReq.AddPutRequest(putReq, true)
	}

	wmRes, err = suite.Client.WriteMultiple(wmReq)
	if suite.NoErrorf(err, "WriteMultiple(PutOp.IdentityCacheSize=%d) got error: %v", idCacheSize, err) {
		if suite.Equalf(n, len(wmRes.ResultSet), "unexpected number of results") {
			for _, r := range wmRes.ResultSet {
				expectId++
				id, ok := r.GeneratedValue.(int64)
				if suite.Truef(ok, "expect generated id value of int64, got %T", r.GeneratedValue) {
					suite.Equalf(expectId, id, "wrong id value returned")
				}
			}
		}
	}

	// Put a value for the identity column. This should fail.
	pk++
	value = &types.MapValue{}
	value.Put("pk", pk).Put("sk", 1).Put("str", "str-"+strconv.Itoa(pk))
	value.Put("id", 1)
	_, err = suite.Client.Put(putReq)
	suite.Truef(nosqlerr.IsIllegalArgument(err),
		"Put(pk=%d, id=%d) expect IllegalArgument error, got: %v", pk, 1, err)

}

// Test request size limit.
func (suite *DataOpsTestSuite) TestRequestSizeLimit() {
	table := suite.GetTableName("RequestSizeTest")
	// Drop and re-create test tables.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sk STRING, "+
		"pk STRING, "+
		"data STRING, "+
		"PRIMARY KEY(shard(sk), pk))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  50,
		WriteUnits: 50,
		StorageGB:  1,
	}
	suite.ReCreateTable(table, stmt, limits)

	skSize, pkSize, dataSize := 5, 10, proto.RequestSizeLimit
	value := &types.MapValue{}
	sk := strings.Repeat("s", skSize)
	value.Put("sk", sk).Put("pk", strings.Repeat("a", pkSize))
	value.Put("data", strings.Repeat("d", dataSize))
	putReq := &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}

	msgPrefix := fmt.Sprintf("Put(skSize=%d, pkSize=%d, dataSize=%d) ",
		skSize, pkSize, dataSize)
	_, err := suite.Client.Put(putReq)
	// Request size is not checked for on-premise case.
	if suite.IsOnPrem() {
		suite.NoErrorf(err, msgPrefix+"got error %v", err)
	} else {
		// There are two violations in this test case.
		// 1. the request size exceeds the limit of 2MB
		// 2. the row size exceeds the limit of 512KB
		// We expect a RequestSizeLimitExceeded error, rather than
		// RowSizeLimitExceeded error, because the 1st error is reported early
		// by client.
		suite.Truef(nosqlerr.Is(err, nosqlerr.RequestSizeLimitExceeded),
			msgPrefix+"got %v, expect RequestSizeLimitExceeded", err)
	}

	// Test sub request size for write multiple request.
	key := &types.MapValue{}
	key.Put("sk", sk).Put("pk", strings.Repeat("b", pkSize))
	delReq := &nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	}

	wmReq := &nosqldb.WriteMultipleRequest{
		TableName: table,
	}
	wmReq.AddPutRequest(putReq, true)
	wmReq.AddDeleteRequest(delReq, true)
	msgPrefix = fmt.Sprintf("WriteMultiple: Put(size=%d), Delete(size=%d) ",
		skSize+pkSize+dataSize, skSize+pkSize)

	_, err = suite.Client.WriteMultiple(wmReq)
	if suite.IsOnPrem() {
		suite.NoErrorf(err, msgPrefix+"got error %v", err)
	} else {
		suite.Truef(nosqlerr.Is(err, nosqlerr.RequestSizeLimitExceeded),
			msgPrefix+"got error %v, expect RequestSizeLimitExceeded", err)
	}

	// Test the case where each sub request size does not exceed the limit,
	// but the WriteMultipleRequest size exceeds the limit.
	wmReq.Clear()
	wmReq.TableName = table
	totalSize := 0
	pkSize = 0
	dataSize = proto.RequestSizeLimit / 2
	for {
		pkSize++
		value := &types.MapValue{}
		value.Put("sk", sk)
		value.Put("pk", strings.Repeat("a", pkSize))
		value.Put("data", strings.Repeat("d", dataSize))
		totalSize += skSize + pkSize + dataSize
		putReq := &nosqldb.PutRequest{
			TableName: table,
			Value:     value,
		}
		wmReq.AddPutRequest(putReq, true)
		if totalSize > proto.BatchRequestSizeLimit {
			break
		}
	}

	_, err = suite.Client.WriteMultiple(wmReq)
	if suite.IsOnPrem() {
		// Request size is not checked for on-premise, but the server may set
		// a limit for http request content length, e.g. 25MB, we may get the
		// "error response: 413 Request Entity Too Large" error.
		suite.Truef(err == nil || strings.Contains(err.Error(), "413 Request Entity Too Large"),
			"WriteMultiple() got unexpected error %v", err)
	} else {
		suite.Truef(nosqlerr.Is(err, nosqlerr.RequestSizeLimitExceeded),
			"WriteMultiple() got error %v, expect RequestSizeLimitExceeded", err)
	}
}

// Test key size limit and data size limit of Put request.
func (suite *DataOpsTestSuite) TestDataSizeLimit() {
	table := suite.GetTableName("DataSizeTest")
	// Drop and re-create test tables.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sk STRING, "+
		"data STRING, "+
		"pk STRING, "+
		"PRIMARY KEY(shard(sk), pk))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  500,
		WriteUnits: 500,
		StorageGB:  1,
	}
	suite.ReCreateTable(table, stmt, limits)

	// Key and data size limits are only for cloud service.
	const keySizeLimit = 64
	const dataSizeLimit = 512 * 1024

	type dataSizeLimitTestCase struct {
		skValueSize   int
		pkValueSize   int
		dataValueSize int
		expErr        nosqlerr.ErrorCode
	}

	var testCase1 []*dataSizeLimitTestCase
	var testCase2 []*dataSizeLimitTestCase

	testCase1 = []*dataSizeLimitTestCase{
		// Positive tests
		{keySizeLimit, 0, 100, nosqlerr.NoError},
		{0, keySizeLimit, 100, nosqlerr.NoError},
		{keySizeLimit - 2, 2, 100, nosqlerr.NoError},
		{1, keySizeLimit - 1, 100, nosqlerr.NoError},
		{1, keySizeLimit - 1, dataSizeLimit - keySizeLimit, nosqlerr.NoError},
	}

	if test.IsCloud() {
		testCase2 = []*dataSizeLimitTestCase{
			// Negative tests
			{keySizeLimit, 2, 0, nosqlerr.KeySizeLimitExceeded},
			{2, keySizeLimit, 0, nosqlerr.KeySizeLimitExceeded},
			{2, 2, dataSizeLimit, nosqlerr.RowSizeLimitExceeded},
		}
	} else {
		// On-prem servers do not enforce key and data size limits.
		testCase2 = []*dataSizeLimitTestCase{
			{keySizeLimit, 2, 0, nosqlerr.NoError},
			{2, keySizeLimit, 0, nosqlerr.NoError},
			{2, 2, dataSizeLimit, nosqlerr.NoError},
		}
	}

	testCase1 = append(testCase1, testCase2...)
	var msgPrefix string
	for i, r := range testCase1 {
		msgPrefix = fmt.Sprintf("Testcase %d: Put(skSize=%d, pkSize=%d, dataSize=%d) ",
			i+1, r.skValueSize, r.pkValueSize, r.dataValueSize)
		m := map[string]interface{}{
			"sk":   test.GenString(r.skValueSize),
			"pk":   test.GenString(r.pkValueSize),
			"data": test.GenString(r.dataValueSize),
		}
		value := types.NewMapValue(m)

		putReq := &nosqldb.PutRequest{
			TableName: table,
			Value:     value,
		}
		_, err := suite.Client.Put(putReq)
		switch r.expErr {
		case nosqlerr.NoError:
			suite.NoErrorf(err, msgPrefix+"got error: %v", err)

		case nosqlerr.KeySizeLimitExceeded:
			suite.Truef(nosqlerr.Is(err, r.expErr),
				msgPrefix+"key size exceeds the limit of %d, expect error: %v, got %v",
				keySizeLimit, nosqlerr.KeySizeLimitExceeded, err)

		case nosqlerr.RowSizeLimitExceeded:
			suite.Truef(nosqlerr.Is(err, r.expErr),
				msgPrefix+"data size exceeds the limit of %d, expect error: %v, got %v",
				dataSizeLimit, nosqlerr.RowSizeLimitExceeded, err)
		}
	}
}

func (suite *DataOpsTestSuite) TestNonNumericDataTypes() {
	var stmt string
	var err error
	table := suite.GetTableName("NonNumericDataTest")
	// Drop and re-create test tables.
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"bl BOOLEAN, "+
		"s STRING, "+
		"e ENUM(red, yellow, blue), "+
		"ts TIMESTAMP(9), "+
		"bi BINARY, "+
		"fbi BINARY(10), "+
		"json JSON,"+
		"PRIMARY KEY(id)"+
		")", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  500,
		WriteUnits: 500,
		StorageGB:  2,
	}
	suite.ReCreateTable(table, stmt, limits)

	intVal := 1
	boolVal := true
	strVal := "Oracle NoSQL"
	bi10Val := test.GenBytes(10)
	bi20Val := test.GenBytes(20)
	strByte10 := base64.StdEncoding.EncodeToString(bi10Val)
	strByte20 := base64.StdEncoding.EncodeToString(bi20Val)
	tsStrVal := "2019-05-02T10:23:42.123"
	tsVal, err := time.Parse(types.ISO8601Layout, tsStrVal)
	suite.Require().NoErrorf(err, "failed to parse %s as time.Time, got error %v.", tsStrVal, err)

	testCases := []struct {
		targetField   string
		invalidValues []types.FieldValue
		validValues   []types.FieldValue
	}{
		// Boolean type
		{"bl", []types.FieldValue{intVal, tsVal, bi10Val}, []types.FieldValue{boolVal, strVal, "TRUE", "FalSE"}},
		// String type
		{"s", []types.FieldValue{intVal, boolVal, tsVal, bi10Val}, []types.FieldValue{strVal}},
		// Enum type
		{
			"e",
			// Enum values are case-sensitive
			[]types.FieldValue{intVal, boolVal, strVal, tsVal, bi10Val, "RED", "YEllow", "BLue"},
			[]types.FieldValue{"red", "yellow", "blue"},
		},
		// Timestamp type
		{
			"ts",
			[]types.FieldValue{intVal, boolVal, strVal, bi10Val, "2019-05-02 10:23:42.123"},
			[]types.FieldValue{tsVal, tsStrVal, tsVal.Format(types.ISO8601Layout),
				tsVal.Format(time.RFC3339), tsVal.Format(time.RFC3339Nano)},
		},
		// Binary type
		{"bi", []types.FieldValue{intVal, boolVal, strVal, tsVal}, []types.FieldValue{bi10Val, bi20Val, strByte10, strByte20}},
		// Fixed binary type
		{"fbi", []types.FieldValue{intVal, boolVal, strVal, tsVal, bi20Val, strByte20}, []types.FieldValue{bi10Val, strByte10}},
		// JSON type
		{"json", nil, []types.FieldValue{intVal, boolVal, strVal, tsVal, bi10Val, bi20Val, strByte10, strByte20}},
	}

	for i, r := range testCases {
		id := i*10 + 1
		if r.targetField != "json" {
			suite.runPut(table, r.targetField, id, r.invalidValues, nosqlerr.IllegalArgument)
		}
		suite.runPut(table, r.targetField, id, r.validValues, nosqlerr.NoError)
	}

}

func (suite *DataOpsTestSuite) TestNumericDataTypes() {
	var stmt string
	table := suite.GetTableName("NumericDataTest")

	// Drop and re-create test tables.
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"i INTEGER, "+
		"l LONG, "+
		"f FLOAT,"+
		"d DOUBLE, "+
		"n NUMBER, "+
		"PRIMARY KEY(shard(id), n)"+
		")", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  20000,
		WriteUnits: 20000,
		StorageGB:  50,
	}
	suite.ReCreateTable(table, stmt, limits)

	var n int
	count := 20

	ints := []int{
		math.MinInt32,
		math.MaxInt32,
		0,
		-1,
		12345,
	}
	n = count - len(ints)
	ints = append(ints, genInts(-1000, 1000, n)...)

	longs := []int64{
		math.MinInt64,
		math.MaxInt64,
		0,
		-1,
		987654321012345678,
	}
	n = count - len(longs)
	longs = append(longs, genInt64s(-1000, 1000, n)...)

	floats := []float32{
		// math.SmallestNonzeroFloat32,
		-math.MaxFloat32,
		math.MaxFloat32,
		0.0,
		float32(1.234567),
	}
	n = count - len(floats)
	floats = append(floats, genFloat32s(-1000, 1000, n)...)

	doubles := []float64{
		// math.SmallestNonzeroFloat64,
		-math.MaxFloat64,
		math.MaxFloat64,
		0.0,
		0.33,
		float64(math.MaxInt64),
		float64(math.MinInt64),
		9.8765432123456,
		math.Inf(1),  // positive infinity
		math.Inf(-1), // negative infinity
		math.NaN(),   // an IEEE 754 “not-a-number” value
	}
	n = count - len(doubles)
	doubles = append(doubles, genFloat64s(-1000, 1000, n)...)

	numbers := []string{
		"98765432109876543210987654321098765432109876543210",
		"0",
		"1",
		"-1",
		fmt.Sprintf("%d", math.MaxInt32),
		fmt.Sprintf("%d", math.MinInt32),
		fmt.Sprintf("%d", math.MaxInt64),
		fmt.Sprintf("%d", math.MinInt64),
		fmt.Sprintf("%E", math.MaxFloat32),
		fmt.Sprintf("%E", math.MaxFloat64),
		// fmt.Sprintf("%e", math.SmallestNonzeroFloat64),
	}
	n = count - len(numbers)
	doubleValues := genFloat64s(-1000, 1000, n)
	doubleStrings := make([]string, n)
	for i := 0; i < n; i++ {
		// use scientific notation
		doubleStrings[i] = fmt.Sprintf("%E", doubleValues[i])
	}
	numbers = append(numbers, doubleStrings...)

	id := 0
	for i := 0; i < count; i++ {
		id++
		// use string as NUMBER value
		suite.runPutGetDeleteNumericData(table, id, ints[i], longs[i], floats[i], doubles[i], numbers[i], false)

		ratNum, ok := new(big.Rat).SetString(numbers[i])
		if suite.Truef(ok, "big.Rat.SetString(%q) failed", numbers[i]) {
			// use big.Rat as NUMBER value
			id++
			suite.runPutGetDeleteNumericData(table, id, ints[i], longs[i], floats[i], doubles[i], ratNum, false)
		}

		// Non-numeric numbers such as NaN, +Inf, -Inf are not supported by JSON
		if math.IsNaN(doubles[i]) || math.IsInf(doubles[i], 0) {
			continue
		}

		// Test the cases where key and value are created from JSON strings.
		id++
		suite.runPutGetDeleteNumericData(table, id, ints[i], longs[i], floats[i], doubles[i], numbers[i], true)

		if ok {
			id++
			suite.runPutGetDeleteNumericData(table, id, ints[i], longs[i], floats[i], doubles[i], ratNum, true)
		}
	}
}

// Test compatible numeric values.
func (suite *DataOpsTestSuite) TestCompatibleNumericTypes() {
	var stmt string
	table := suite.GetTableName("CompatibleNumericTest")

	// Drop and re-create test tables.
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"i INTEGER, "+
		"l LONG, "+
		"f FLOAT,"+
		"d DOUBLE, "+
		"n NUMBER, "+
		"PRIMARY KEY(id)"+
		")", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  200,
		WriteUnits: 200,
		StorageGB:  5,
	}
	suite.ReCreateTable(table, stmt, limits)

	testCases := []struct {
		fieldName     string      // target field name
		fieldVal      interface{} // input field value
		expFieldVal   interface{} // expect field value
		shouldSucceed bool
	}{
		// Target field type: Integer
		//
		// Use int8 and uint8 values
		{"i", int8(math.MinInt8), int(math.MinInt8), true},
		{"i", int8(math.MaxInt8), int(math.MaxInt8), true},
		{"i", uint8(math.MaxUint8), int(math.MaxUint8), true},
		// Use int16 and uint16 values
		{"i", int16(math.MinInt16), int(math.MinInt16), true},
		{"i", int16(math.MaxInt16), int(math.MaxInt16), true},
		{"i", uint16(math.MaxUint16), int(math.MaxUint16), true},
		// Use int32 and uint32 values
		{"i", int32(math.MinInt32), int(math.MinInt32), true},
		{"i", int32(math.MaxInt32), int(math.MaxInt32), true},
		{"i", uint32(math.MaxInt32 + 1), nil, false},
		{"i", uint32(math.MaxUint32), nil, false},
		// Use int and uint values
		{"i", int(math.MinInt32), int(math.MinInt32), true},
		{"i", int(math.MaxInt32), int(math.MaxInt32), true},
		{"i", uint(math.MaxInt32 + 1), nil, false},
		{"i", uint(math.MaxUint32), nil, false},
		// Use int64 and uint64 values
		{"i", int64(math.MaxInt32), int(math.MaxInt32), true},
		{"i", int64(math.MinInt32 - 1), nil, false},
		{"i", int64(math.MaxInt32 + 1), nil, false},
		{"i", int64(math.MaxInt64), nil, false},
		{"i", int64(math.MinInt64), nil, false},
		{"i", uint64(math.MaxInt32), int(math.MaxInt32), true},
		{"i", uint64(math.MaxInt32 + 1), nil, false},
		{"i", uint64(math.MaxUint64), nil, false},
		// Use float32 values
		{"i", float32(1.2345678e7), int(1.2345678e7), true},
		{"i", float32(-1.1), nil, false},
		// Use float64 values
		{"i", float64(1.2345678e7), int(1.2345678e7), true},
		{"i", float64(-1.1), nil, false},
		// Use *big.Rat
		{"i", new(big.Rat).SetInt64(int64(math.MaxInt32)), int(math.MaxInt32), true},
		{"i", new(big.Rat).SetInt64(int64(math.MaxInt64)), nil, false},
		{"i", new(big.Rat).SetFloat64(float64(1.2345678e7)), int(1.2345678e7), true},
		//
		// Target field type: Long
		//
		// Use int32 and uint32 values
		{"l", int32(math.MinInt32), int64(math.MinInt32), true},
		{"l", int32(math.MaxInt32), int64(math.MaxInt32), true},
		{"l", uint32(math.MaxUint32), int64(math.MaxUint32), true},
		// Use int and uint values
		{"l", int(math.MinInt32), int64(math.MinInt32), true},
		{"l", int(math.MaxInt32), int64(math.MaxInt32), true},
		{"l", uint(math.MaxUint32), int64(math.MaxUint32), true},
		// Use int64 and uint64 values
		{"l", int64(math.MaxInt64), int64(math.MaxInt64), true},
		{"l", int64(math.MinInt64), int64(math.MinInt64), true},
		{"l", uint64(math.MaxInt64 + 1), nil, false},
		{"l", uint64(math.MaxUint64), nil, false},
		// Use float64 values
		{"l", float64(1.234567890123e12), int64(1.234567890123e12), true},
		{"l", float64(-1.1), nil, false},
		// Use *big.Rat
		{"l", new(big.Rat).SetInt64(int64(math.MaxInt64)), int64(math.MaxInt64), true},
		{"l", new(big.Rat).SetFloat64(float64(1.234567890123e12)), int64(1.234567890123e12), true},
		{"l", new(big.Rat).SetFloat64(float64(1234567890.1)), nil, false},
		//
		// Target KV field type: Float
		//
		// Use int and uint values
		{"f", int(16777216), float64(16777216), true},
		// 16777217 cannot be represented with the accuracy of 32-bit floating points
		{"f", int(16777217), nil, false},
		{"f", uint(16777218), float64(16777218), true},
		// Use int64 and uint64 values
		{"f", int64(math.MaxInt64), float64(math.MaxInt64), true},
		{"f", int64(math.MaxInt64 - 1), nil, false},
		{"f", uint64(math.MaxInt64 + 1), nil, false},
		{"f", uint64(math.MaxUint64), nil, false},
		// Use float64 values
		{"f", float64(-math.MaxFloat32), float64(-math.MaxFloat32), true},
		{"f", float64(math.MaxFloat64), nil, false},
		// Use *big.Rat values
		{"f", new(big.Rat).SetFloat64(1.2345678e7), float64(1.2345678e7), true},
		{"f", new(big.Rat).SetFloat64(float64(math.MaxFloat64)), nil, false},
		//
		// Target KV field type: Double
		//
		// Use int/int64 values
		{"d", int(math.MaxInt32), float64(math.MaxInt32), true},
		{"d", int64(math.MaxInt64), float64(math.MaxInt64), true},
		{"d", int64(math.MaxInt64 - 1), nil, false},
		// Use *big.Rat values
		{"d", new(big.Rat).SetInt64(int64(math.MaxInt64)), float64(math.MaxInt64), true},
		{"d", new(big.Rat).SetFloat64(float64(math.MaxInt64 - 1)), nil, false},
		//
		// Target KV field type: Number
		//
		{"n", int(math.MaxInt32), new(big.Rat).SetInt64(int64(math.MaxInt32)), true},
		{"n", int64(math.MaxInt64), new(big.Rat).SetInt64(int64(math.MaxInt64)), true},
		{"n", float32(math.MaxFloat32), new(big.Rat).SetFloat64(float64(math.MaxFloat32)), true},
		{"n", float64(math.MaxFloat64), new(big.Rat).SetFloat64(float64(math.MaxFloat64)), true},
	}

	var prefixMsg string
	for i, r := range testCases {
		id := i + 1
		prefixMsg = fmt.Sprintf("Testcase %d: ", id)
		key := &types.MapValue{}
		key.Put("id", id)

		value := &types.MapValue{}
		value.Put("id", id)
		value.Put(r.fieldName, r.fieldVal)

		putReq := &nosqldb.PutRequest{
			TableName: table,
			Value:     value,
		}
		_, err := suite.Client.Put(putReq)
		if !r.shouldSucceed {
			suite.Errorf(err, prefixMsg+"Put with value=%v should have failed, but succeeded.", value)
			continue
		}

		// Handle the case where r.shouldSucceed=true
		if !suite.NoErrorf(err, prefixMsg+"Put with value=%v failed, got error %v", value, err) {
			continue
		}

		getReq := &nosqldb.GetRequest{
			TableName: table,
			Key:       key,
		}
		getRes, err := suite.Client.Get(getReq)
		if !suite.NoErrorf(err, prefixMsg+"Get with key=%v failed, got error %v.", key, err) {
			continue
		}

		if r.expFieldVal == nil {
			continue
		}

		// Check expected values.
		v, _ := getRes.Value.Get(r.fieldName)
		if r.fieldName == "n" {
			expNumVal, ok := r.expFieldVal.(*big.Rat)
			if suite.Truef(ok, "expected value of \"n\" should be of *big.Rat, got %T", r.expFieldVal) {
				actualNumVal, ok := v.(*big.Rat)
				if suite.Truef(ok, "returned value of \"n\" should be of *big.Rat, got %T", v) {
					suite.checkNumberValue(expNumVal, actualNumVal)
				}
			}

		} else {
			suite.Equalf(r.expFieldVal, v, "got unexpected value of %q field", r.fieldName)
		}

	}
}

func (suite *DataOpsTestSuite) TestTimestampDataType() {
	table := suite.GetTableName("TimestampTest")
	// Drop and re-create test tables.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"ts0 Timestamp(0), "+
		"ts1 Timestamp(1), "+
		"ts2 Timestamp(2), "+
		"ts3 Timestamp(3), "+
		"ts4 Timestamp(4), "+
		"ts5 Timestamp(5), "+
		"ts6 Timestamp(6), "+
		"ts7 Timestamp(7), "+
		"ts8 Timestamp(8), "+
		"ts9 Timestamp(9), "+
		"PRIMARY KEY(SHARD(ts0), ts9)"+
		")", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  20,
		WriteUnits: 20,
		StorageGB:  2,
	}
	suite.ReCreateTable(table, stmt, limits)

	// Unix Epoch time: 1970-01-01T00:00:00.000000000
	epoch := time.Unix(0, 0)
	// Zero value of time.Time: 0001-01-01T00:00:00.000000000
	var zeroTS time.Time

	testCases := []struct {
		tsValue       interface{} // Note: specify a string or time.Time value
		shouldSucceed bool
	}{
		{"2019-07-13T16:48:05.123456789", true},
		// Use Unix Epoch time
		{epoch, true},
		// 1970-01-01T00:00:00.999999999
		{epoch.Add(time.Duration(999999999)), true},
		{zeroTS, true},
		// The minimum timestamp value supported by server is -6383-01-01.
		{minTimestamp, true},
		{minTimestamp.Add(-1 * time.Nanosecond), false},
		// Go's time.Parse() cannot handle negative years, try test with the
		// string representation of minimum non-negative time: "0000-01-01T00:00:00"
		{"0000-01-01T00:00:00", true},
		{time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC), true},
		// The maximum timestamp value supported by server.
		{"9999-12-31T23:59:59.999999999", true},
		{maxTimestamp, true},
		{maxTimestamp.Add(time.Nanosecond), false},
	}

	for _, r := range testCases {
		suite.runTimestampTest(table, r.tsValue, r.shouldSucceed)
	}
}

func (suite *DataOpsTestSuite) TestNullJsonNull() {
	var stmt string
	var err error
	jsonTable := suite.GetTableName("TJson")
	recordTable := suite.GetTableName("TRecord")

	// Drop and re-create test tables.
	limits := &nosqldb.TableLimits{
		ReadUnits:  10,
		WriteUnits: 10,
		StorageGB:  1,
	}
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"info JSON,"+
		"PRIMARY KEY(id)"+
		")", jsonTable)
	suite.ReCreateTable(jsonTable, stmt, limits)

	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"info RECORD(name STRING, age INTEGER),"+
		"PRIMARY KEY(id)"+
		")", recordTable)
	suite.ReCreateTable(recordTable, stmt, limits)

	rowJsonNull := types.NewMapValue(map[string]interface{}{
		"id": 0,
		"info": map[string]interface{}{
			"name": types.JSONNullValueInstance,
			"age":  20,
		},
	})

	rowNull := types.NewMapValue(map[string]interface{}{
		"id": 0,
		"info": map[string]interface{}{
			"name": types.NullValueInstance,
			"age":  20,
		},
	})

	// Put rows with NullValue or JsonNullValue
	testCases := []struct {
		table string
		value *types.MapValue
	}{
		{jsonTable, rowJsonNull},
		{recordTable, rowNull},
	}

	for _, r := range testCases {
		putReq := &nosqldb.PutRequest{
			TableName: r.table,
			Value:     r.value,
		}
		putRes, err := suite.Client.Put(putReq)
		if suite.NoError(err) {
			suite.NotNil(putRes.Version)
		}
		id, _ := r.value.Get("id")
		key := &types.MapValue{}
		key.Put("id", id)
		getReq := &nosqldb.GetRequest{
			TableName: r.table,
			Key:       key,
		}
		getRes, err := suite.Client.Get(getReq)
		if suite.NoError(err) {
			suite.Truef(test.CompareMapValue(r.value, getRes.Value, false),
				"unexpected value returned, expect %v, got %v",
				r.value, getRes.Value)
		}
	}

	// Query with variable for json field and set NullValue or JsonNullValue to
	// variable, the NullValue is expected to be converted to JsonNullValue
	stmt = fmt.Sprintf("declare $name json;"+
		"select * from %s t where t.info.name = $name", jsonTable)
	prepReq := &nosqldb.PrepareRequest{
		Statement: stmt,
	}
	prepRes, err := suite.Client.Prepare(prepReq)
	if suite.NoError(err, "Prepare(%s) got error %v", stmt, err) {
		prepRes.PreparedStatement.SetVariable("$name", types.JSONNullValueInstance)
		queryReq := &nosqldb.QueryRequest{
			PreparedStatement: &prepRes.PreparedStatement,
		}

		results, err := suite.ExecuteQueryRequest(queryReq)
		if suite.NoErrorf(err, "Query($name=JSONNullValue) got error %v", err) {
			if suite.Equalf(1, len(results), "unexpected number of rows returned.") {
				suite.Truef(test.CompareMapValue(rowJsonNull, results[0], false),
					"unexpected value returned, expect %v, got %v",
					rowJsonNull.Map(), results[0].Map())
			}
		}

		prepRes.PreparedStatement.SetVariable("$name", types.NullValueInstance)
		results, err = suite.ExecuteQueryRequest(queryReq)
		if suite.NoErrorf(err, "Query($name=NullValue) got error %v", err) {
			suite.Equalf(0, len(results), "unexpected number of rows returned.")
		}
	}
}

func (suite *DataOpsTestSuite) runTimestampTest(table string, ts interface{}, putShouldSucceed bool) {
	var tsTimeVal time.Time
	var err error
	switch ts.(type) {
	case string:
		strVal := ts.(string)
		tsTimeVal, err = time.Parse(types.ISO8601Layout, strVal)
		suite.Require().NoErrorf(err, "failed to parse %q into time.Time, got error: %v", strVal, err)

	case time.Time:
		tsTimeVal = ts.(time.Time)

	default:
		suite.T().Errorf("unexpected timestamp value, expect string or time.Time, got %T.", ts)
		return
	}

	value := types.NewMapValue(map[string]interface{}{
		"ts0": ts,
		"ts1": ts,
		"ts2": ts,
		"ts3": ts,
		"ts4": ts,
		"ts5": ts,
		"ts6": ts,
		"ts7": ts,
		"ts8": ts,
		"ts9": ts,
	})

	// Put a row.
	putReq := &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	putRes, err := suite.Client.Put(putReq)
	if putShouldSucceed {
		suite.Require().NoErrorf(err, "Put should have succeeded, got error: %v", err)
		suite.NotNil(putRes.Version)
		if test.IsCloud() {
			suite.Greaterf(putRes.WriteKB, 0, "WriteKB should be > 0")
		} else {
			suite.Equalf(0, putRes.WriteKB, "WriteKB should be 0")
		}

	} else {
		suite.Require().Errorf(err, "Put should have failed")
		// Do not proceed if Put fails.
		return
	}

	// Get the row.
	key := types.NewMapValue(map[string]interface{}{
		"ts0": ts,
		"ts9": ts,
	})
	getReq := &nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	}
	getRes, err := suite.Client.Get(getReq)
	// Go's time.Parse() cannot handle negative years, so Get operation would fail.
	if tsTimeVal.Equal(minTimestamp) {
		suite.Require().Errorf(err, "Get should have failed.")
		return
	}

	suite.Require().NoErrorf(err, "Get should have succeeded. got error: %v", err)
	suite.NotNil(getRes.Value, "Get should return a non-nil value.")
	if test.IsCloud() {
		suite.Greaterf(getRes.ReadKB, 0, "ReadKB should be > 0")
	} else {
		suite.Equalf(0, getRes.ReadKB, "ReadKB should be 0")
	}

	var expect time.Time
	fieldNames := [...]string{
		"ts0", "ts1", "ts2", "ts3", "ts4", "ts5", "ts6", "ts7", "ts8", "ts9",
	}
	for i, name := range fieldNames {
		v, _ := getRes.Value.Get(name)
		actual, ok := v.(time.Time)
		if suite.Truef(ok, "unexpected value of %q field, expect time.Time, got %T.", name, v) {
			precision := i
			d := time.Duration(int64(math.Pow10(9 - precision)))
			if tsTimeVal.Equal(maxTimestamp) {
				expect = tsTimeVal.Truncate(time.Nanosecond * d)
			} else {
				expect = tsTimeVal.Round(time.Nanosecond * d)
			}

			suite.Truef(expect.Equal(actual), "unexpect timestamp value returned, expect %v, got %v.", expect, actual)
		}
	}

	// Delete the row
	delReq := &nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	}
	delRes, err := suite.Client.Delete(delReq)
	suite.Require().NoErrorf(err, "delete with key=%v failed, got error %v.", key, err)
	suite.Truef(delRes.Success, "delete with key=%v should have succeeded.", key)
}

func (suite *DataOpsTestSuite) runPutGetDeleteNumericData(table string,
	id int, i int, l int64, f float32, d float64,
	n interface{}, /* string or *big.Rat */
	useJson bool /* indicates whether to parse value from JSON */) {

	var key, value *types.MapValue
	var err error

	strNumVal, isStrNum := n.(string)
	ratNumVal, isRatNum := n.(*big.Rat)
	if !suite.Truef(isStrNum || isRatNum,
		"expect NUMBER value be of string or *big.Rat, got %T", n) {
		return
	}

	// Construct a JSON string with provided values and create a MapValue from
	// the JSON string.
	if useJson {
		floatVerbs := [6]string{"%e", "%E", "%f", "%F", "%g", "%G"}
		verbs := floatVerbs[rand.Intn(6)]

		var fv float64
		if isRatNum {
			if ratNumVal.IsInt() {
				strNumVal = ratNumVal.RatString()
			} else {
				// Convert to float64, may loss precision.
				fv, _ = ratNumVal.Float64()
				strNumVal = fmt.Sprintf("%g", fv)
				// Adjust expected NUMBER value
				ratNumVal = ratNumVal.SetFloat64(fv)
			}
		}

		// A JSON string that represents the key.
		keyStr := fmt.Sprintf(`{"id": %d, "n": "%s"}`, id, strNumVal)

		// A JSON string that represents the value
		valueStr := fmt.Sprintf("{"+
			`"id": %d,`+
			`"i": %d,`+
			`"l": %d,`+
			`"f": `+verbs+","+
			`"d": `+verbs+","+
			`"n": "%s"`+
			"}",
			id, i, l, f, d, strNumVal)

		// Adjust expected float32 and float64 values
		fv, _ = strconv.ParseFloat(fmt.Sprintf(verbs, f), 32)
		f = float32(fv)
		d, _ = strconv.ParseFloat(fmt.Sprintf(verbs, d), 64)

		key, err = types.NewMapValueFromJSON(keyStr)
		suite.NoErrorf(err, "failed to create Key from JSON %q", keyStr)
		value, err = types.NewMapValueFromJSON(valueStr)
		suite.NoErrorf(err, "failed to create Value from JSON %q", valueStr)

	} else {
		key = types.NewMapValue(map[string]interface{}{
			"id": id,
			"n":  n,
		})
		value = types.NewMapValue(map[string]interface{}{
			"id": id,
			"i":  i,
			"l":  l,
			"f":  f,
			"d":  d,
			"n":  n,
		})
	}

	// Put a row.
	putReq := &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
	}
	putRes, err := suite.Client.Put(putReq)
	if !suite.NoErrorf(err, "Put with key=%v failed, got error %v.", key, err) {
		return
	}

	if !suite.NotNilf(putRes.Version, "Put with key=%v should have returned a non-nil version", key) {
		return
	}

	// Get the row
	getReq := &nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	}
	getRes, err := suite.Client.Get(getReq)
	if !suite.NoErrorf(err, "Get with key=%v failed, got error %v.", key, err) {
		return
	}

	intVal, ok := getRes.Value.GetInt("i")
	if suite.Truef(ok, "value of \"i\" should be of int type, got %T", intVal) {
		suite.Equalf(i, intVal, "unexpected value of field \"i\"")
	}

	longVal, ok := getRes.Value.GetInt64("l")
	if suite.Truef(ok, "value of \"l\" should be of int64 type, got %T", longVal) {
		suite.Equalf(l, longVal, "unexpected value of field \"l\"")
	}

	// float32 value is converted to float64 on server
	floatVal, ok := getRes.Value.GetFloat64("f")
	if suite.Truef(ok, "value of \"f\" should be of float64 type, got %T", floatVal) {
		suite.Equalf(float64(f), floatVal, "unexpected value of field \"f\"")
	}

	doubleVal, ok := getRes.Value.GetFloat64("d")
	if suite.Truef(ok, "value of \"d\" should be of float64 type, got %T", doubleVal) {
		if math.IsNaN(d) {
			suite.Truef(math.IsNaN(doubleVal), "expect NaN, got %v", doubleVal)
		} else {
			suite.Equalf(d, doubleVal, "unexpected value of field \"d\"")
		}
	}

	v, _ := getRes.Value.Get("n")
	numVal, ok := v.(*big.Rat)
	if suite.Truef(ok, "value of \"n\" should be of *big.Rat type, got %T", v) {
		if isRatNum {
			suite.checkNumberValue(ratNumVal, numVal)
		} else {
			suite.checkNumberValue(strNumVal, numVal)
		}
	}

	// Delete the row
	delReq := &nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	}
	delRes, err := suite.Client.Delete(delReq)
	if !suite.NoErrorf(err, "Delete with key=%v failed, got error %v.", key, err) {
		return
	}
	suite.Truef(delRes.Success, "Delete with key=%v failed.", key)
}

// Check if the specified values of NUMBER type are equal.
// The specified values should be either *big.Rat or string.
func (suite *DataOpsTestSuite) checkNumberValue(expectNumVal interface{}, actualNumVal interface{}) {
	var expectNum *big.Rat
	var actualNum *big.Rat
	var ok bool

	switch v := expectNumVal.(type) {
	case *big.Rat:
		expectNum = v
	case string:
		expectNum, ok = new(big.Rat).SetString(v)
		if !suite.Truef(ok, "big.Rat.SetString(%q) failed.", v) {
			return
		}
	default:
		suite.T().Errorf("expect *big.Rat or string, got %T", expectNumVal)
		return
	}

	switch v := actualNumVal.(type) {
	case *big.Rat:
		actualNum = v
	case string:
		actualNum, ok = new(big.Rat).SetString(v)
		if !suite.Truef(ok, "big.Rat.SetString(%q) failed.", v) {
			return
		}
	default:
		suite.T().Errorf("expect *big.Rat or string, got %T", actualNumVal)
		return
	}

	if expectNum.Cmp(actualNum) == 0 {
		return
	}

	// Convert to float64 and compare the values.
	expectFloatVal, exact := expectNum.Float64()
	actualFloatVal, _ := actualNum.Float64()
	expectBits := math.Float64bits(expectFloatVal)
	actualBits := math.Float64bits(actualFloatVal)
	if exact {
		suite.Truef(expectBits == actualBits, "expect NUMBER value %v, got %v",
			expectNum, actualNum)
	} else {
		suite.Truef(math.Abs(float64(expectBits-actualBits)) <= 1, "expect NUMBER value %v, got %v",
			expectNum, actualNum)
	}
}

func (suite *DataOpsTestSuite) runPut(tableName, targetField string,
	id int, values []types.FieldValue, expErrCode nosqlerr.ErrorCode) {

	putReq := &nosqldb.PutRequest{
		TableName: tableName,
	}

	for i, v := range values {
		idVal := id + i
		row := &types.MapValue{}
		row.Put("id", idVal)
		row.Put(targetField, v)
		putReq.Value = row
		_, err := suite.Client.Put(putReq)
		switch expErrCode {
		case nosqlerr.NoError:
			suite.NoErrorf(err, "Put with id=%d should have succeeded, got error: %v", idVal, err)

		default:
			suite.Truef(nosqlerr.Is(err, expErrCode),
				"Put with id=%d should have failed with error: %v, got error: %v", idVal, expErrCode, err)
		}
	}
}

func (suite *DataOpsTestSuite) checkPutResult(req *nosqldb.PutRequest, res *nosqldb.PutResult,
	shouldSucceed, rowPresent bool, expPrevValue *types.MapValue,
	expPrevVersion types.Version, recordKB int) {

	if shouldSucceed {
		suite.NotNilf(res.Version, "Put(req=%v) should have succeeded and return a non-nil version.", req)
	} else {
		suite.Nilf(res.Version, "Put(req=%v) should have failed and returned a nil version.", req)
	}

	suite.checkExistingValueVersion(&res.WriteResult, req.ReturnRow, shouldSucceed,
		rowPresent, expPrevValue, expPrevVersion)

	expReadKB, expWriteKB := 0, 0
	if test.IsCloud() {
		expReadKB, expWriteKB = suite.getPutReadWriteCost(req,
			shouldSucceed,
			rowPresent,
			recordKB)
	}

	suite.AssertReadWriteKB(res, expReadKB, expWriteKB, 0 /* prepare cost */, true /* isAbsolute */)
}

func (suite *DataOpsTestSuite) checkGetResult(req *nosqldb.GetRequest, res *nosqldb.GetResult,
	rowPresent, checkFieldOrder bool, expValue *types.MapValue, expVersion types.Version, recordKB int) {

	var expReadKB int
	if rowPresent {
		if expValue != nil {
			suite.Truef(test.CompareMapValue(expValue, res.Value, checkFieldOrder),
				"expect %v, got %v", expValue.Map(), res.Value.Map())
			// Double check field values by specifying insertion order.
			if checkFieldOrder {
				suite.Truef(res.Value.IsOrdered(), "expect an ordered MapValue, got an unordered MapValue")
				for i := 1; i <= expValue.Len(); i++ {
					k1, v1, _ := expValue.GetByIndex(i)
					k2, v2, ok2 := res.Value.GetByIndex(i)
					suite.Equalf(k1, k2, "GetByIndex(%d): expect field name %s, got %s", i, k1, k2)
					if suite.Truef(ok2, "GetByIndex(%d): cannot find the field value") {
						suite.Equalf(v1, v2, "GetByIndex(%d): expect field value %v, got %v", i, v1, v2)
					}
				}

			}
		} else {
			suite.NotNilf(res.Value, "unexpected value")
		}

		if expVersion != nil {
			suite.Equalf(expVersion, res.Version, "unexpected version")
		} else {
			suite.NotNilf(res.Version, "unexpected version")
		}

		expReadKB = recordKB

	} else {
		suite.Nilf(res.Value, "unexpected value")
		suite.Nilf(res.Version, "unexpected version")
		expReadKB = test.MinReadKB
	}

	if test.IsOnPrem() {
		expReadKB = 0
	}
	suite.AssertReadWriteKB(res, expReadKB, 0, 0, req.Consistency == types.Absolute)
}

func (suite *DataOpsTestSuite) checkDeleteResult(req *nosqldb.DeleteRequest, res *nosqldb.DeleteResult,
	shouldSucceed, rowPresent bool, expPrevValue *types.MapValue,
	expPrevVersion types.Version, recordKB int) {

	if shouldSucceed {
		suite.Truef(res.Success, "Delete(req=%v) should have succeeded.", req)
	} else {
		suite.Falsef(res.Success, "Delete(req=%v) should have failed.", req)
	}

	suite.checkExistingValueVersion(&res.WriteResult, req.ReturnRow, shouldSucceed,
		rowPresent, expPrevValue, expPrevVersion)

	expReadKB, expWriteKB := 0, 0
	if test.IsCloud() {
		expReadKB, expWriteKB = suite.getDeleteReadWriteCost(req,
			shouldSucceed,
			rowPresent,
			recordKB)
	}

	suite.AssertReadWriteKB(res, expReadKB, expWriteKB, 0 /* prepare cost */, true /* isAbsolute */)
}

func (suite *DataOpsTestSuite) checkExistingValueVersion(res *nosqldb.WriteResult,
	returnRow, shouldSucceed, rowPresent bool, expPrevValue *types.MapValue,
	expPrevVersion types.Version) {

	hasReturnRow := !shouldSucceed && rowPresent && returnRow
	if hasReturnRow {
		suite.NotNilf(res.ExistingValue, "previous value should have been non-nil")
		if expPrevValue != nil {
			suite.Truef(test.CompareMapValue(expPrevValue, res.ExistingValue, false), "unexpected previous value")
		}

		suite.NotNilf(res.ExistingVersion, "previous version should have been non-nil")
		if expPrevVersion != nil {
			suite.Equalf(expPrevVersion, res.ExistingVersion, "unexpected previous version")
		}

	} else {
		suite.Nilf(res.ExistingValue, "previous value should have been nil")
		suite.Nilf(res.ExistingVersion, "previous version should have been nil")
	}
}

func (suite *DataOpsTestSuite) getPutReadWriteCost(req *nosqldb.PutRequest,
	shouldSucceed, rowPresent bool, recordKB int) (readKB int, writeKB int) {

	minReadKB := test.MinReadKB
	readReturnRow := !shouldSucceed && rowPresent && req.ReturnRow

	switch req.PutOption {
	case types.PutIfAbsent:
		if readReturnRow {
			readKB = recordKB
		} else {
			readKB = minReadKB
		}

		if shouldSucceed {
			writeKB = recordKB
		} else {
			writeKB = 0
		}

	case types.PutIfVersion:
		if readReturnRow {
			readKB = recordKB
		} else {
			readKB = minReadKB
		}

		if shouldSucceed {
			writeKB = recordKB + recordKB // old + new record size
		} else {
			writeKB = 0
		}

	case types.PutIfPresent:
		// PutIfPresent never returns previous row but cost minReadKB for
		// searching existing row
		readKB = minReadKB
		if shouldSucceed {
			writeKB = recordKB + recordKB // old + new record size
		} else {
			writeKB = 0
		}

	default:
		// Put never returns previous row
		readKB = 0
		if shouldSucceed {
			writeKB = recordKB
			if rowPresent {
				writeKB += recordKB
			}
		} else {
			writeKB = 0
		}
	}

	return readKB, writeKB
}

func (suite *DataOpsTestSuite) getDeleteReadWriteCost(req *nosqldb.DeleteRequest,
	shouldSucceed, rowPresent bool, recordKB int) (readKB int, writeKB int) {

	minReadKB := test.MinReadKB

	if req.MatchVersion != nil {
		// The record is present but the version does not match, read cost is
		// recordKB, otherwise minReadKB.
		readReturnRow := !shouldSucceed && rowPresent && req.ReturnRow
		if readReturnRow {
			readKB = recordKB
		} else {
			readKB = minReadKB
		}

		if shouldSucceed {
			writeKB = recordKB
		} else {
			writeKB = 0
		}

	} else {
		// Delete never returns previous row
		readKB = minReadKB
		if shouldSucceed {
			writeKB = recordKB
		} else {
			writeKB = 0
		}
	}

	return readKB, writeKB
}

func genInts(min, max, n int) []int {
	ret := make([]int, n)
	for i := 0; i < n; i++ {
		ret[i] = min + rand.Intn(max-min)
	}
	return ret
}

func genInt64s(min, max int64, n int) []int64 {
	ret := make([]int64, n)
	for i := 0; i < n; i++ {
		ret[i] = min + rand.Int63n(max-min)
	}
	return ret
}

func genFloat32s(min, max float32, n int) []float32 {
	ret := make([]float32, n)
	for i := 0; i < n; i++ {
		ret[i] = min + rand.Float32()*(max-min)
	}
	return ret
}

func genFloat64s(min, max float64, n int) []float64 {
	ret := make([]float64, n)
	for i := 0; i < n; i++ {
		ret[i] = min + rand.Float64()*(max-min)
	}
	return ret
}

func TestDataOperations(t *testing.T) {
	test := &DataOpsTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
