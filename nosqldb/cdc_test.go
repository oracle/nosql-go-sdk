//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

//go:build cloud
// +build cloud

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

// CDCTestSuite contains tests for CDC APIs.
type CDCTestSuite struct {
	*test.NoSQLTestSuite
}

// Test Put/Get/Delete APIs with different operation options.
func (suite *CDCTestSuite) TestPutGetDelete() {
    if suite.IsOnPrem() {
        suite.T().Skip("Skipping CDC tests in onprem mode")
    }
	var stmt string
	var err error
	//table := suite.GetTableName("TestUsers")
	// temporary: use full ocid for tablename to avoid kv<-->cdc<-->proxy mismatch
	table := "ocid1_nosqltable_cloudsim_GoTestUsers"
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

	var consumer *nosqldb.ChangeConsumer = nil

	// Enable CDC on the new table
	// wait a few seconds for producer to notice the table
	time.Sleep(3 * time.Second)
	tableReq := &nosqldb.TableRequest{
		TableName:  table,
		CDCConfig: &nosqldb.TableCDCConfig{Enabled: true},
	}
	_, err = suite.Client.DoTableRequest(tableReq)
	if err != nil {
		if nosqlerr.Is(err, nosqlerr.OperationNotSupported) {
			suite.T().Skipf("Skipping CDC: %v", err)
		} else {
			suite.Require().NoErrorf(err, "failed to enable CDC streaming on table %s: %v", table, err)
		}
	} else {
		// create a CDC consumer that reads this table.
		config := suite.Client.CreateChangeConsumerConfig().
			AddTable(table, "", nosqldb.Latest, nil).
			GroupID("test_group").
			CommitAutomatic()
		consumer, err = suite.Client.CreateChangeConsumer(config)
		if err != nil {
			suite.Require().NoErrorf(err, "failed to create CDC consumer: %v", err)
		}
		// Give the subscriber a few seconds to start
		time.Sleep(3 * time.Second)
	}

	var putReq *nosqldb.PutRequest
	var putRes *nosqldb.PutResult
	var curVersion, oldVersion, ifVersion, newVersion types.Version
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
			false, // returnPrevRow
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
		curVersion = putRes.Version
	}

	// Put row again with ReturnRow=true
	// If server serial version > V4, expect previous row data returned.
	// Otherwise no row data should be returned.
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			suite.Client.GetServerSerialVersion() > 4, // returnPrevRow
			value,  // expPrevValue
			curVersion,  // expPrevVersion
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
			false, // returnPrevRow
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
			true, // returnPrevRow
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
			false, // returnPrevRow
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
		oldVersion = putRes.Version
	}

	// PutIfPresent succeed + ReturnRow=true
	// If server serial version > V4, expect previous row data returned.
	// Otherwise no row data should be returned.
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			suite.Client.GetServerSerialVersion() > 4, // returnPrevRow
			value,  // expPrevValue
			oldVersion,  // expPrevVersion
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
			false, // returnPrevRow
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfPresent fail + ReturnRow=true, expect no previous row
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false, // shouldSucceed
			false, // rowPresent
			false, // returnPrevRow
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
			false, // returnPrevRow
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
			false, // returnPrevRow
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// PutIfVersion fails + ReturnRow=true
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			false,     // shouldSucceed
			true,      // rowPresent
			true,      // returnPrevRow
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
			false, // returnPrevRow
			nil,  // expPrevValue
			nil,  // expPrevVersion
			recordKB)
		ifVersion = putRes.Version
	}

	// PutIfVersion succeed + ReturnRow=true
	putReq.MatchVersion = ifVersion
	putReq.ReturnRow = true
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true, // shouldSucceed
			true, // rowPresent
			false, // returnPrevRow
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

	// Get the version of the row using a query. Validate that the
	// version returned matches the version returned from the previous get.
	stmt = fmt.Sprintf("select row_version($t) as version from %s $t where id = 10", table)
	qReq := &nosqldb.QueryRequest{
		Statement:   stmt,
		Consistency: types.Absolute,
	}
	qRes, err := suite.Client.Query(qReq)
	results, rerr := qRes.GetResults()
	ver, _ := results[0].GetBinary("version")
	if suite.NoError(err) && suite.NoError(rerr) {
		suite.checkGetResult(getReq, getRes,
			true,               // rowPresent
			true,               // checkFieldOrder
			nil,                // expected value
			types.Version(ver), // expected version
			recordKB)
	}

	// Update the row with a PutIfVersion using the version returned by the
	// row_version query. This should succeed.
	putReq = &nosqldb.PutRequest{
		TableName:    table,
		Value:        value,
		PutOption:    types.PutIfVersion,
		MatchVersion: types.Version(ver),
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoError(err) {
		suite.checkPutResult(putReq, putRes,
			true,  // shouldSucceed
			false, // rowPresent
			false, // returnPrevRow
			nil,   // expPrevValue
			nil,   // expPrevVersion
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
			false, // shouldReturnPrev
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

	// Delete succeed + ReturnRow=true
	delReq.ReturnRow = true
	delRes, err = suite.Client.Delete(delReq)
	if suite.NoError(err) {
		suite.checkDeleteResult(delReq, delRes,
			true, // shouldSucceed
			true, // rowPresent
			suite.Client.GetServerSerialVersion() > 4, // returnPrevRow
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
			false, // shouldReturnPrev
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
			false, // returnPrevRow
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
			true,      // returnPrevRow
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
			false, // returnPrevRow
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
			false, // returnPrevRow
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
			false, // returnPrevRow
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
			false, // shouldReturnPrev
			nil,   // expPrevValue
			nil,   // expPrevVersion
			recordKB)
	}

	// Give the replication stream a couple seconds to catch up
	time.Sleep(3 * time.Second)

	numEvents := 0
	for i:=0; i<10; i++ {
		bundle, err := consumer.Poll(2, time.Duration(1*time.Second))
		suite.Require().NoErrorf(err, "Poll returned error: %v", err)
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.
		if bundle == nil || len(bundle.Messages) == 0 {
			//fmt.Printf("Received empty message: sleeping for one second\n")
			time.Sleep(100 * time.Millisecond);
		} else {
			fmt.Printf("Received message: %v\n", *bundle)
			for _, msg := range bundle.Messages {
				numEvents += len(msg.Events)
			}
		}
	}

	// TODO: check for exact number of CDC events (15?)
	fmt.Printf("Received %d CDC events\n", numEvents)
	suite.Require().GreaterOrEqual(numEvents, 15, "expected at least 15 CDC events")

	// Do another put/delete cycle, check events again
	// Put the row back to store
	putReq = &nosqldb.PutRequest{TableName: table, Value: value}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err)

	key.Put("id", 10)
	delReq = &nosqldb.DeleteRequest{TableName: table, Key: key}
	delRes, err = suite.Client.Delete(delReq)
	suite.Require().NoError(err)


	// Give the replication stream a couple seconds to catch up
	time.Sleep(2 * time.Second)

	numEvents = 0
	for i:=0; i<10; i++ {
		bundle, err := consumer.Poll(2, time.Duration(1*time.Second))
		suite.Require().NoErrorf(err, "Poll returned error: %v", err)
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.
		if bundle == nil || len(bundle.Messages) == 0 {
			//fmt.Printf("Received empty message: sleeping for one second\n")
			time.Sleep(100 * time.Millisecond);
		} else {
			fmt.Printf("Received message: %v\n", *bundle)
			for _, msg := range bundle.Messages {
				numEvents += len(msg.Events)
			}
		}
	}

	fmt.Printf("Received %d CDC events\n", numEvents)
	suite.Require().GreaterOrEqual(numEvents, 2, "expected at least 2 more CDC events")
}

func (suite *CDCTestSuite) checkPutResult(req *nosqldb.PutRequest, res *nosqldb.PutResult,
	shouldSucceed, rowPresent, shouldReturnPrev bool, expPrevValue *types.MapValue,
	expPrevVersion types.Version, recordKB int) {

	if shouldSucceed {
		suite.NotNilf(res.Version, "Put(req=%v) should have succeeded and return a non-nil version.", req)
	} else {
		suite.Nilf(res.Version, "Put(req=%v) should have failed and returned a nil version.", req)
	}

	suite.checkExistingValueVersion(&res.WriteResult, shouldReturnPrev, expPrevValue, expPrevVersion)

	expReadKB, expWriteKB := 0, 0
	if test.IsCloud() {
		expReadKB, expWriteKB = suite.getPutReadWriteCost(req,
			shouldSucceed,
			rowPresent,
			recordKB)
	}

	suite.AssertReadWriteKB(res, expReadKB, expWriteKB, 0 /* prepare cost */, true /* isAbsolute */)
}

func (suite *CDCTestSuite) checkGetResult(req *nosqldb.GetRequest, res *nosqldb.GetResult,
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
			suite.Truef(test.VersionsEqual(expVersion, res.Version), "unexpected version")
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

func (suite *CDCTestSuite) checkDeleteResult(req *nosqldb.DeleteRequest, res *nosqldb.DeleteResult,
	shouldSucceed, rowPresent, shouldReturnPrev bool, expPrevValue *types.MapValue,
	expPrevVersion types.Version, recordKB int) {

	if shouldSucceed {
		suite.Truef(res.Success, "Delete(req=%v) should have succeeded.", req)
	} else {
		suite.Falsef(res.Success, "Delete(req=%v) should have failed.", req)
	}

	suite.checkExistingValueVersion(&res.WriteResult, shouldReturnPrev, expPrevValue, expPrevVersion)

	expReadKB, expWriteKB := 0, 0
	if test.IsCloud() {
		expReadKB, expWriteKB = suite.getDeleteReadWriteCost(req,
			shouldSucceed,
			rowPresent,
			recordKB)
	}

	suite.AssertReadWriteKB(res, expReadKB, expWriteKB, 0 /* prepare cost */, true /* isAbsolute */)
}

func (suite *CDCTestSuite) checkExistingValueVersion(res *nosqldb.WriteResult,
	shouldReturnPrev bool, expPrevValue *types.MapValue,
	expPrevVersion types.Version) {

	if shouldReturnPrev {
		suite.NotNilf(res.ExistingValue, "previous value should have been non-nil")
		if expPrevValue != nil {
			suite.Truef(test.CompareMapValue(expPrevValue, res.ExistingValue, false), "unexpected previous value")
		}

		suite.NotNilf(res.ExistingVersion, "previous version should have been non-nil")
		if expPrevVersion != nil {
			suite.Truef(test.VersionsEqual(expPrevVersion, res.ExistingVersion), "unexpected previous version")
		}

	} else {
		suite.Nilf(res.ExistingValue, "previous value should have been nil")
		suite.Nilf(res.ExistingVersion, "previous version should have been nil")
	}
}

func (suite *CDCTestSuite) getPutReadWriteCost(req *nosqldb.PutRequest,
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
		// PutIfPresent may return previous row, or cost minReadKB for
		// searching existing row
		readKB = minReadKB
		if suite.Client.GetServerSerialVersion() > 4 && req.ReturnRow && rowPresent {
			readKB = recordKB
		}
		if shouldSucceed {
			writeKB = recordKB + recordKB // old + new record size
		} else {
			writeKB = 0
		}

	default:
		// Put might return previous row
		readKB = 0
		if suite.Client.GetServerSerialVersion() > 4 && req.ReturnRow && rowPresent {
			readKB = recordKB
		}
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

func (suite *CDCTestSuite) getDeleteReadWriteCost(req *nosqldb.DeleteRequest,
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
		// Delete returns previous only if ReturnRow is true and server protocol > 4
		if suite.Client.GetServerSerialVersion() > 4 && req.ReturnRow && rowPresent {
			readKB = recordKB
		} else {
			readKB = minReadKB
		}
		if shouldSucceed {
			writeKB = recordKB
		} else {
			writeKB = 0
		}
	}

	return readKB, writeKB
}

func TestCDCOperations(t *testing.T) {
	test := &CDCTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
