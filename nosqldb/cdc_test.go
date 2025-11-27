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
	table := suite.GetTableName("PutGetDelete")
	// temporary: use full ocid for tablename to avoid kv<-->cdc<-->proxy mismatch
	//table := "ocid1_nosqltable_cloudsim_GoTestUsers"
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
	groupID := "putGetDeleteGroup"

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
	}
	// create a CDC consumer that reads this table.
	config := suite.Client.CreateChangeConsumerConfig().
		AddTable(table, "", nosqldb.Latest, nil).
		GroupID(groupID).
		CommitAutomatic()
	consumer, err = suite.Client.CreateChangeConsumer(config)
	if err != nil {
		suite.Require().NoErrorf(err, "failed to create CDC consumer: %v", err)
	}
	cloudsimCDC := isCloudsimCDC(consumer)
	defer func() {
		err := consumer.Close()
		if err != nil {
			fmt.Printf("ERROR: closing consumer generated error: %v\n", err)
		}
		err = suite.Client.DeleteChangeConsumerGroup(groupID, "")
		if err != nil {
			fmt.Printf("ERROR: deleting consumer group generated error: %v\n", err)
		}
	}()
	// Give the subscriber a few seconds to start
	time.Sleep(3 * time.Second)

	// keep track of how many CDC events these operations should generate
	// NOTE: Cloudsim using internal CDC test client will generate more events,
	// because it doesn't manage ifVersion properly.
	expectedEvents := 0

	var putReq *nosqldb.PutRequest
	var oldVersion, ifVersion types.Version
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
	putRes, err := suite.Client.Put(putReq)
	suite.Require().NoError(err, "Put failed")
	oldVersion = putRes.Version
	expectedEvents++

	// Put row again. Even though it's the same, it should
	// generate a new CDC event.
	putReq.ReturnRow = true
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "Put failed")
	expectedEvents++

	// PutIfAbsent an existing row, it should not create an event
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
		PutOption: types.PutIfAbsent,
	}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "PutIfAbsent failed")
	if cloudsimCDC { expectedEvents++ }

	// PutIfPresent an existing row, it should succeed
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     value,
		PutOption: types.PutIfPresent,
	}
	putRes, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "PutIfPresent failed")
	ifVersion = putRes.Version
	expectedEvents++

	// PutIfPresent an new row, it should not generate an event
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     newValue,
		PutOption: types.PutIfPresent,
	}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "PutIfPresent failed")
	if cloudsimCDC { expectedEvents++ }

	// PutIfAbsent an new row, it should succeed
	putReq = &nosqldb.PutRequest{
		TableName: table,
		Value:     newValue,
		PutOption: types.PutIfAbsent,
	}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "PutIfAbsent failed")
	expectedEvents++

	// PutIfVersion an existing row with unmatched version, it should fail.
	putReq = &nosqldb.PutRequest{
		TableName:    table,
		Value:        value,
		PutOption:    types.PutIfVersion,
		MatchVersion: oldVersion,
	}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "PutIfVersion failed")
	if cloudsimCDC { expectedEvents++ }

	// Put an existing row with matching version, it should succeed.
	putReq = &nosqldb.PutRequest{
		TableName:    table,
		Value:        value,
		PutOption:    types.PutIfVersion,
		MatchVersion: ifVersion,
	}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "PutIfVersion failed")
	expectedEvents++

	// Test Get operation: should not generate an event
	key := types.ToMapValue("id", 10)
	getReq := &nosqldb.GetRequest{
		TableName: table,
		Key:       key,
	}
	_, err = suite.Client.Get(getReq)
	suite.Require().NoError(err, "Get failed")
	//expectedEvents++

	// TODO: Insert row with a query. Should generate an event.

	// Delete a row: should genrate an event
	key.Put("id", 10)
	delReq := &nosqldb.DeleteRequest{
		TableName: table,
		Key:       key,
	}
	_, err = suite.Client.Delete(delReq)
	suite.Require().NoError(err, "Delete failed")
	expectedEvents++

	// Delete again, fail: should not generate event
	_, err = suite.Client.Delete(delReq)
	suite.Require().NoError(err, "Delete failed")
	if cloudsimCDC { expectedEvents++ }

	// Give the replication stream a couple seconds to catch up
	time.Sleep(3 * time.Second)

	numEvents := 0
	for i:=0; i<10; i++ {
		bundle, err := consumer.Poll(2, time.Duration(1*time.Second))
		suite.Require().NoErrorf(err, "Poll returned error: %v", err)
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.
		if bundle == nil || len(bundle.Messages) == 0 {
			//fmt.Printf("Received empty message: sleeping for 100ms\n")
			time.Sleep(100 * time.Millisecond);
		} else {
			//fmt.Printf("Received message: %v\n", *bundle)
			for _, msg := range bundle.Messages {
				numEvents += len(msg.Events)
				fmt.Printf("Received message: ocid=%s\n", msg.TableOCID)
				for _, ev := range msg.Events {
					for _, rec := range ev.Records {
						fmt.Printf("   Received record: key=%v  cur=%v\n", *rec.RecordKey, rec.CurrentImage)
					}
				}
			}
		}
	}

	// check for exact number of CDC events
	fmt.Printf("Received %d CDC events\n", numEvents)
	suite.Require().Equal(expectedEvents, numEvents, "Did not get expected number of events")

	// Do another put/delete cycle, check events again
	// Put the row back to store
	putReq = &nosqldb.PutRequest{TableName: table, Value: value}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err)

	key.Put("id", 10)
	delReq = &nosqldb.DeleteRequest{TableName: table, Key: key}
	_, err = suite.Client.Delete(delReq)
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
	suite.Require().Equal(2, numEvents, "expected 2 more CDC events")
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


func (suite *CDCTestSuite) pollAndCheckEvent(consumer *nosqldb.ChangeConsumer,
                                             tableOCID string,
                                             expKey *types.MapValue,
                                             expValue *types.MapValue) {
	// Poll until we get this event back, then verify the returned CDC event matches
	// the record that was written.
	bundle, err := consumer.Poll(1, time.Duration(10*time.Second))
	suite.Require().NoErrorf(err, "Poll returned error: %v", err)
	if bundle == nil || len(bundle.Messages) == 0 {
		suite.Require().Fail("Poll returned no results after 10 seconds")
	}
	fmt.Printf("Received bundle: %v\n", *bundle)
	if len(bundle.Messages) != 1 {
		suite.Require().Fail("Poll returned %d messages, expected 1", len(bundle.Messages))
	}
	message := bundle.Messages[0]
	fmt.Printf(" message: %v\n", *message)
	if len(message.Events) != 1 {
		suite.Require().Fail("Poll returned %d events, expected 1", len(message.Events))
	}
	// TODO: check message.TableName against table name (not OCID)
	// TODO: check message.CompartmentOCID when using a different compartment

	suite.Require().Equal(message.TableOCID, tableOCID, "Table OCID mismatch")
	event := message.Events[0];
	fmt.Printf("    event: %v\n", *event)
	if len(event.Records) != 1 {
		suite.Require().Fail("Event contained %d records, expected 1", len(event.Records))
	}
	record := event.Records[0]
	fmt.Printf("      record: %v\n", record)

	// TODO: check EventID?

	suite.Require().Equal(expKey, record.RecordKey, "record keys do not match")
	if expValue != nil {
		suite.Require().NotNil(record.CurrentImage, "Current image must not be nil")
		suite.Require().Equal(expValue, record.CurrentImage.RecordValue, "Values do not match")
		// TODO record.CurrentImage.RecordMetadata
	} else {
		suite.Require().Nil(record.CurrentImage, "Current image should be nil")
	}

	// TODO: beforeImage testing
	//suite.Require().Nil(record.BeforeImage)

	// TODO record.ModificationTime time.Time
	// TODO record.ExpirationTime time.Time
	// TODO record.PartitionID int
	// TODO record.RegionID int
}

func (suite *CDCTestSuite) createBasicTable(tableName string) {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"name STRING, "+
		"PRIMARY KEY(id))", tableName)
	limits := &nosqldb.TableLimits{
		ReadUnits:  500,
		WriteUnits: 500,
		StorageGB:  50,
	}
	suite.ReCreateTable(tableName, stmt, limits)
}

// Test that every put/delete/insert/update generates an
// event that matches the operation
func (suite *CDCTestSuite) TestEcho() {
	if suite.IsOnPrem() {
	    suite.T().Skip("Skipping CDC tests in onprem mode")
	}
	table := suite.GetTableName("Echo")
	// temporary: use full ocid for tablename to avoid kv<-->cdc<-->proxy mismatch
	// "ocid1_nosqltable_cloudsim_" is a hardcoded ocid prefix in cloudsim
	//table := "ocid1_nosqltable_cloudsim_GoTestUsers"
	suite.createBasicTable(table)

	// Enable CDC on the new table
	err := suite.Client.EnableChangeDataCapture(table, "")
	if err != nil {
		if nosqlerr.Is(err, nosqlerr.OperationNotSupported) {
			suite.T().Skipf("Skipping CDC: %v", err)
		} else {
			suite.Require().Fail("failed to enable CDC streaming on table %s: %v", table, err)
		}
	}

	// wait a few seconds for producer to notice the table
	time.Sleep(3 * time.Second)

	// create a CDC consumer that reads this table.
	var consumer *nosqldb.ChangeConsumer = nil
	groupID := "echoGroup"

	config := suite.Client.CreateChangeConsumerConfig().
		AddTable(table, "", nosqldb.Latest, nil).
		GroupID(groupID).
		CommitAutomatic()
	consumer, err = suite.Client.CreateChangeConsumer(config)
	if err != nil {
		suite.Require().Fail("failed to create CDC consumer: %v", err)
	}

	defer func() {
		err := consumer.Close()
		if err != nil {
			fmt.Printf("ERROR: closing consumer generated error: %v\n", err)
		}
		err = suite.Client.DeleteChangeConsumerGroup(groupID, "")
		if err != nil {
			fmt.Printf("ERROR: deleting consumer group generated error: %v\n", err)
		}
	}()

	// Give the subscriber a few seconds to start
	time.Sleep(3 * time.Second)


	name := test.GenString(100)
	value := types.NewOrderedMapValue().Put("id", 10).Put("name", name)

	// Put a row.
	putReq := &nosqldb.PutRequest{TableName: table, Value: value}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "Put failed")

	// Poll until we get this event back, then verify the returned CDC event matches
	// the record that was written.
	// CDC returns record values that do NOT have the key fields in them...
	expKey := types.NewOrderedMapValue().Put("id", 10)
	expValue := types.NewOrderedMapValue().Put("name", name)
	suite.pollAndCheckEvent(consumer, table, expKey, expValue)

	// Put row again. Even though it's the same, it should
	// generate a new CDC event.
	putReq = &nosqldb.PutRequest{TableName: table, Value: value}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "Put failed")
	// TODO: this time it should have a beforeImage
	suite.pollAndCheckEvent(consumer, table, expKey, expValue)

	// Now delete the row. We should get a new delete event.
	delReq := &nosqldb.DeleteRequest{TableName: table, Key: expKey}
	_, err = suite.Client.Delete(delReq)
	suite.Require().NoError(err, "Delete failed")
	// TODO: this time it should have a beforeImage
	suite.pollAndCheckEvent(consumer, table, expKey, nil)

	//table2 := "ocid1_nosqltable_cloudsim_GoTestUsers2"
	table2 := suite.GetTableName("Echo2")
	suite.createBasicTable(table2)

	// Enable CDC on the new table
	err = suite.Client.EnableChangeDataCapture(table2, "")
	if err != nil {
		suite.Require().Fail("failed to enable CDC streaming on table %s: %v", table, err)
	}
	err = consumer.AddTable(table2, "", nosqldb.Latest, nil)
	suite.Require().NoError(err, "Adding second table failed")
}

// consumers may have metadata associated with them for debugging/testing purposes.
// One value indicates if the CDC operations are limited due to using CloudSim.
// Note this is independent of the "cloudsim" test mode, because that can use
// either internal proxy cloudsim CDC client, or the real cloud CDC client.
func isCloudsimCDC(consumer *nosqldb.ChangeConsumer) bool {
	if consumer.Metadata == nil {
		return false
	}
	if val, ok := consumer.Metadata.GetString("cloudsim"); ok {
		if val != "" {
			return true
		}
	}
	return false
}


// Test functionality that currently should pass.
func (suite *CDCTestSuite) TestMinimalPassing() {
	if suite.IsOnPrem() {
	    suite.T().Skip("Skipping CDC tests in onprem mode")
	}
	table := suite.GetTableName("MinimalPassing")
	// temporary: use full ocid for tablename to avoid kv<-->cdc<-->proxy mismatch
	// "ocid1_nosqltable_cloudsim_" is a hardcoded ocid prefix in cloudsim
	//table := "ocid1_nosqltable_cloudsim_GoTestUsers"
	suite.createBasicTable(table)

	// Enable CDC on the new table
	err := suite.Client.EnableChangeDataCapture(table, "")
	if err != nil {
		if nosqlerr.Is(err, nosqlerr.OperationNotSupported) {
			suite.T().Skipf("Skipping CDC: %v", err)
		} else {
			suite.Require().Fail("failed to enable CDC streaming on table %s: %v", table, err)
		}
	}

	// wait a few seconds for producer to notice the table
	time.Sleep(3 * time.Second)

	// create a CDC consumer that reads this table.
	var consumer *nosqldb.ChangeConsumer = nil
	groupID := "echoGroup"

	config := suite.Client.CreateChangeConsumerConfig().
		AddTable(table, "", nosqldb.Latest, nil).
		GroupID(groupID).
		CommitAutomatic()
	consumer, err = suite.Client.CreateChangeConsumer(config)
	if err != nil {
		suite.Require().Fail("failed to create CDC consumer: %v", err)
	}

	defer func() {
		err := consumer.Close()
		if err != nil {
			fmt.Printf("ERROR: closing consumer generated error: %v\n", err)
		}
		err = suite.Client.DeleteChangeConsumerGroup(groupID, "")
		if err != nil {
			fmt.Printf("ERROR: deleting consumer group generated error: %v\n", err)
		}
	}()

	// Give the subscriber a few seconds to start
	time.Sleep(3 * time.Second)


	name := test.GenString(100)
	value := types.NewOrderedMapValue().Put("id", 10).Put("name", name)

	// Put a row.
	putReq := &nosqldb.PutRequest{TableName: table, Value: value}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "Put failed")

	// Poll until we get this event back, then verify the returned CDC event matches
	// the record that was written.
	// CDC returns record values that do NOT have the key fields in them...
	expKey := types.NewOrderedMapValue().Put("id", 10)
	expValue := types.NewOrderedMapValue().Put("name", name)
	suite.pollAndCheckEvent(consumer, table, expKey, expValue)

	// Put row again. Even though it's the same, it should
	// generate a new CDC event.
	putReq = &nosqldb.PutRequest{TableName: table, Value: value}
	_, err = suite.Client.Put(putReq)
	suite.Require().NoError(err, "Put failed")
	// TODO: this time it should have a beforeImage
	suite.pollAndCheckEvent(consumer, table, expKey, expValue)

	// Now delete the row. We should get a new delete event.
	delReq := &nosqldb.DeleteRequest{TableName: table, Key: expKey}
	_, err = suite.Client.Delete(delReq)
	suite.Require().NoError(err, "Delete failed")
	// TODO: this time it should have a beforeImage
	suite.pollAndCheckEvent(consumer, table, expKey, nil)
}

func TestCDCOperations(t *testing.T) {
	test := &CDCTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
