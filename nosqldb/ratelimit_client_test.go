//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// +build cloud

package nosqldb_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// RateLimitClientTestSuite contains tests for Put/Get/Delete APIs.
type RateLimitClientTestSuite struct {
	*test.NoSQLTestSuite
}

// Runs puts and gets continuously for N seconds.
//
// Verify that the resultant RUs/WUs used match the
// given rate limits.
func (suite *RateLimitClientTestSuite) doRateLimitedOps(
	numSeconds, readLimit, writeLimit, maxRows int,
	checkUnits, useExternalLimiters bool,
	usePercent float64, tableName string) {

	if readLimit == 0 && writeLimit == 0 {
		return
	}

	putRequest := &nosqldb.PutRequest{
		TableName: tableName,
	}
	getRequest := &nosqldb.GetRequest{
		TableName: tableName,
	}

	value := &types.MapValue{}
	// TODO: random sizes 0-nKB
	//recordKB := 2
	//name := test.GenString((recordKB - 1) * 1024)
	value.Put("name", "jane")
	key := &types.MapValue{}

	startTime := currentTimeMillis()
	endTime := startTime + int64(numSeconds*1000)

	var readUnitsUsed int
	var writeUnitsUsed int

	var totalDelayTime time.Duration
	var throttleExceptions int

	var rlim common.RateLimiter = nil
	var wlim common.RateLimiter = nil

	maxRVal := float64(readLimit) + float64(writeLimit)

	suite.verbf("Running gets/puts: RUs=%d WUs=%d percent=%.2f", readLimit, writeLimit, usePercent)

	if useExternalLimiters == false {
		// reset internal limiters so they don't have unused units
		suite.Client.ResetRateLimiters(tableName)
	} else {
		rlim = common.NewSimpleRateLimiterWithDuration((float64(readLimit)*usePercent)/100.0, 1)
		wlim = common.NewSimpleRateLimiterWithDuration((float64(writeLimit)*usePercent)/100.0, 1)
	}

	var doPut bool

	for {
		id := rand.Intn(maxRows)
		if readLimit == 0 {
			doPut = true
		} else if writeLimit == 0 {
			doPut = false
		} else {
			doPut = (rand.Intn(int(maxRVal)) >= readLimit)
		}
		if doPut {
			value.Put("id", id)
			putRequest.Value = value
			putRequest.SetReadRateLimiter(nil)
			putRequest.SetWriteRateLimiter(wlim)
			pres, err := suite.Client.Put(putRequest)
			if err == nil && pres != nil {
				used, _ := pres.ConsumedCapacity()
				writeUnitsUsed += used.WriteKB
				totalDelayTime += pres.Delayed().RateLimitTime
			}
			//rs := pres.getRetryStats()
			//if (rs != nil) {
			//throttleExceptions +=
			//rs.getNumExceptions(WriteThrottlingException.class)
			//}
		} else {
			key.Put("id", id)
			getRequest.Key = key
			getRequest.SetReadRateLimiter(rlim)
			getRequest.SetWriteRateLimiter(nil)
			gres, err := suite.Client.Get(getRequest)
			if err == nil && gres != nil {
				used, _ := gres.ConsumedCapacity()
				readUnitsUsed += used.ReadUnits
				totalDelayTime += gres.Delayed().RateLimitTime
			}
			//rs := gres.getRetryStats()
			//if (rs != nil) {
			//throttleExceptions +=
			//rs.getNumExceptions(ReadThrottlingException.class)
			//}
		}
		// we should not get throttling exceptions TODO
		if currentTimeMillis() > endTime {
			break
		}
	}

	numSeconds = int((currentTimeMillis() - startTime) / 1000)

	RUs := readUnitsUsed / numSeconds
	WUs := writeUnitsUsed / numSeconds

	suite.verbf("Resulting RUs=%d and WUs=%d", RUs, WUs)
	suite.verbf("Rate delayed time = %dus", totalDelayTime/time.Microsecond)
	suite.verbf("Internal throttle exceptions = %d", throttleExceptions)

	if checkUnits == false {
		return
	}

	usePercent = usePercent / 100.0

	if RUs < int(float64(readLimit)*usePercent*0.8) ||
		RUs > int(float64(readLimit)*usePercent*1.2) {
		suite.FailNowf("Gets", "Expected around %.1f RUs, got %d", float64(readLimit)*usePercent, RUs)
	}
	if WUs < int(float64(writeLimit)*usePercent*0.8) ||
		WUs > int(float64(writeLimit)*usePercent*1.2) {
		suite.FailNowf("Puts", "Expected around %.1f WUs, got %d", float64(writeLimit)*usePercent, WUs)
	}
}

// Runs queries continuously for N seconds.
//
// Verify that the resultant RUs used match the
// given rate limit.

func (suite *RateLimitClientTestSuite) doRateLimitedQueries(
	numSeconds, readLimit, maxKB int,
	singlePartition, useExternalLimiters bool,
	usePercent float64, tableName string) {

	startTime := currentTimeMillis()
	endTime := startTime + int64(numSeconds*1000)

	var readUnitsUsed int

	// TODO
	//var totalDelayTime time.Duration
	//var throttleExceptions int

	var rlim common.RateLimiter = nil
	var wlim common.RateLimiter = nil

	if useExternalLimiters == false {
		// reset internal limiters so they don't have unused units
		suite.Client.ResetRateLimiters(tableName)
	} else {
		rlim = common.NewSimpleRateLimiterWithDuration((float64(readLimit)*usePercent)/100.0, 1)
		wlim = common.NewSimpleRateLimiterWithDuration((float64(readLimit)*usePercent)/100.0, 1)
	}

	prepReq := &nosqldb.PrepareRequest{}
	if singlePartition {
		// Query based on single partition scanning
		id := rand.Intn(500)
		prepReq.Statement = fmt.Sprintf("select * from %s where id = %d", tableName, id)
	} else {
		// Query based on all partitions scanning
		prepReq.Statement = fmt.Sprintf("select * from %s where name = \"jane\"", tableName)
	}

	prepRes, err := suite.Client.Prepare(prepReq)
	if err != nil {
		suite.FailNowf("Query", "Prepare statement failed: %v", err)
	}
	used, err := prepRes.ConsumedCapacity()
	if err != nil {
		suite.FailNowf("Query", "Prepare statement ConsumedCapacity() returned error: %v", err)
	}
	readUnitsUsed += used.ReadUnits

	suite.verbf("Running queries: RUs=%d percent=%.2f", readLimit, usePercent)

	for {
		// we need a 20 second timeout because in some cases this
		// is called on a table with 500 rows and 50RUs
		// (uses 1000RUs = 20 seconds)
		queryReq := &nosqldb.QueryRequest{
			PreparedStatement: &prepRes.PreparedStatement,
			Timeout:           20000 * time.Millisecond,
			TableName:         tableName,
		}
		if maxKB > 0 {
			// Query with size limit
			queryReq.MaxReadKB = uint(maxKB)
		} else {
			// use readLimit as maxKB
			queryReq.MaxReadKB = uint(float64(readLimit) * usePercent / 100.0)
		}
		if queryReq.MaxReadKB > 2048 {
			queryReq.MaxReadKB = 2048
		}
		queryReq.TableName = tableName
		queryReq.SetReadRateLimiter(rlim)
		queryReq.SetWriteRateLimiter(wlim)
		for {
			res, err := suite.Client.Query(queryReq)
			if err != nil {
				if nosqlerr.Is(err, nosqlerr.ReadLimitExceeded, nosqlerr.WriteLimitExceeded) {
					suite.FailNow("Expected no throttling exceptions, got one")
				}
				suite.FailNowf("Query", "Got unexpected error running query with readLimit=%d maxKB=%d singlePart=%v usePercent=%.2f%%: %s", readLimit, maxKB, singlePartition, usePercent, err.Error())
			} else {
				used, _ := res.ConsumedCapacity()
				if used.ReadUnits == 0 {
					suite.FailNow("Query used zero read units")
				}
				readUnitsUsed += used.ReadUnits
			}
			if queryReq.IsDone() {
				break
			}
			if currentTimeMillis() > endTime {
				break
			}
		}
		if currentTimeMillis() > endTime {
			break
		}
	}

	numSeconds = int((currentTimeMillis() - startTime) / 1000)

	usePercent = usePercent / 100.0

	RUs := readUnitsUsed / numSeconds

	suite.verbf("Resulting query RUs=%d", RUs)

	expectedRUs := int(float64(readLimit) * usePercent)

	// for very small expected amounts, just verify within 1 RU
	if expectedRUs < 4 &&
		RUs <= (expectedRUs+1) &&
		RUs >= (expectedRUs-1) {
		return
	}

	if RUs < (int)(float64(expectedRUs)*0.6) ||
		RUs > (int)(float64(expectedRUs)*1.5) {
		suite.FailNowf("Queries", "Expected around %d RUs, got %d", expectedRUs, RUs)
	}
}

func (suite *RateLimitClientTestSuite) setLimitsOnTable(
	readLimit, writeLimit int,
	tableName string) error {

	req := &nosqldb.TableRequest{
		TableLimits: &nosqldb.TableLimits{
			ReadUnits:  uint(readLimit),
			WriteUnits: uint(writeLimit),
			StorageGB:  5,
		},
		TableName: tableName,
		Timeout:   3 * time.Second,
	}
	_, err := suite.Client.DoTableRequestAndWait(req, 5*time.Second, 1*time.Second)
	return err
}

func (suite *RateLimitClientTestSuite) ensureTableExistsWithLimits(
	readLimit, writeLimit int,
	tableName string) {

	// first try to just set limits on table
	err := suite.setLimitsOnTable(readLimit, writeLimit, tableName)
	if err == nil {
		return
	}

	// if that fails, try to create table with limits
	suite.createAndPopulateTable(tableName)

	err = suite.setLimitsOnTable(readLimit, writeLimit, tableName)
	suite.NoError(err, "Failed to set limits on table table")
}

func (suite *RateLimitClientTestSuite) createAndPopulateTable(tableName string) {

	limits := &nosqldb.TableLimits{
		ReadUnits:  50000,
		WriteUnits: 50000,
		StorageGB:  5,
	}
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, name STRING, PRIMARY KEY(id))", tableName)
	suite.ReCreateTable(tableName, stmt, limits)

	// fill table with data
	suite.doRateLimitedOps(
		5,            // seconds
		50000, 50000, // r/w limits
		5000,  // maxRows
		false, // don't check resulting rate
		false, // use internal limiting
		100.0, // usePercent
		tableName)
}

// Runs get/puts then queries on a table.
// Verify RUs/WUs are within given limits.
func (suite *RateLimitClientTestSuite) runLimitedOpsOnTable(
	readLimit, writeLimit, maxSeconds, maxRows int,
	useExternalLimiters bool,
	usePercent float64, tableName string) {

	// TODO: test large versus small records

	suite.verbf("Running rate limiting test: RUs=%d WUs=%d usePercent=%.2f external=%v",
		readLimit, writeLimit, usePercent, useExternalLimiters)

	suite.ensureTableExistsWithLimits(readLimit, writeLimit, tableName)

	// we have to do the read/write ops separately since we're
	// running single-threaded, and the result is hard to tell
	// if it's correct (example: for a limit of 50 we may get 37RUs
	// and 13WUs)

	suite.doRateLimitedOps(maxSeconds, 0, writeLimit,
		maxRows, true, useExternalLimiters, usePercent, tableName)
	suite.doRateLimitedOps(maxSeconds, readLimit, 0,
		maxRows, true, useExternalLimiters, usePercent, tableName)

	// Query based on single partition scanning
	suite.doRateLimitedQueries(maxSeconds, readLimit,
		20, true, useExternalLimiters, usePercent, tableName)
	// Query based on all partitions scanning
	suite.doRateLimitedQueries(maxSeconds, readLimit,
		20, false, useExternalLimiters, usePercent, tableName)
	// Query based on all partitions scanning, no limit per req
	suite.doRateLimitedQueries(maxSeconds, readLimit,
		0, false, useExternalLimiters, usePercent, tableName)
}

func (suite *RateLimitClientTestSuite) TestInternalFull() {
	suite.testLimiters(false, 500, 200, 200, 10, 100.0, "RLTest")
}

func (suite *RateLimitClientTestSuite) TestExternalFull() {
	suite.testLimiters(true, 500, 200, 200, 10, 100.0, "RLTest")
}

func (suite *RateLimitClientTestSuite) TestInternalPercent() {
	suite.testLimiters(false, 500, 200, 200, 10, 20.0, "RLTest")
}

func (suite *RateLimitClientTestSuite) TestExternalPercent() {
	suite.testLimiters(true, 500, 200, 200, 10, 20.0, "RLTest")
}

func (suite *RateLimitClientTestSuite) testLimiters(
	useExternalLimiters bool,
	maxRows, readLimit, writeLimit, testSeconds int,
	usePercent float64, tableName string) {

	// clear any previous rate limiters
	suite.Client.EnableRateLimiting(false, usePercent)

	// configure our handle for rate limiting
	if useExternalLimiters == false {
		suite.Client.EnableRateLimiting(true, usePercent)
	}

	// limit bursts in tests
	// TODO
	//System.setProperty("test.rldurationsecs", "1")

	// then do the actual testing
	suite.runLimitedOpsOnTable(readLimit, writeLimit, testSeconds,
		maxRows, useExternalLimiters, usePercent, tableName)
}

func (suite *RateLimitClientTestSuite) TestExtendedInternalFull() {
	// Skip unless extended tests are enabled
	if suite.RunExtended == false {
		suite.T().Skip("TestExtendedInternalFull() only runs with RunExtended=true")
	}

	allunits := [3]int{1, 50, 2000}
	for _, units := range allunits {
		suite.testLimiters(false, 500, units, units, 10, 100.0, "RLInternalFull")
	}
}

func (suite *RateLimitClientTestSuite) TestExtendedInternalPercent() {
	// Skip unless extended tests are enabled
	if suite.RunExtended == false {
		suite.T().Skip("TestExtendedInternalPercent() only runs with RunExtended=true")
	}

	allunits := [3]int{10, 100, 2000}
	for _, units := range allunits {
		suite.testLimiters(false, 500, units, units, 10, 10.0, "RLInternalPercent")
	}
}

func (suite *RateLimitClientTestSuite) TestExtendedExternalFull() {
	// Skip unless extended tests are enabled
	if suite.RunExtended == false {
		suite.T().Skip("TestExtendedExternalFull() only runs with RunExtended=true")
	}

	allunits := [3]int{1, 50, 2000}
	for _, units := range allunits {
		suite.testLimiters(true, 500, units, units, 10, 100.0, "RLExternalFull")
	}
}

func (suite *RateLimitClientTestSuite) TestExtendedExternalPercent() {
	// Skip unless extended tests are enabled
	if suite.RunExtended == false {
		suite.T().Skip("TestExtendedExternalPercent() only runs with RunExtended=true")
	}

	allunits := [3]int{10, 100, 2000}
	for _, units := range allunits {
		suite.testLimiters(true, 500, units, units, 10, 10.0, "RLExternalPercent")
	}
}

func (suite *RateLimitClientTestSuite) verb(str string) {
	if suite.Verbose {
		fmt.Println(str)
	}
}

func (suite *RateLimitClientTestSuite) verbf(messageFormat string, messageArgs ...interface{}) {
	if suite.Verbose == false {
		return
	}
	fmt.Printf(messageFormat, messageArgs...)
	fmt.Println("")
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func TestRateLimitClient(t *testing.T) {
	test := &RateLimitClientTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
