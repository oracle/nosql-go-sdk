//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// +build cloud onprem

package nosqldb_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// QueryTestSuite contains tests for Prepare and Query APIs.
type QueryTestSuite struct {
	*test.NoSQLTestSuite
	testInterval time.Duration
	table        string
	index        string
}

// SetupSuite will run before the tests in the suite are run.
//
// This method implements suite.SetupAllSuite interface in testify package.
func (suite *QueryTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	suite.table = suite.GetTableName("QueryTable")
	suite.index = "idx"
	suite.testInterval = 500 * time.Millisecond
	suite.createTableAndIndex(suite.table, suite.index)

}

// TearDownTest will run after each test in the suite.
//
// This method implements suite.TearDownTestSuite interface in testify package.
func (suite *QueryTestSuite) TearDownTest() {
	err := test.DeleteTable(suite.table, "sid", "id")
	suite.NoError(err)
}

func (suite *QueryTestSuite) createTableAndIndex(table, index string) {
	// Create a test table.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sid INTEGER, "+
		"id INTEGER, "+
		"name STRING, "+
		"age INTEGER, "+
		"state STRING, "+
		"salary LONG, "+
		"array ARRAY(INTEGER), "+
		"longString STRING, "+
		"PRIMARY KEY(SHARD(sid), id))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  15000,
		WriteUnits: 15000,
		StorageGB:  50,
	}
	suite.ReCreateTable(table, stmt, limits)

	stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s on %s(name)",
		index, table)
	suite.ExecuteTableDDL(stmt)
}

func (suite *QueryTestSuite) TestBasicQuery() {
	numMajor, numPerMajor := 10, 30
	if testing.Short() {
		numMajor, numPerMajor = 10, 10
	}

	tableName := suite.table
	numRows := numMajor * numPerMajor
	suite.loadRowsToTable(numMajor, numPerMajor, 1, tableName)

	// Perform a simple query.
	stmt := fmt.Sprintf("select * from %s where sid > 7", tableName)
	expNumRows := (numMajor - 7 - 1) * numPerMajor
	suite.executeQuery(stmt, nil, expNumRows, 0, false, nil)

	// Perform an update.
	stmt = fmt.Sprintf("update %s f set f.name = 'joe' where sid = 9 and id = 9", tableName)
	results, err := suite.ExecuteQueryStmt(stmt)
	if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
		// Perform a query to check if the previous update operation succeeded.
		stmt = fmt.Sprintf("select name from %s where sid = 9 and id = 9", tableName)
		results, err = suite.ExecuteQueryStmt(stmt)
		if suite.NoErrorf(err, "Query(%s) got error: %v", stmt, err) {
			if suite.Equalf(1, len(results), "unexpected number of rows returned.") {
				name, _ := results[0].GetString("name")
				suite.Equalf("joe", name, "unexpected value returned for \"name\" field")
			}
		}
	}

	// Perform a full table scan and count the number of rows.
	stmt = "select * from " + tableName
	suite.executeQuery(stmt, nil, numRows, 0, false /* use prepared statement */, nil)
	suite.executeQuery(stmt, nil, numRows, 0, true /* use prepared statement */, nil)

	// Query with external variables.
	stmt = fmt.Sprintf("declare $sid integer; $id integer;"+
		"select name from %s where sid = $sid and id >= $id", tableName)
	bindVars := map[string]interface{}{
		"$sid": 9,
		"$id":  3,
	}
	expNumRows = numPerMajor - 3
	suite.executeQuery(stmt, bindVars, expNumRows, 0, true, nil)

	// Query with sort.
	stmt = fmt.Sprintf("select * from %s where sid = 0 order by sid, id", tableName)
	suite.executeQuery(stmt, nil, numPerMajor, 0, false, nil)
	suite.executeQuery(stmt, nil, numPerMajor, 0, true, nil)

	// Check if returned values are in desired order as specified in select statement.
	expValues := make([]*types.MapValue, 1)
	var expVal *types.MapValue

	// Test with "select * from ..."
	stmt = fmt.Sprintf("select * from %s where sid = 1 and id = 1", tableName)
	expNumRows = 1
	expVal = types.NewOrderedMapValue()
	expVal.Put("sid", 1).Put("id", 1).Put("name", "name_1").Put("age", 1)
	expVal.Put("state", "OR").Put("salary", int64(15000))
	expVal.Put("array", []types.FieldValue{4, 7, 7, 11})
	expVal.Put("longString", "")
	expValues[0] = expVal
	suite.executeQuery(stmt, nil, expNumRows, 0, true, expValues)

	// Test with "select field_list from ..."
	stmt = fmt.Sprintf("select name, id, sid, age from %s where sid = 2 and id = 2", tableName)
	expNumRows = 1
	expVal = types.NewOrderedMapValue()
	expVal.Put("name", "name_2").Put("id", 2).Put("sid", 2).Put("age", 2)
	expValues[0] = expVal
	suite.executeQuery(stmt, nil, expNumRows, 0, true, expValues)
}

func (suite *QueryTestSuite) TestQueryWithSmallLimit() {
	numMajor, numPerMajor, recordKB := 1, 5, 2
	minRead := suite.minRead()
	prepCost := suite.minQueryCost()

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)

	// Update with number based limit of 1.
	newRecordKB := 1
	longStr := test.GenString((newRecordKB - 1) * 1024)
	stmt := fmt.Sprintf(`update %s set longString = "%s" where sid = 0 and id = 0`,
		suite.table, longStr)
	req := &nosqldb.QueryRequest{
		Statement: stmt,
		Limit:     1,
	}
	res, err := suite.Client.Query(req)

	expectReadKB := minRead + recordKB
	expectWriteKB := recordKB + newRecordKB

	if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
		if test.IsCloud() {
			suite.AssertReadWriteKB(res, expectReadKB, expectWriteKB, prepCost, true)
		} else {
			suite.AssertZeroReadWriteKB(res)
		}
	}

	if suite.IsCloud() || (suite.IsOnPrem() && suite.Version > "20.3") {
		// Update with maxReadKB of 1, expect an IllegalArgument error.
		stmt = fmt.Sprintf(`update %s set longString = "%s" where sid = 0 and id = 1`,
			suite.table, longStr)
		for kb := 1; kb <= expectReadKB; kb++ {
			req = &nosqldb.QueryRequest{
				Statement: stmt,
				MaxReadKB: uint(kb),
			}
			res, err = suite.Client.Query(req)
			if kb < expectReadKB {
				suite.Truef(nosqlerr.IsIllegalArgument(err),
					"Update with MaxReadKB=%d (expect ReadKB=%d) should have failed with an "+
						"IllegalArgument error, got error %v", kb, expectReadKB, err)

			} else if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
				if test.IsCloud() {
					suite.AssertReadWriteKB(res, expectReadKB, expectWriteKB, prepCost, true)
				} else {
					suite.AssertZeroReadWriteKB(res)
				}
			}
		}
	}

	// Update 0 rows with maxReadKB of 1.
	stmt = fmt.Sprintf("update %s set longString = \"%s\" where sid = 10000 and id = 1",
		suite.table, longStr)
	req = &nosqldb.QueryRequest{
		Statement: stmt,
		MaxReadKB: 1,
	}
	res, err = suite.Client.Query(req)
	if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
		if test.IsCloud() {
			suite.AssertReadWriteKB(res, minRead, 0, prepCost, true)
		} else {
			suite.AssertZeroReadWriteKB(res)
		}
	}

	// Query with number limit of 1
	stmt = fmt.Sprintf("select * from %s where sid = 0 and id > 1", suite.table)
	numRows := numMajor * (numPerMajor - 2)
	expectReadKB = suite.getExpectReadKB(false, recordKB, numRows, numRows)
	suite.executeQueryTest(stmt,
		false,        // keyOnly
		false,        // indexScan
		numRows,      // expected number of rows returned
		expectReadKB, // expected readKB
		1,            // limit
		0,            // maxReadKB
		recordKB)

	// Query with MaxReadKB of 1, expect an IllegalArgument error
	req = &nosqldb.QueryRequest{
		Statement: stmt,
		MaxReadKB: 1,
	}
	_, err = suite.ExecuteQueryRequest(req)
	suite.Truef(nosqlerr.IsIllegalArgument(err), "Query(MaxReadKB=1, stmt=%s) "+
		"should have failed with an IllegalArgument error, got error %v", err)
}

func (suite *QueryTestSuite) TestIllegalQuery() {

	var prepReq *nosqldb.PrepareRequest
	var prepRes *nosqldb.PrepareResult
	var req *nosqldb.QueryRequest
	var err error
	var stmt string
	queryErrMsg := "Testcase %d: Query(req=%#v) got error %v, expect IllegalArgument error."
	testNum := 0

	type queryTestCase struct {
		stmt       string
		expErrCode nosqlerr.ErrorCode
		expErrMsg  string // Expected keywords in the error message.
	}

	tests := []struct {
		stmt       string
		expErrCode nosqlerr.ErrorCode
		expErrMsg  string // Expected keywords in the error message.
	}{
		// Syntax error
		{
			stmt:       "select * from",
			expErrCode: nosqlerr.IllegalArgument,
		},
		// table not found
		{
			stmt:       "select * from non_exist_table",
			expErrCode: nosqlerr.TableNotFound,
		},
		// invalid column
		{
			stmt:       fmt.Sprintf("select * from %s where non_exist_column = 1", suite.table),
			expErrCode: nosqlerr.IllegalArgument,
		},
		// Prepare or query with a DDL statement.
		{
			stmt:       "create table t1 (id integer, name string, primary key(id))",
			expErrCode: nosqlerr.IllegalArgument,
		},
		{
			stmt:       "drop table t1",
			expErrCode: nosqlerr.IllegalArgument,
		},
		{
			stmt:       "create namespace ns001",
			expErrCode: nosqlerr.IllegalArgument,
		},
	}

	for _, r := range tests {
		testNum++
		prepReq = &nosqldb.PrepareRequest{Statement: r.stmt}
		prepRes, err = suite.Client.Prepare(prepReq)
		switch r.expErrCode {
		case nosqlerr.NoError:
			suite.NoErrorf(err, "Testcase %d: Prepare(stmt=%q) got error %v",
				testNum, r.stmt, err)

		default:
			suite.Truef(nosqlerr.Is(err, r.expErrCode),
				"Testcase %d: Prepare(stmt=%q) expect error %v, got error %v",
				testNum, r.stmt, r.expErrCode, err)
		}

		testNum++
		req = &nosqldb.QueryRequest{Statement: r.stmt}
		_, err = suite.Client.Query(req)
		switch r.expErrCode {
		case nosqlerr.NoError:
			suite.NoErrorf(err, "Testcase %d: Query(req=%#v) got error %v",
				testNum, req, err)

		default:
			suite.Truef(nosqlerr.Is(err, r.expErrCode),
				"Testcase %d: Query(req=%#v) expect error %v, got error %v",
				testNum, req, r.expErrCode, err)
		}
	}

	// Try a query that requires external variables that are missing.
	stmt = fmt.Sprintf("declare $sid integer; $id integer;"+
		"select name from %s where sid = $sid and id >= $id", suite.table)
	testNum++
	req = &nosqldb.QueryRequest{Statement: stmt}
	_, err = suite.Client.Query(req)
	suite.Truef(nosqlerr.IsIllegalArgument(err), queryErrMsg, testNum, req, err)

	testNum++
	prepReq = &nosqldb.PrepareRequest{Statement: stmt}
	prepRes, err = suite.Client.Prepare(prepReq)
	if suite.NoErrorf(err,
		"Testcase %d: Prepare(req=%#v) should have succeeded, got error: %v",
		testNum, prepReq, err) {

		testNum++
		req = &nosqldb.QueryRequest{PreparedStatement: &prepRes.PreparedStatement}
		_, err = suite.Client.Query(req)
		suite.Truef(nosqlerr.IsIllegalArgument(err), queryErrMsg, testNum, req, err)

		// Wrong name of variables.
		testNum++
		// variable name should be "$sid" and "$id"
		prepRes.PreparedStatement.SetVariable("sid", 9.1)
		prepRes.PreparedStatement.SetVariable("id", 3)

		req = &nosqldb.QueryRequest{PreparedStatement: &prepRes.PreparedStatement}
		_, err = suite.Client.Query(req)
		suite.Truef(nosqlerr.IsIllegalArgument(err), queryErrMsg, testNum, req, err)

		// Wrong type for variables.
		testNum++
		prepRes.PreparedStatement.SetVariable("$sid", 9.1)
		prepRes.PreparedStatement.SetVariable("$id", 3)
		req = &nosqldb.QueryRequest{PreparedStatement: &prepRes.PreparedStatement}
		_, err = suite.Client.Query(req)
		suite.Truef(nosqlerr.IsIllegalArgument(err), queryErrMsg, testNum, req, err)
	}

}

func (suite *QueryTestSuite) TestEvolution() {
	tableName := suite.GetTableName("EvolutionTable")
	idxName := "idx001"
	suite.createTableAndIndex(tableName, idxName)
	numMajor, numPerMajor := 1, 10
	suite.loadRowsToTable(numMajor, numPerMajor, 2, tableName)

	stmt := "select age from " + tableName
	prepReq := &nosqldb.PrepareRequest{Statement: stmt}
	prepRes, err := suite.Client.Prepare(prepReq)
	suite.Require().NoErrorf(err, "Prepare(%s) got error: %v", stmt, err)

	pStmt := &prepRes.PreparedStatement
	req := &nosqldb.QueryRequest{PreparedStatement: pStmt}
	results, err := suite.ExecuteQueryRequest(req)
	if suite.NoErrorf(err, "Query(%s) got error: %v", stmt, err) {
		suite.Equalf(numPerMajor, len(results), "unexpected number of rows returned")
	}

	// Evolve the table and try the query again, it will fail because the
	// target column has been dropped.
	suite.ExecuteTableDDL(fmt.Sprintf("alter table %s (drop age)", tableName))

	_, err = suite.ExecuteQueryRequest(req)
	suite.Require().Truef(nosqlerr.IsIllegalArgument(err),
		"Query(%s) should have failed with an "+
			"IllegalArgument error, got error %v.", stmt, err)
}

func (suite *QueryTestSuite) TestPrepare() {
	testCases := [...]string{
		"select * from " + suite.table,
		fmt.Sprintf("declare $sval string; $sid integer; $id integer;"+
			"update %s set longString = $sval "+
			"where sid = $sid and id = $id", suite.table),
	}
	for _, stmt := range testCases {
		req := &nosqldb.PrepareRequest{Statement: stmt}
		res, err := suite.Client.Prepare(req)
		if suite.NoErrorf(err, "Prepare(%s) got error: %v", stmt, err) {
			suite.Equalf(0, res.WriteKB, "wrong writeKB")
			if test.IsCloud() {
				suite.Equalf(suite.minQueryCost(), res.ReadKB, "wrong readKB.")
			} else {
				suite.Equalf(0, res.ReadKB, "wrong readKB.")
			}
		}
	}
}

func (suite *QueryTestSuite) TestLimits() {
	numMajor, numPerMajor := 10, 101
	numRows := numMajor * numPerMajor
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)

	testCases := [...]struct {
		stmt        string // query statement
		numReadRows int    // number of rows to read
		numReadKeys int    // number of keys to read
		expCnt      int    // expected number of rows returned
		indexScan   bool
		numLimits   []int
		sizeLimits  []int
	}{
		// Read rows from all partitions.
		{
			stmt:        "select * from " + suite.table,
			numReadRows: numRows,
			numReadKeys: numRows,
			expCnt:      numRows,
			indexScan:   false,
			numLimits:   []int{0, 20, 100, numRows, numRows + 1},
			sizeLimits:  []int{0, 500, 1000, 2000},
		},
		// Read rows from a single partition.
		{
			stmt:        fmt.Sprintf("select * from %s where sid = 5", suite.table),
			numReadRows: numPerMajor,
			numReadKeys: numPerMajor,
			expCnt:      numPerMajor,
			indexScan:   false,
			numLimits:   []int{0, 20, 100, numPerMajor, numPerMajor + 1},
			sizeLimits:  []int{0, 50, 100, 250},
		},
		// Read rows from all shards.
		{
			stmt:        fmt.Sprintf("select * from %s where name = 'name_1'", suite.table),
			numReadRows: numMajor,
			numReadKeys: numMajor,
			expCnt:      numMajor,
			indexScan:   true,
			numLimits:   []int{0, 5, numMajor, numMajor + 1},
			sizeLimits:  []int{0, 50, 10, 25},
		},
	}

	for _, r := range testCases {

		expReadKB := suite.getExpectReadKB(false, recordKB, r.numReadRows, r.numReadKeys)
		// Test number-based limits.
		for _, limit := range r.numLimits {
			suite.executeQueryTest(r.stmt,
				false,       // keyOnly
				r.indexScan, // indexScan
				r.expCnt,    // expected number of rows
				expReadKB,   // expected readKB
				limit,       // number limit
				0,           // size limit
				recordKB)
			time.Sleep(suite.testInterval)
		}

		// Test size-based limits.
		for _, maxReadKB := range r.sizeLimits {
			suite.executeQueryTest(r.stmt,
				false,       // keyOnly
				r.indexScan, // indexScan
				r.expCnt,    // expected number of rows
				expReadKB,   // expected readKB
				0,           // number limit
				maxReadKB,   // size limit
				recordKB)
			time.Sleep(suite.testInterval)
		}

		// Test number-based and size-based limits.
		idx := rand.Intn(len(r.numLimits))
		limit := r.numLimits[idx]

		idx = rand.Intn(len(r.sizeLimits))

		maxReadKB := r.sizeLimits[idx]

		suite.executeQueryTest(r.stmt,
			false,       // keyOnly
			r.indexScan, // indexScan
			r.expCnt,    // expected number of rows
			expReadKB,   // expected readKB
			limit,       // nubmer limit
			maxReadKB,   // size limit
			recordKB)
	}
}

func (suite *QueryTestSuite) TestJson() {
	jsonRecords := [...]string{
		"{" +
			" \"id\":0," +
			" \"info\":" +
			"  {" +
			"    \"firstName\":\"first0\", \"lastName\":\"last0\",\"age\":10," +
			"    \"address\":" +
			"    {" +
			"      \"city\": \"San Francisco\"," +
			"      \"state\"  : \"CA\"," +
			"      \"phones\" : [ { \"areacode\" : 408,  \"number\" : 50, \"kind\" : \"home\" }," +
			"                     { \"areacode\" : 650,  \"number\" : 51, \"kind\" : \"work\" }," +
			"                     \"650-234-4556\"," +
			"                     650234455" +
			"                   ]" +
			"    }," +
			"    \"children\":" +
			"    {" +
			"      \"Anna\" : { \"age\" : 10, \"school\" : \"sch_1\", \"friends\" : [\"Anna\", \"John\", \"Maria\"]}," +
			"      \"Lisa\" : { \"age\" : 12, \"friends\" : [\"Ada\"]}" +
			"    }" +
			"  }" +
			"}",

		"{" +
			"  \"id\":1," +
			"  \"info\":" +
			"  {" +
			"    \"firstName\":\"first1\", \"lastName\":\"last1\",\"age\":11," +
			"    \"address\":" +
			"    {" +
			"      \"city\"   : \"Boston\"," +
			"      \"state\"  : \"MA\"," +
			"      \"phones\" : [ { \"areacode\" : 304,  \"number\" : 30, \"kind\" : \"work\" }," +
			"                   { \"areacode\" : 318,  \"number\" : 31, \"kind\" : \"work\" }," +
			"                   { \"areacode\" : 400,  \"number\" : 41, \"kind\" : \"home\" }]" +
			"    }," +
			"    \"children\":" +
			"    {" +
			"      \"Anna\" : { \"age\" : 9,  \"school\" : \"sch_1\", \"friends\" : [\"Bobby\", \"John\", null]}," +
			"      \"Mark\" : { \"age\" : 4,  \"school\" : \"sch_1\", \"friends\" : [\"George\"]}," +
			"      \"Dave\" : { \"age\" : 15, \"school\" : \"sch_3\", \"friends\" : [\"Bill\", \"Sam\"]}" +
			"    }" +
			"  }" +
			"}",

		"{" +
			"  \"id\":2," +
			"  \"info\":" +
			"  {" +
			"    \"firstName\":\"first2\", \"lastName\":\"last2\",\"age\":12," +
			"    \"address\":" +
			"    {" +
			"      \"city\"   : \"Portland\"," +
			"      \"state\"  : \"OR\"," +
			"      \"phones\" : [ { \"areacode\" : 104,  \"number\" : 10, \"kind\" : \"home\" }," +
			"                   { \"areacode\" : 118,  \"number\" : 11, \"kind\" : \"work\" } ]" +
			"    }," +
			"    \"children\":" +
			"    {" +
			"    }" +
			"  }" +
			"}",

		"{ " +
			"  \"id\":3," +
			"  \"info\":" +
			"  {" +
			"    \"firstName\":\"first3\", \"lastName\":\"last3\",\"age\":13," +
			"    \"address\":" +
			"    {" +
			"      \"city\"   : \"Seattle\"," +
			"      \"state\"  : \"WA\"," +
			"      \"phones\" : null" +
			"    }," +
			"    \"children\":" +
			"    {" +
			"      \"George\" : { \"age\" : 7,  \"school\" : \"sch_2\", \"friends\" : [\"Bill\", \"Mark\"]}," +
			"      \"Matt\" :   { \"age\" : 14, \"school\" : \"sch_2\", \"friends\" : [\"Bill\"]}" +
			"    }" +
			"  }" +
			"}",
	}

	jsonTable := suite.GetTableName("jsonTable")
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INTEGER, info JSON, "+
		"PRIMARY KEY(id))", jsonTable)
	limits := &nosqldb.TableLimits{
		ReadUnits:  15000,
		WriteUnits: 15000,
		StorageGB:  50,
	}
	suite.ReCreateTable(jsonTable, stmt, limits)

	// Load rows into table.
	for _, s := range jsonRecords {
		v, err := types.NewMapValueFromJSON(s)
		suite.Require().NoError(err, "NewMapValueFromJSON(%q) got error %v", s, err)
		req := &nosqldb.PutRequest{
			TableName: jsonTable,
			Value:     v,
		}
		res, err := suite.Client.Put(req)
		suite.Require().NoError(err)
		suite.Require().NotNil(res.Version)
	}

	// Basic query on a table with JSON field
	stmt = fmt.Sprintf("select id, f.info from %s f", jsonTable)
	suite.executeQuery(stmt, nil, 4, 0, false, nil)

	// Test JsonNull
	stmt = fmt.Sprintf("select id from %s f where f.info.address.phones = null", jsonTable)
	suite.executeQuery(stmt, nil, 1, 0, false, nil)

	// Bind JsonNull value
	stmt = fmt.Sprintf("declare $phones json;"+
		"select id, f.info.address.phones from %s f "+
		"where f.info.address.phones != $phones", jsonTable)
	bindVars := map[string]interface{}{
		"$phones": types.JSONNullValueInstance,
	}
	suite.executeQuery(stmt, bindVars, 3, 0, true, nil)

	// Bind 2 string values
	stmt = fmt.Sprintf("declare $city string;$name string;"+
		"select id, f.info.address.city, f.info.children.keys() "+
		"from %s f "+
		"where f.info.address.city = $city and "+
		"not f.info.children.keys() =any $name", jsonTable)
	bindVars = map[string]interface{}{
		"$city": "Portland",
		"$name": "John",
	}
	suite.executeQuery(stmt, bindVars, 1, 0, true, nil)

	// Bind MapValue
	stmt = fmt.Sprintf("declare $child json;"+
		"select id, f.info.children.values() "+
		"from %s f "+
		"where f.info.children.values() =any $child", jsonTable)
	bindVars = map[string]interface{}{
		"$child": map[string]interface{}{
			"age":    14,
			"school": "sch_2",
			//TODO: accept []string
			"friends": []types.FieldValue{"Bill"},
		},
	}
	suite.executeQuery(stmt, bindVars, 1, 0, true, nil)

	// Bind ArrayValue
	stmt = fmt.Sprintf("declare $friends json;"+
		"select id, f.info.children.values() "+
		"from %s f "+
		"where f.info.children.values().friends =any $friends", jsonTable)
	bindVars = map[string]interface{}{
		//TODO: accept []string
		"$friends": []types.FieldValue{"Bill", "Mark"},
	}
	suite.executeQuery(stmt, bindVars, 1, 0, true, nil)

}

func (suite *QueryTestSuite) TestGroupByWithLimits() {

	numMajor, numPerMajor := 10, 101
	numRows := numMajor * numPerMajor
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)

	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idxSidAge ON %s(sid, age)",
		suite.table)
	suite.ExecuteTableDDL(stmt)

	testCases := []struct {
		stmt        string // query statement with group by clause
		numReadRows int    // number of rows to read
		numReadKeys int    // number of keys to read
		expCnt      int    // expected number of rows
		keyOnly     bool
		indexScan   bool
		numLimits   []int
		sizeLimits  []int
	}{
		// Single partition scan, key only.
		{
			stmt:        fmt.Sprintf("select count(*) from %s where sid = 1", suite.table),
			numReadRows: 0,
			numReadKeys: numPerMajor,
			expCnt:      1,
			keyOnly:     true,
			indexScan:   false,
			numLimits:   []int{0, 1 /* expCnt */, 2 /* expCnt + 1 */},
			sizeLimits:  []int{0, 50, 100, 101},
		},
		// Single partition scan, key + row.
		{
			stmt:        fmt.Sprintf("select min(name), min(age) from %s where sid = 1", suite.table),
			numReadRows: numPerMajor,
			numReadKeys: numPerMajor,
			expCnt:      1,
			keyOnly:     false,
			indexScan:   false,
			numLimits:   []int{0, 1, 2},
			sizeLimits:  []int{0, 10, 100, 300, 303},
		},
		// All partitions scan, key only.
		{
			stmt:        fmt.Sprintf("select count(*) from %s group by sid", suite.table),
			numReadRows: 0,
			numReadKeys: numRows,
			expCnt:      numMajor,
			keyOnly:     true,
			indexScan:   false,
			numLimits:   []int{0, 5, numMajor, numMajor + 1},
			sizeLimits:  []int{0, 10, 100, 500, 1000, 1010},
		},
		// All partitions scan, key + row.
		{
			stmt:        fmt.Sprintf("select min(name) from %s group by sid", suite.table),
			numReadRows: numRows,
			numReadKeys: numRows,
			expCnt:      numMajor,
			keyOnly:     false,
			indexScan:   false,
			numLimits:   []int{0, 5, numMajor, numMajor + 1},
			sizeLimits:  []int{0, 10, 100, 500, 1000, 2047},
		},
		// All shards scan, key only.
		{
			stmt:        fmt.Sprintf("select count(*) from %s group by sid, age", suite.table),
			numReadRows: 0,
			numReadKeys: numRows,
			expCnt:      numMajor * 10, // There are 10 different values for the "age" field.
			keyOnly:     true,
			indexScan:   true,
			numLimits:   []int{0, 5, 50, numMajor * 10, numMajor*10 + 1},
			sizeLimits:  []int{0, 10, 100, 500, 1000, 1010},
		},
		// All shards scan, key + row
		{
			stmt:        fmt.Sprintf("select max(name) from %s group by sid, age", suite.table),
			numReadRows: numRows,
			numReadKeys: numRows,
			expCnt:      numMajor * 10,
			keyOnly:     false,
			indexScan:   true,
			numLimits:   []int{0, 5, 50, numMajor * 10, numMajor*10 + 1},
			sizeLimits:  []int{0, 10, 100, 500, 1000, 2047},
		},
		// All partitions scan, key only. Single row returned.
		{
			stmt:        fmt.Sprintf("select count(*) from %s", suite.table),
			numReadRows: 0,
			numReadKeys: numRows,
			expCnt:      1,
			keyOnly:     true,
			indexScan:   false,
			numLimits:   []int{0, 1, 2},
			sizeLimits:  []int{0, 10, 100, 500, 1000, 1010},
		},
		// All shards scan, key only. Single row returned.
		{
			stmt:        fmt.Sprintf("select min(name) from %s", suite.table),
			numReadRows: 0,
			numReadKeys: numRows,
			expCnt:      1,
			keyOnly:     true,
			indexScan:   true,
			numLimits:   []int{0, 1, 2},
			sizeLimits:  []int{0, 10, 100, 500, 1000, 1010},
		},
	}

	for _, r := range testCases {

		expReadKB := suite.getExpectReadKB(r.keyOnly, recordKB, r.numReadRows, r.numReadKeys)
		// Test number-based limits.
		for _, limit := range r.numLimits {
			suite.executeQueryTest(r.stmt,
				r.keyOnly,   // keyOnly
				r.indexScan, // indexScan
				r.expCnt,    // expected number of rows
				expReadKB,   // expected readKB
				limit,       // number limit
				0,           // size limit
				recordKB)
			time.Sleep(suite.testInterval)
		}

		// Test size-based limits.
		for _, maxReadKB := range r.sizeLimits {
			suite.executeQueryTest(r.stmt,
				r.keyOnly,   // keyOnly
				r.indexScan, // indexScan
				r.expCnt,    // expected number of rows
				expReadKB,   // expected readKB
				0,           // number limit
				maxReadKB,   // size limit
				recordKB)
			time.Sleep(suite.testInterval)
		}

		// Test number-based and size-based limits.
		idx := rand.Intn(len(r.numLimits))
		limit := r.numLimits[idx]

		idx = rand.Intn(len(r.sizeLimits))
		maxReadKB := r.sizeLimits[idx]

		suite.executeQueryTest(r.stmt,
			r.keyOnly,   // keyOnly
			r.indexScan, // indexScan
			r.expCnt,    // expected number of rows
			expReadKB,   // expected readKB
			limit,       // number limit
			maxReadKB,   // size limit
			recordKB)
	}
}

func (suite *QueryTestSuite) TestUpdateQuery() {
	numMajor, numPerMajor := 1, 10
	recordKB := 2
	minRead := suite.minRead()
	prepCost := suite.minQueryCost()

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)

	// Update a row.
	newRecordKB := 1
	longStr := test.GenString((newRecordKB - 1) * 1024)
	stmt := fmt.Sprintf(`update %s set longString = "%s" where sid = 0 and id = 0`,
		suite.table, longStr)
	req := &nosqldb.QueryRequest{Statement: stmt}
	res, err := suite.Client.Query(req)
	if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
		if test.IsCloud() {
			suite.AssertReadWriteKB(res, minRead+recordKB, recordKB+newRecordKB, prepCost, true)
		} else {
			suite.AssertZeroReadWriteKB(res)
		}
	}

	// Update non-exsiting row.
	stmt = fmt.Sprintf(`update %s set longString = "test" where sid = 1000 and id = 0`, suite.table)
	req = &nosqldb.QueryRequest{Statement: stmt}
	res, err = suite.Client.Query(req)
	if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
		if test.IsCloud() {
			suite.AssertReadWriteKB(res, minRead, 0, prepCost, true)
		} else {
			suite.AssertZeroReadWriteKB(res)
		}
	}

	// Update using prepared statement
	stmt = fmt.Sprintf("declare $sval string; $sid integer; $id integer;"+
		"update %s set longString = $sval where sid = $sid and id = $id", suite.table)
	prepReq := &nosqldb.PrepareRequest{Statement: stmt}
	prepRes, err := suite.Client.Prepare(prepReq)
	if suite.NoErrorf(err, "failed to prepare for %q, got error: %v", stmt, err) {
		//TODO
		// suite.NotNil(prepRes.PreparedStatement.Statement)

		if test.IsCloud() {
			suite.AssertReadWriteKB(prepRes, prepCost, 0, 0, false)
		} else {
			suite.AssertZeroReadWriteKB(prepRes)
		}

		prepRes.PreparedStatement.SetVariable("$sval", longStr)
		prepRes.PreparedStatement.SetVariable("$sid", 0)
		prepRes.PreparedStatement.SetVariable("$id", 1)

		req = &nosqldb.QueryRequest{PreparedStatement: &prepRes.PreparedStatement}
		res, err = suite.Client.Query(req)
		if suite.NoErrorf(err, "failed to execute %q, got error: %v", stmt, err) {
			if test.IsCloud() {
				suite.AssertReadWriteKB(res, minRead+recordKB, recordKB+newRecordKB, 0, true)
			} else {
				suite.AssertZeroReadWriteKB(res)
			}
		}
	}

}

func (suite *QueryTestSuite) TestDupElim() {
	numMajor, numPerMajor := 10, 40
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)
	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idxArray ON %s(array[])",
		suite.table)
	suite.ExecuteTableDDL(stmt)

	stmt = fmt.Sprintf("select sid, id, t.array[size($)-2:] from %s t where t.array[] >any 11",
		suite.table)
	suite.executeQuery(stmt, nil, 200, 20, true, nil)
}

func (suite *QueryTestSuite) TestOrderByPartitions() {
	numMajor, numPerMajor := 5, 10
	numRows := numMajor * numPerMajor
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)
	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idxStateAge ON %s(state, age)",
		suite.table)
	suite.ExecuteTableDDL(stmt)

	testCases := []struct {
		stmt        string // query statement with group by clause
		numReadRows int    // number of rows to read
		numReadKeys int    // number of keys to read
		expCnt      int    // expected number of rows
		keyOnly     bool
		indexScan   bool
		sizeLimits  []int
	}{
		// Case 1: partial key
		{
			stmt:        fmt.Sprintf("select sid, id, name, state from %s order by sid", suite.table),
			numReadRows: numRows,
			numReadKeys: numRows,
			expCnt:      numRows,
			keyOnly:     false,
			indexScan:   false,
			sizeLimits:  []int{0, 4, 25, 37, 66},
		},
		// Case 2: partial key + offset + limit
		{
			stmt: fmt.Sprintf("select sid, id, name, state from %s "+
				"order by sid limit 10 offset 4", suite.table),
			numReadRows: -1,
			numReadKeys: -1,
			expCnt:      10,
			keyOnly:     false,
			indexScan:   false,
			sizeLimits:  []int{0, 5, 6, 7, 8, 9, 20, 44, 81},
		},
		// Case 3: partial key + offset + limit
		{
			stmt: fmt.Sprintf("select sid, id, name, state from %s "+
				"order by sid limit 5 offset 44", suite.table),
			numReadRows: -1,
			numReadKeys: -1,
			expCnt:      5,
			keyOnly:     false,
			indexScan:   false,
			sizeLimits:  []int{0, 5, 14, 51, 88},
		},
	}

	var expReadKB int
	for _, r := range testCases {
		if r.numReadRows == -1 || r.numReadKeys == -1 {
			expReadKB = -1
		} else {
			expReadKB = suite.getExpectReadKB(r.keyOnly, recordKB, r.numReadRows, r.numReadKeys)
		}
		for _, maxReadKB := range r.sizeLimits {
			suite.executeQueryWithOpts(r.stmt, r.keyOnly, r.indexScan,
				r.expCnt, expReadKB, 0 /* number limits */, maxReadKB,
				recordKB, types.Eventual)
		}
	}
}

func (suite *QueryTestSuite) TestGroupByPartitions() {
	numMajor, numPerMajor := 5, 10
	numRows := numMajor * numPerMajor
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)
	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idxStateAge ON %s(state, age)",
		suite.table)
	suite.ExecuteTableDDL(stmt)

	testCases := []struct {
		stmt        string // query statement with group by clause
		numReadRows int    // number of rows to read
		numReadKeys int    // number of keys to read
		expCnt      int    // expected number of rows
		keyOnly     bool
		indexScan   bool
		sizeLimits  []int
	}{
		// Case 1: partial key
		{
			stmt: fmt.Sprintf("select sid, count(*) as cnt, sum(salary) as sum "+
				"from %s group by sid", suite.table),
			numReadRows: numRows,
			numReadKeys: numRows,
			expCnt:      5,
			keyOnly:     false,
			indexScan:   false,
			sizeLimits:  []int{0},
		},
	}

	var expReadKB int
	for _, r := range testCases {
		if r.numReadRows == -1 || r.numReadKeys == -1 {
			expReadKB = -1
		} else {
			expReadKB = suite.getExpectReadKB(r.keyOnly, recordKB, r.numReadRows, r.numReadKeys)
		}
		for _, maxReadKB := range r.sizeLimits {
			suite.executeQueryWithOpts(r.stmt, r.keyOnly, r.indexScan,
				r.expCnt, expReadKB, 0 /* number limits */, maxReadKB,
				recordKB, types.Eventual)
		}
	}
}

func (suite *QueryTestSuite) TestOrderByShards() {
	numMajor, numPerMajor := 10, 40
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)
	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idxStateAge ON %s(state, age)",
		suite.table)
	suite.ExecuteTableDDL(stmt)

	testCases := []struct {
		stmt        string // query statement with group by clause
		numReadRows int    // number of rows to read
		numReadKeys int    // number of keys to read
		expCnt      int    // expected number of rows
		keyOnly     bool
		indexScan   bool
		sizeLimits  []int
	}{
		// Case 1: multi-shard, covering index
		{
			stmt: fmt.Sprintf("select sid, id, state from %s "+
				"order by state limit 20 offset 4", suite.table),
			numReadRows: 0,
			numReadKeys: 24,
			expCnt:      20,
			keyOnly:     true,
			indexScan:   true,
			sizeLimits:  []int{0, 5, 7, 11},
		},
		// Case 2: multi-shard, non-covering index
		{
			stmt: fmt.Sprintf("select sid, id, state, salary from %s "+
				"order by state limit 27 offset 5", suite.table),
			numReadRows: 32,
			numReadKeys: 32,
			expCnt:      27,
			keyOnly:     false,
			indexScan:   true,
			sizeLimits:  []int{6, 7, 8},
		},
		// Case 3: single-partition, non-covering index
		{
			stmt: fmt.Sprintf("select sid, id, state, salary from %s "+
				"where sid = 3 order by sid, id limit 27 offset 5", suite.table),
			numReadRows: 32,
			numReadKeys: 32,
			expCnt:      27,
			keyOnly:     false,
			indexScan:   true,
			sizeLimits:  []int{4, 5, 12},
		},
	}

	var expReadKB int
	for _, r := range testCases {
		if r.numReadRows == -1 || r.numReadKeys == -1 {
			expReadKB = -1
		} else {
			expReadKB = suite.getExpectReadKB(r.keyOnly, recordKB, r.numReadRows, r.numReadKeys)
		}
		for _, maxReadKB := range r.sizeLimits {
			suite.executeQueryWithOpts(r.stmt, r.keyOnly, r.indexScan,
				r.expCnt, expReadKB, 0 /* number limits */, maxReadKB,
				recordKB, types.Eventual)
		}
	}
}

func (suite *QueryTestSuite) TestGroupByShards() {
	numMajor, numPerMajor := 10, 101
	recordKB := 2

	suite.loadRowsToTable(numMajor, numPerMajor, recordKB, suite.table)
	stmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idxStateAge ON %s(state, age)",
		suite.table)
	suite.ExecuteTableDDL(stmt)

	testCases := []struct {
		stmt        string // query statement with group by clause
		numReadRows int    // number of rows to read
		numReadKeys int    // number of keys to read
		expCnt      int    // expected number of rows
		keyOnly     bool
		indexScan   bool
		sizeLimits  []int
	}{
		// Case 1: multi-shard, covering index
		{
			stmt:        fmt.Sprintf(`select count(*) from %s where state = "CA"`, suite.table),
			numReadRows: 0,
			numReadKeys: 210,
			expCnt:      1,
			keyOnly:     true,
			indexScan:   true,
			sizeLimits:  []int{10, 17, 23, 37, 209, 210, 500},
			// sizeLimits: []int{10},
		},
		// Case 2: multi-shard, non-covering index
		{
			stmt: fmt.Sprintf(`select count(*), sum(salary) from %s where state = "VT"`,
				suite.table),
			numReadRows: 200,
			numReadKeys: 200,
			expCnt:      1,
			keyOnly:     false,
			indexScan:   true,
			sizeLimits:  []int{0},
			// sizeLimits:  []int{9, 19, 31, 44, 200, 500},
		},
		// Case 3:
		{
			stmt:        fmt.Sprintf("select state, count(*) from %s group by state", suite.table),
			numReadRows: 0,
			numReadKeys: 1010,
			expCnt:      5,
			keyOnly:     true,
			indexScan:   true,
			sizeLimits:  []int{30},
		},
		// Case 4:
		{
			stmt: fmt.Sprintf("select state, count(*) as cnt, sum(salary) as sum, avg(salary) as avg "+
				"from %s group by state", suite.table),
			numReadRows: 1010,
			numReadKeys: 1010,
			expCnt:      5,
			keyOnly:     false,
			indexScan:   true,
			sizeLimits:  []int{34},
		},
	}

	var expReadKB int
	for _, r := range testCases {
		if r.numReadRows == -1 || r.numReadKeys == -1 {
			expReadKB = -1
		} else {
			expReadKB = suite.getExpectReadKB(r.keyOnly, recordKB, r.numReadRows, r.numReadKeys)
		}
		for _, maxReadKB := range r.sizeLimits {
			suite.executeQueryWithOpts(r.stmt, r.keyOnly, r.indexScan,
				r.expCnt, expReadKB, 0 /* number limits */, maxReadKB,
				recordKB, types.Eventual)
		}
	}
}

func (suite *QueryTestSuite) minQueryCost() int {
	return test.MinQueryCost
}

func (suite *QueryTestSuite) minRead() int {
	return test.MinReadKB
}

func (suite *QueryTestSuite) getExpectReadKB(keyOnly bool, recordKB, numReadRows, numReadKeys int) int {
	minRead := suite.minRead()
	readKB := numReadKeys * minRead
	if !keyOnly {
		readKB += numReadRows * recordKB
	}
	if readKB == 0 {
		return minRead
	}
	return readKB
}

func (suite *QueryTestSuite) loadRowsToTable(numMajor, numPerMajor, nKB int, table string) {
	states := []string{"CA", "OR", "WA", "VT", "NY"}
	salaries := []int{1000, 15000, 8000, 9000}
	arrays := [][]types.FieldValue{
		{1, 5, 7, 10},
		{4, 7, 7, 11},
		{3, 8, 17, 21},
		{3, 8, 12, 14},
	}
	n := (nKB - 1) * 1024
	for i := 0; i < numMajor; i++ {
		v := &types.MapValue{}
		v.Put("sid", i)

		for j := 0; j < numPerMajor; j++ {
			v.Put("id", j)
			v.Put("name", fmt.Sprintf("name_%d", j))
			v.Put("age", j%10)
			v.Put("state", states[j%5])
			v.Put("salary", salaries[j%4])
			v.Put("array", arrays[j%4])
			v.Put("longString", test.GenString(n))
			req := &nosqldb.PutRequest{
				TableName: table,
				Value:     v,
			}
			res, err := suite.Client.Put(req)
			if suite.NoError(err) {
				suite.NotNil(res.Version)
			}
		}
	}
}

func (suite *QueryTestSuite) executeQuery(stmt string, bindVars map[string]interface{},
	expectNumRows int, maxReadKB uint, usePrepStmt bool, expValue []*types.MapValue) {

	var req *nosqldb.QueryRequest
	if !usePrepStmt || len(bindVars) == 0 {
		req = &nosqldb.QueryRequest{
			Statement: stmt,
			MaxReadKB: maxReadKB,
		}

	} else {
		prepReq := &nosqldb.PrepareRequest{Statement: stmt}
		prepRes, err := suite.Client.Prepare(prepReq)
		if suite.NoErrorf(err, "Prepare(%s) got error %v", stmt, err) {
			pStmt := &prepRes.PreparedStatement
			for k, v := range bindVars {
				err = pStmt.SetVariable(k, v)
				suite.NoErrorf(err, "PreparedStatement.SetVariable(k=%s) got error %v", k, err)
			}
			req = &nosqldb.QueryRequest{
				PreparedStatement: pStmt,
				MaxReadKB:         maxReadKB,
			}
		}
	}

	actualValue, err := suite.ExecuteQueryRequest(req)
	if !suite.NoErrorf(err, "Query(%s) got error %v", stmt, err) {
		return
	}

	suite.Equalf(expectNumRows, len(actualValue), "unexpected number of rows returned for the query %q", stmt)

	if len(expValue) == 0 {
		return
	}

	// Check if returned values are in desired order, either the declaration order
	// when table was created, or the order specified in select list.
	for i, v := range expValue {
		for j := 1; j <= v.Len(); j++ {
			k1, v1, _ := v.GetByIndex(j)
			k2, v2, _ := actualValue[i].GetByIndex(j)
			suite.Equalf(k1, k2, "GetByIndex(%d): unexpected field name.", j)
			suite.Equalf(v1, v2, "GetByIndex(%d): unexpected field value.", j)
		}
	}
}

func (suite *QueryTestSuite) executeQueryTest(stmt string, keyOnly, indexScan bool,
	expNumRows, expReadKB, numLimit, sizeLimit, recordKB int) {
	suite.executeQueryWithOpts(stmt, keyOnly, indexScan, expNumRows, expReadKB,
		numLimit, sizeLimit, recordKB, types.Absolute)
	suite.executeQueryWithOpts(stmt, keyOnly, indexScan, expNumRows, expReadKB,
		numLimit, sizeLimit, recordKB, types.Eventual)
}

func (suite *QueryTestSuite) executeQueryWithOpts(stmt string, keyOnly, indexScan bool,
	expNumRows, expReadKB, numLimit, sizeLimit, recordKB int, consistency types.Consistency) {

	minRead := suite.minRead()
	prepCost := suite.minQueryCost()
	isAbsolute := consistency == types.Absolute
	isDelete := strings.Contains(strings.ToLower(stmt), "delete")
	// fmt.Printf("+++++++++++ is delete: %v\n", isDelete)
	req := &nosqldb.QueryRequest{
		Statement:   stmt,
		Limit:       uint(numLimit),
		Consistency: consistency,
		MaxReadKB:   uint(sizeLimit),
	}
	expReadUnits := expReadKB
	var expBatchReadUnits int
	if sizeLimit > 0 {
		expBatchReadUnits = sizeLimit
	} else {
		expBatchReadUnits = test.MaxReadKBLimit
	}

	if indexScan && !keyOnly {
		expBatchReadUnits += recordKB
	} else {
		expBatchReadUnits += minRead
	}

	if isDelete {
		expBatchReadUnits += minRead
	}

	if isAbsolute {
		expReadUnits <<= 1
		expBatchReadUnits <<= 1
	}

	var numRows, readKB, readUnits, numBatches, totalPrepCost int
	for {
		res, err := suite.Client.Query(req)
		if !suite.NoErrorf(err, "failed to execute query %q with expNumRows=%d, expReadKB=%d, numLimit=%d, sizeLimit=%d, recordKB=%d numBatches=%d", stmt, expNumRows, expReadKB, numLimit, sizeLimit, recordKB, numBatches) {
			break
		}

		// Verify writeKB = 0
		test.AssertWriteKB(suite.Assert(), 0, res.WriteKB)
		results, err := res.GetResults()
		if !suite.NoErrorf(err, "QueryResult.GetResults() got error %v", err) {
			break
		}

		cnt := len(results)
		if numLimit > 0 {
			suite.LessOrEqual(cnt, numLimit, "unexpected number of rows returned")
		}

		// suite.LessOrEqual(res.ReadUnits, expBatchReadUnits+prepCost, "unexpected read units")

		numRows += cnt
		capacity, err := res.ConsumedCapacity()
		suite.NoErrorf(err, "Result.ConsumedCapacity() got error %v", err)
		readKB += capacity.ReadKB
		readUnits += capacity.ReadUnits
		// readKB += res.ReadKB
		// readUnits += res.ReadUnits
		if numBatches == 0 {
			prepCost = suite.minQueryCost()
		} else {
			prepCost = 0
		}

		totalPrepCost += prepCost
		numBatches++

		if req.IsDone() {
			break
		}
	}

	suite.Equal(expNumRows, numRows, "unexpected number of rows returned.")

	if test.IsOnPrem() {
		suite.Equalf(0, readKB, "readKB should be 0")
		suite.Equalf(0, readUnits, "readUnits should be 0")

	} else {
		suite.Greaterf(readKB, 0, "readKB should be > 0")
		suite.Greaterf(readUnits, 0, "readUnits should be > 0")

		/*
			Commenting out; generates different errors in different environments.
			We now rely on java driver to verify RU/WU usage in queries.
				if expReadKB >= 0 {
					if numBatches == 1 {
						test.AssertReadKB(suite.Assert(), expReadKB, readKB, readUnits,
							totalPrepCost, isAbsolute)
					} else {
						// When read cost exceeds size limit after reading the key, the read cost
						// may have an additional minRead exceeded per batch.
						delta := (numBatches - 1) * minRead
						if isAbsolute {
							delta <<= 1
						}
						expReadUnits += totalPrepCost
						suite.GreaterOrEqual(readUnits, expReadUnits, "wrong readUnits")
						suite.LessOrEqual(readUnits, expReadUnits+delta, "wrong readUnits")
						test.AssertReadUnits(suite.Assert(), readKB, readUnits, totalPrepCost, isAbsolute)
					}
				}
		*/
	}
}

func TestQuery(t *testing.T) {
	test := &QueryTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
