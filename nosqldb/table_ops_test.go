//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
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

// TableOpsTestSuite contains tests for table operation APIs:
//
//   DoTableRequest
//   DoTableRequestAndWait
//   GetTable
//   ListTables
//   GetIndexes
//
type TableOpsTestSuite struct {
	*test.NoSQLTestSuite
	table string
	ns    string // Namespace name.
}

type tableRequestTestCase struct {
	req    *nosqldb.TableRequest
	expErr nosqlerr.ErrorCode // Expected error code.
}

type getIndexTestCase struct {
	req       *nosqldb.GetIndexesRequest
	expIdxNum int                // Expected number of indexes.
	expErr    nosqlerr.ErrorCode // Expected error code.
}

func (suite *TableOpsTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	const stmt string = "CREATE TABLE IF NOT EXISTS %s (id INTEGER, c1 STRING, c2 LONG, PRIMARY KEY(id))"
	suite.table = suite.GetTableName("TableOps")
	createStmt := fmt.Sprintf(stmt, suite.table)
	suite.ReCreateTable(suite.table, createStmt, test.OkTableLimits)

	// Create a namespace and table in that namespace.
	if test.IsOnPrem() {
		suite.ns = "Ns001"
		suite.ExecuteDDL("create namespace if not exists " + suite.ns)
	}
}

// TestTableDDLRequest tests table DDL operations using TableRequest API.
func (suite *TableOpsTestSuite) TestTableDDLRequest() {
	tableName := suite.GetTableName("TableDDLTest")
	testCases := getTableDDLTestCases(tableName)

	// Do table DDL operations with a namespace qualified table.
	if test.IsOnPrem() {
		tableName = suite.GetNsTableName(suite.ns, "TableDDLTest")
		test02 := getTableDDLTestCases(tableName)
		testCases = append(testCases, test02...)
	}

	suite.doTableRequestTest(testCases)
}

// TestTableLimits tests operations that change table limits using TableRequest API.
func (suite *TableOpsTestSuite) TestTableLimits() {
	tableName := suite.table
	var testCases []*tableRequestTestCase

	if test.IsOnPrem() {
		// Change table limits operation is not supported for on-prem.
		testCases = []*tableRequestTestCase{
			{
				req: &nosqldb.TableRequest{
					TableName:   tableName,
					TableLimits: &nosqldb.TableLimits{5, 5, 2},
					Timeout:     test.OkTimeout,
				},
				expErr: nosqlerr.OperationNotSupported,
			},
		}

	} else {
		testCases = []*tableRequestTestCase{
			// Positive test cases.
			{
				req: &nosqldb.TableRequest{
					TableName:   tableName,
					TableLimits: &nosqldb.TableLimits{5, 5, 2},
					Timeout:     test.OkTimeout,
				},
			},
			{
				req: &nosqldb.TableRequest{
					TableName:   tableName,
					TableLimits: &nosqldb.TableLimits{6, 4, 1},
					Timeout:     test.OkTimeout,
				},
			},
			// Negative test cases.
			//
			// Invalid table name.
			{
				req: &nosqldb.TableRequest{
					TableName:   "",
					TableLimits: test.OkTableLimits,
					Timeout:     test.OkTimeout,
				},
				expErr: nosqlerr.IllegalArgument,
			},
			{
				req: &nosqldb.TableRequest{
					TableName:   "NotExistsTable",
					TableLimits: test.OkTableLimits,
					Timeout:     test.OkTimeout,
				},
				expErr: nosqlerr.TableNotFound,
			},
			// Invalid table limits.
			{
				req: &nosqldb.TableRequest{
					TableName:   tableName,
					TableLimits: nil,
					Timeout:     test.OkTimeout,
				},
				expErr: nosqlerr.IllegalArgument,
			},
			{
				req: &nosqldb.TableRequest{
					TableName:   tableName,
					TableLimits: test.BadTableLimits,
					Timeout:     test.OkTimeout,
				},
				expErr: nosqlerr.IllegalArgument,
			},
			// Invalid timeout.
			{
				req: &nosqldb.TableRequest{
					TableName:   tableName,
					TableLimits: test.OkTableLimits,
					Timeout:     test.BadTimeout,
				},
				expErr: nosqlerr.IllegalArgument,
			},
		}
	}

	suite.doTableRequestTest(testCases)
}

// TestWaitForCompletion performs tests for the WaitForCompletion API.
func (suite *TableOpsTestSuite) TestWaitForCompletion() {
	tableName := suite.table
	testCases := []*struct {
		desc     string
		tableRes *nosqldb.TableResult
		client   *nosqldb.Client
		timeout  time.Duration
		delay    time.Duration
		expErr   nosqlerr.ErrorCode
	}{
		// nil TableResult
		{
			"nil TableResult",
			nil,
			suite.Client,
			test.OkTimeout,
			time.Second,
			nosqlerr.IllegalArgument,
		},
		// nil client
		{
			"nil Client",
			&nosqldb.TableResult{TableName: tableName, OperationID: "1234", State: types.Creating},
			nil,
			test.OkTimeout,
			time.Second,
			nosqlerr.IllegalArgument,
		},
		{
			"empty OperationID",
			&nosqldb.TableResult{TableName: tableName, OperationID: "", State: types.Creating},
			suite.Client,
			test.WaitTimeout,
			time.Second,
			nosqlerr.IllegalArgument,
		},
		{
			"pollInterval is less than 1ms",
			&nosqldb.TableResult{TableName: tableName, OperationID: "1234", State: types.Creating},
			suite.Client,
			time.Second,
			time.Millisecond - 1,
			nosqlerr.IllegalArgument,
		},
		{
			"waitTimeout is less than pollInterval",
			&nosqldb.TableResult{TableName: tableName, OperationID: "1234", State: types.Creating},
			suite.Client,
			500*time.Millisecond - 1, // wait timeout
			500 * time.Millisecond,   // delay
			nosqlerr.IllegalArgument,
		},
		{
			"table is in Active state",
			&nosqldb.TableResult{TableName: tableName, OperationID: "1234", State: types.Active},
			suite.Client,
			test.OkTimeout,
			time.Second,
			nosqlerr.NoError,
		},
		{
			"table is in Dropped state",
			&nosqldb.TableResult{TableName: tableName, OperationID: "1234", State: types.Dropped},
			suite.Client,
			test.OkTimeout,
			time.Second,
			nosqlerr.NoError,
		},
	}

	var err error
	var msg string
	var reqTableName string
	for i, r := range testCases {
		if r.tableRes != nil {
			reqTableName = r.tableRes.TableName
		} else {
			reqTableName = "<nil TableResult>"
		}

		msg = fmt.Sprintf("Testcase %d(%s): WaitForCompletion(tableName=%q) ",
			i+1, r.desc, reqTableName)
		_, err = r.tableRes.WaitForCompletion(r.client, r.timeout, r.delay)
		switch r.expErr {
		case nosqlerr.NoError:
			suite.NoErrorf(err, msg+"got error %v", err)

		default:
			suite.Truef(nosqlerr.Is(err, r.expErr),
				msg+"expect error: %v, got error: %v", r.expErr, err)
		}
	}
}

func (suite *TableOpsTestSuite) TestGetTable() {
	const numTables = 6
	const maxNumCols = 10
	const maxLimits = 5

	var table string
	var msgPrefix string
	var limits *nosqldb.TableLimits

	for i := 1; i <= numTables; i++ {
		table = suite.GetTableName(fmt.Sprintf("Table%d", i))
		if test.IsCloud() {
			limits = &nosqldb.TableLimits{
				ReadUnits:  uint(1 + rand.Intn(maxLimits)),
				WriteUnits: uint(1 + rand.Intn(maxLimits)),
				StorageGB:  uint(1 + rand.Intn(maxLimits)),
			}

		} else {
			limits = nil
			// Create tables in the specified namespace.
			if i%2 == 0 {
				table = suite.GetNsTableName(suite.ns, table)
			}
		}

		numCols := 1 + rand.Intn(maxNumCols)
		stmt := test.GenCreateTableStmt(table, numCols, "C")
		suite.ReCreateTable(table, stmt, limits)

		msgPrefix = fmt.Sprintf("Testcase %d: GetTable(table=%q) ", i, table)
		req := &nosqldb.GetTableRequest{
			TableName: table,
			Timeout:   test.OkTimeout,
		}
		res, err := suite.Client.GetTable(req)
		if !suite.NoErrorf(err, msgPrefix+"got error %v", err) {
			continue
		}

		suite.Equalf(table, res.TableName, msgPrefix+"got unexpected table name.")
		suite.Equalf(types.Active, res.State, msgPrefix+"got unexpected table state.")
		// Verify table limits are set as expected.
		if limits != nil {
			suite.Equalf(*limits, res.Limits, msgPrefix+"got unexpected table limits.")
		} else {
			// If test with on-prem server, the returned table limits should be zero.
			suite.Equalf(nosqldb.TableLimits{}, res.Limits, msgPrefix+"got unexpected table limits.")
		}
	}

	table = suite.table
	// Negative tests
	testCases := []struct {
		req    *nosqldb.GetTableRequest
		expErr nosqlerr.ErrorCode
	}{
		// nil request
		{nil, nosqlerr.IllegalArgument},
		// invalid table name
		{&nosqldb.GetTableRequest{TableName: ""}, nosqlerr.IllegalArgument},
		{&nosqldb.GetTableRequest{TableName: "not_exists_table"}, nosqlerr.TableNotFound},
		// invalid operation id
		{&nosqldb.GetTableRequest{TableName: table, OperationID: "not_exists_op_id"}, nosqlerr.IllegalArgument},
		// invalid timeout
		{&nosqldb.GetTableRequest{TableName: table, Timeout: test.BadTimeout}, nosqlerr.IllegalArgument},
	}

	for i, r := range testCases {
		msgPrefix = fmt.Sprintf("Testcase %d: GetTable(%#v) ", i+1, r.req)
		_, err := suite.Client.GetTable(r.req)
		if !suite.Errorf(err, msgPrefix+"should have failed with error %v, but succeeded.", r.expErr) {
			continue
		}

		suite.Truef(nosqlerr.Is(err, r.expErr), msgPrefix+"expect error: %v, got error: %v.", r.expErr, err)
	}
}

func (suite *TableOpsTestSuite) TestGetIndexes() {
	var table string
	// Create a table without specifying an explicit namespace.
	table = suite.GetTableName("TestUsers")
	suite.doGetIndexTest(table)

	if test.IsOnPrem() {
		// Create a table in the specified namespace.
		table = suite.GetNsTableName(suite.ns, "TestUsers")
		suite.doGetIndexTest(table)
	}
}

func (suite *TableOpsTestSuite) TestListTables() {
	var tableNames []string
	var tableLimits []*nosqldb.TableLimits
	var numLimit int
	numTables := 8
	// If -test.short flag is set.
	if testing.Short() {
		numTables = 4
	}

	// Create tables.
	if test.IsCloud() {
		tableNames, tableLimits = suite.CreateTables(numTables, "")
	} else {
		tableNames, tableLimits = suite.CreateTables(numTables/2, suite.ns)
		names, limits := suite.CreateTables(numTables-len(tableNames), "")
		tableNames = append(tableNames, names...)
		tableLimits = append(tableLimits, limits...)
	}

	testCases := []*nosqldb.ListTablesRequest{
		{},
		{Limit: 5},
		{StartIndex: 2, Limit: 8},
		{StartIndex: 2},
	}

	// test case number
	var n int
	var msgPrefix string
	var expectTableCnt int
	for _, req := range testCases {
		n++
		msgPrefix = fmt.Sprintf("Testcase %d: ListTables(%#v) ", n, req)
		res, err := suite.Client.ListTables(req)
		if !suite.NoErrorf(err, msgPrefix+"got error: %v.", err) {
			continue
		}

		// Check returned number of tables.
		actualTableCnt := len(res.Tables)
		numLimit = int(req.Limit)
		switch {
		case numLimit == 0, numLimit >= numTables:
			expectTableCnt = numTables
			// Assert actualTableCnt >= expectTableCnt as the NoSQL store
			// may contain more tables than those created by this test.
			suite.GreaterOrEqual(actualTableCnt, expectTableCnt,
				msgPrefix+"expect number of tables >= %d, got %d.",
				expectTableCnt, actualTableCnt)

		case numLimit < numTables:
			expectTableCnt = numLimit
			suite.Equalf(expectTableCnt, actualTableCnt, msgPrefix+"got unexpected number of tables.")
		}

		expectLastIdx := actualTableCnt + int(req.StartIndex)
		suite.Equalf(expectLastIdx, int(res.LastIndexReturned), msgPrefix+"unexpected last index returned")
	}

	// Get all tables.
	returnedTables := make(map[string]bool, numTables)
	req := &nosqldb.ListTablesRequest{
		Limit: uint(1 + rand.Intn(numTables)),
	}
	for {
		n++
		res, err := suite.Client.ListTables(req)
		if !suite.NoErrorf(err, "Testcase %d: ListTables(%#v) got error %v", n, req, err) {
			break
		}

		// No more tables available.
		if len(res.Tables) == 0 || res.LastIndexReturned == req.StartIndex {
			break
		}

		for _, table := range res.Tables {
			returnedTables[table] = true
		}

		req.StartIndex = res.LastIndexReturned
		req.Limit = uint(1 + rand.Intn(numTables))
	}

	// Check if the returned list includes the tables created in this test.
	for _, name := range tableNames {
		suite.Truef(returnedTables[name],
			"ListTables() should have included the table %q.", name)
	}

	// Negative tests.
	negTestCases := []struct {
		req    *nosqldb.ListTablesRequest
		expErr nosqlerr.ErrorCode
	}{
		// nil request
		{
			req:    nil,
			expErr: nosqlerr.IllegalArgument,
		},
		// Invalid timeout
		{
			req:    &nosqldb.ListTablesRequest{StartIndex: 1, Limit: 10, Timeout: test.BadTimeout},
			expErr: nosqlerr.IllegalArgument,
		},
	}

	for _, r := range negTestCases {
		n++
		_, err := suite.Client.ListTables(r.req)
		// Check if returned error is non-nil
		if suite.Errorf(err, "Testcase %d: ListTables(%#v) should have failed with error %v, but succeeded.",
			n, r.req, r.expErr) {

			// Check if returned error is expected.
			suite.Truef(nosqlerr.Is(err, r.expErr), "Testcase %d: ListTables(%#v) expect error: %v, got error: %v",
				n, r.req, r.expErr, err)
		}
	}
}

func (suite *TableOpsTestSuite) doGetIndexTest(table string) {
	const idx1 string = "idx1"
	const idx2 string = "iDx2"
	const idx3 string = "IDX3"
	const createTmpl string = "CREATE TABLE IF NOT EXISTS %s (" +
		"id INTEGER, " +
		"name STRING, " +
		"address STRING, " +
		"age INTEGER, " +
		"description JSON, " +
		"PRIMARY KEY(id))"

	var limits *nosqldb.TableLimits
	if test.IsCloud() {
		limits = test.OkTableLimits
	}

	// Create a table.
	createStmt := fmt.Sprintf(createTmpl, table)
	suite.ReCreateTable(table, createStmt, limits)

	// Get indexes on the table before any indexes are created.
	testCases := []*getIndexTestCase{
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				Timeout:   test.OkTimeout,
			},
			expIdxNum: 0,
		},
		// Index does not exist.
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx1,
			},
			expIdxNum: 0,
			expErr:    nosqlerr.IndexNotFound,
		},
	}

	suite.verifyGetIndex(testCases)

	// Create indexes on the table.
	statements := []string{
		fmt.Sprintf("create index %s on %s(name)", idx1, table),
		fmt.Sprintf("create index %s on %s(address, age)", idx2, table),
		// Create a JSON index.
		fmt.Sprintf("create index %s on %s(description.category as string)", idx3, table),
	}
	for _, stmt := range statements {
		suite.ExecuteTableDDL(stmt)
	}

	// Try to get indexes again after the indexes have been created.
	testCases = []*getIndexTestCase{
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
			},
			expIdxNum: 3,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx1,
			},
			expIdxNum: 1,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx2,
				Timeout:   test.OkTimeout,
			},
			expIdxNum: 1,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx3,
			},
			expIdxNum: 1,
		},
		// Verify table/index names are case-insensitive.
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: strings.ToUpper(table),
				IndexName: idx2,
			},
			expIdxNum: 1,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: strings.ToUpper(table),
				IndexName: strings.ToLower(idx2),
			},
			expIdxNum: 1,
		},
	}

	suite.verifyGetIndex(testCases)

	// Drop idx1 and idx2 on the table.
	statements = []string{
		fmt.Sprintf("drop index %s on %s", idx1, table),
		fmt.Sprintf("drop index %s on %s", idx2, table),
	}
	for _, stmt := range statements {
		suite.ExecuteTableDDL(stmt)
	}

	// Try to get indexes after idx1 and idx2 are dropped.
	testCases = []*getIndexTestCase{
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
			},
			expIdxNum: 1,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx1,
				Timeout:   test.OkTimeout,
			},
			expIdxNum: 0,
			expErr:    nosqlerr.IndexNotFound,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx2,
			},
			expIdxNum: 0,
			expErr:    nosqlerr.IndexNotFound,
		},
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx3,
			},
			expIdxNum: 1,
		},
	}

	suite.verifyGetIndex(testCases)

	// Negative tests.
	negativeTestCases := []*getIndexTestCase{
		// nil request
		{
			req:    nil,
			expErr: nosqlerr.IllegalArgument,
		},
		// Invalid table name.
		{
			req:    &nosqldb.GetIndexesRequest{TableName: ""},
			expErr: nosqlerr.IllegalArgument,
		},
		{
			req:    &nosqldb.GetIndexesRequest{TableName: "not_exists_table"},
			expErr: nosqlerr.TableNotFound,
		},
		// Invalid index name.
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: "not_exists_index",
			},
			expErr: nosqlerr.IndexNotFound,
		},
		// Invalid timeout.
		{
			req: &nosqldb.GetIndexesRequest{
				TableName: table,
				IndexName: idx1,
				Timeout:   test.BadTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
	}

	suite.verifyGetIndex(negativeTestCases)
}

// verifyGetIndex calls GetIndexes for the specified test cases and verifies results.
func (suite *TableOpsTestSuite) verifyGetIndex(testCases []*getIndexTestCase) {
	for _, r := range testCases {
		res, err := suite.Client.GetIndexes(r.req)
		switch r.expErr {
		case nosqlerr.NoError:
			suite.NoErrorf(err, "GetIndexes(req=%#v) got error: %v", r.req, err)

		default:
			if !suite.Truef(nosqlerr.Is(err, r.expErr),
				"GetIndexes(req=%#v) expect error: %v, got error: %v",
				r.req, r.expErr, err) {
				continue
			}

			if res != nil {
				// Checks if the returned number of indexes is as expected.
				suite.Equalf(r.expIdxNum, len(res.Indexes),
					"GetIndexes(req=%#v) got unexpected number of indexes.", r.req)
			}
		}
	}
}

func (suite *TableOpsTestSuite) doTableRequestTest(testCases []*tableRequestTestCase) {
	for _, r := range testCases {
		res, err := suite.Client.DoTableRequestAndWait(r.req, test.WaitTimeout, time.Second)
		switch r.expErr {
		case nosqlerr.NoError:
			if suite.NoErrorf(err, "TableRequest(req=%#v) got error: %v", r.req, err) {
				// Verify table limits.
				if test.IsOnPrem() {
					suite.Equalf(nosqldb.TableLimits{}, res.Limits,
						"the returned table limits %#v is wrong.", res.Limits)

				} else {
					if r.req != nil && r.req.TableLimits != nil {
						suite.Equalf(*r.req.TableLimits, res.Limits,
							"the returned table limits %#v is wrong.", res.Limits)
					}
				}
			}

		default:
			suite.Truef(nosqlerr.Is(err, r.expErr), "TableRequest(req=%v) expect error: %v, got error: %v",
				r.req, r.expErr, err)
		}
	}
}

// getTableDDLTestCases returns a list of test cases for table DDL operations.
// The test cases are applicable for both cloud and on-premise.
func getTableDDLTestCases(table string) []*tableRequestTestCase {
	idx1 := "idx1"
	idx2 := "idx2"
	idx3 := "idx3"
	tmpl := "create table %s (id integer, c1 string, c2 long, c3 float, primary key(id))"
	createTable := fmt.Sprintf(tmpl, table)
	createTableIf := fmt.Sprintf(tmpl, "if not exists "+table)
	addColumnC4 := fmt.Sprintf("alter table %s (add c4 integer)", table)
	addColumnC5 := fmt.Sprintf("alter table %s (add c5 integer)", table)
	dropColumnC5 := fmt.Sprintf("alter table %s (drop c5)", table)
	createIndexOnC1 := fmt.Sprintf("create index %s on %s(c1)", idx1, table)
	createIndexOnC23 := fmt.Sprintf("create index %s on %s(c2, c3)", idx2, table)
	createIndexOnC4 := fmt.Sprintf("create index %s on %s(c4)", idx3, table)
	dropIndex := fmt.Sprintf("drop index %s on %s", idx1, table)
	dropIndexIf := fmt.Sprintf("drop index if exists %s on %s", idx1, table)
	dropTable := fmt.Sprintf("drop table %s", table)
	dropTableIf := fmt.Sprintf("drop table if exists %s", table)

	okTimeout := test.OkTimeout
	badTimeout := test.BadTimeout
	okTableLimits := test.OkTableLimits

	return []*tableRequestTestCase{
		// Postive test cases.
		//
		// Drop table if exists.
		{
			req: &nosqldb.TableRequest{
				Statement: fmt.Sprintf("drop table if exists %s", table),
				Timeout:   okTimeout,
			},
		},
		// Create table.
		{
			req: &nosqldb.TableRequest{
				Statement:   createTable,
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
		},
		// Create table if not exists.
		{
			req: &nosqldb.TableRequest{
				Statement:   createTableIf,
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
		},
		// Create index.
		{
			req: &nosqldb.TableRequest{
				Statement: createIndexOnC1,
				Timeout:   okTimeout,
			},
		},
		// Create index. Using default timeout.
		{
			req: &nosqldb.TableRequest{
				Statement: createIndexOnC23,
			},
		},
		// Add a new column c4.
		{
			req: &nosqldb.TableRequest{
				Statement: addColumnC4,
				Timeout:   okTimeout,
			},
		},
		// Create an index on column c4
		{
			req: &nosqldb.TableRequest{
				Statement: createIndexOnC4,
				Timeout:   okTimeout,
			},
		},
		// Add a new column c5.
		{
			req: &nosqldb.TableRequest{
				Statement: addColumnC5,
				Timeout:   okTimeout,
			},
		},
		// Drop column c5
		{
			req: &nosqldb.TableRequest{
				Statement: dropColumnC5,
				Timeout:   okTimeout,
			},
		},
		// Negative test cases.
		//
		// nil request
		{
			req:    nil,
			expErr: nosqlerr.IllegalArgument,
		},
		// table already exists
		{
			req: &nosqldb.TableRequest{
				Statement:   createTable,
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.TableExists,
		},
		// An empty table ddl statement.
		{
			req: &nosqldb.TableRequest{
				Statement:   "",
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Invalid statements.
		{
			req: &nosqldb.TableRequest{
				Statement:   "create table",
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		{
			req: &nosqldb.TableRequest{
				Statement:   "create tab x (id integer, primary key(id))",
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Invalid table names.
		{
			req: &nosqldb.TableRequest{
				Statement:   "create table 123 (id integer, primary key(id))",
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		{
			req: &nosqldb.TableRequest{
				Statement:   "create table name$ (id integer, primary key(id))",
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Full text index is not supported for cloud.
		// It is supported for on-prem if an Elasticsearch cluster is registered
		// with the store.
		{
			req: &nosqldb.TableRequest{
				Statement: fmt.Sprintf("create fulltext index textIdx on %s(c1)", table),
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Modify column is not supported.
		{
			req: &nosqldb.TableRequest{
				Statement: fmt.Sprintf("alter table %s (modify c4 long)", table),
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Create an index on a non exists table.
		{
			req: &nosqldb.TableRequest{
				Statement: "create index idx9 on not_exists_table(c3)",
				Timeout:   okTimeout,
			},
			expErr: nosqlerr.TableNotFound,
		},
		// Create an index on a non exists column.
		{
			req: &nosqldb.TableRequest{
				Statement: fmt.Sprintf("create index idx10 on %s(NoSuchColumn)", table),
				Timeout:   okTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Specify an invalid timeout.
		{
			req: &nosqldb.TableRequest{
				Statement:   createTable,
				TableLimits: okTableLimits,
				Timeout:     badTimeout,
			},
			expErr: nosqlerr.IllegalArgument,
		},
		// Table already exists.
		{
			req: &nosqldb.TableRequest{
				Statement:   createTable,
				TableLimits: okTableLimits,
				Timeout:     okTimeout,
			},
			expErr: nosqlerr.TableExists,
		},
		// Index already exists.
		{
			req: &nosqldb.TableRequest{
				Statement: createIndexOnC1,
				Timeout:   okTimeout,
			},
			expErr: nosqlerr.IndexExists,
		},
		// Index not found
		{
			req: &nosqldb.TableRequest{
				Statement: "drop index not_exists_idx on " + table,
				Timeout:   okTimeout,
			},
			expErr: nosqlerr.IndexNotFound,
		},
		// Drop indexes.
		{
			req: &nosqldb.TableRequest{
				Statement: dropIndex,
				Timeout:   okTimeout,
			},
		},
		{
			req: &nosqldb.TableRequest{
				Statement: dropIndexIf,
				Timeout:   okTimeout,
			},
		},
		// Drop tables.
		{
			req: &nosqldb.TableRequest{
				Statement: dropTable,
				Timeout:   okTimeout,
			},
		},
		{
			req: &nosqldb.TableRequest{
				Statement: dropTable,
				Timeout:   okTimeout,
			},
			expErr: nosqlerr.TableNotFound,
		},
		{
			req: &nosqldb.TableRequest{
				Statement: dropTableIf,
				Timeout:   okTimeout,
			},
		},
	}

}

func TestTableOperations(t *testing.T) {
	tests := &TableOpsTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, tests)
}
