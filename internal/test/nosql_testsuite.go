//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package test

import (
	"fmt"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// NoSQLTestSuite provides generic utility methods and configurations for test suites.
//
// It should be embedded into test suites that test the cases where a NoSQL client is needed.
type NoSQLTestSuite struct {
	suite.Suite
	*Config
	Client    *nosqldb.Client
	allTables []string
}

// NewNoSQLTestSuite creates a NoSQLTestSuite instance.
func NewNoSQLTestSuite() *NoSQLTestSuite {
	cfg, err := getConfig()
	if err != nil {
		panic(err)
	}

	client, err := createClient(cfg)
	if err != nil {
		panic(err)
	}

	return &NoSQLTestSuite{
		Config: cfg,
		Client: client,
	}
}

// SetupSuite is used to setup test resources before test.
//
// This implements the suite.SetupAllSuite interface defined in the testify package.
func (suite *NoSQLTestSuite) SetupSuite() {
	if interceptor != nil {
		err := interceptor.OnSetupTestSuite()
		suite.Require().NoErrorf(err, "interceptor.OnSetupTestSuite(): %v", err)
	}
}

// TearDownSuite is used to clean up test resources after test.
//
// This implements the suite.TearDownAllSuite interface defined in the testify package.
func (suite *NoSQLTestSuite) TearDownSuite() {
	if suite.DropTablesOnTearDown {
		for _, table := range suite.allTables {
			stmt := "DROP TABLE IF EXISTS " + table
			req := &nosqldb.TableRequest{
				Statement: stmt,
			}
			// Do not wait for completion of the drop table operation.
			suite.Client.DoTableRequest(req)
		}
	}

	if interceptor != nil {
		interceptor.OnTearDownTestSuite()
	}

	suite.Client.Close()
}

// CreateTable creates a table using the specified statement and table limit.
// The table limit is ignored when test against the on-premise NoSQL server.
func (suite *NoSQLTestSuite) CreateTable(createStmt string, limits *nosqldb.TableLimits) {
	req := &nosqldb.TableRequest{
		Statement: createStmt,
	}

	if suite.IsCloud() {
		req.TableLimits = limits
	}

	res, err := suite.Client.DoTableRequestAndWait(req, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "%q: got error %v.", createStmt, err)
	if res != nil {
		suite.AddToTables(res.TableName)
	}
}

// ReCreateTable drops and re-creates a table.
func (suite *NoSQLTestSuite) ReCreateTable(table, createStmt string, limits *nosqldb.TableLimits) {
	suite.DropTable(table, true)
	suite.CreateTable(createStmt, limits)
}

// CreateTables creates the specified number of tables in namespace, returns
// table names and limits of the tables that are created.
// If the specified namespace nsName is empty, the tables will be created in
// the default namespace.
func (suite *NoSQLTestSuite) CreateTables(numTables int, nsName string, offset int) (tables []string, tableLimits []*nosqldb.TableLimits) {
	var limits *nosqldb.TableLimits
	tables = make([]string, 0, numTables)
	tableLimits = make([]*nosqldb.TableLimits, 0, numTables)

	for i := 1; i <= numTables; i++ {
		tableName := suite.GetNsTableName(nsName, fmt.Sprintf("Test%d", i+offset))
		if suite.IsCloud() {
			limits = OkTableLimits
		} else {
			limits = nil
		}

		stmt := fmt.Sprintf(OkCreateTableTmpl, tableName)
		suite.ReCreateTable(tableName, stmt, limits)
		tables = append(tables, tableName)
		tableLimits = append(tableLimits, limits)
	}

	return
}

// GetTableName returns a table name that may have a prefix if the tablePrefix
// is specified in test configuration.
func (suite *NoSQLTestSuite) GetTableName(table string) string {
	return suite.TablePrefix + table
}

// GetNsTableName returns a namespace qualified table name.
// The table name may have a prefix if the tablePrefix is specified in test configuration.
func (suite *NoSQLTestSuite) GetNsTableName(ns, table string) string {
	if len(ns) == 0 {
		return suite.GetTableName(table)
	}

	return ns + ":" + suite.GetTableName(table)
}

// DropTable drops the table.
func (suite *NoSQLTestSuite) DropTable(table string, dropIfExists bool) {
	var stmt string
	if dropIfExists {
		stmt = "DROP TABLE IF EXISTS " + table
	} else {
		stmt = "DROP TABLE " + table
	}

	req := &nosqldb.TableRequest{
		Statement: stmt,
	}

	// BUG(zehliu): DoTableRequestAndWait does not seem to work for MiniCloud.
	_, err := suite.Client.DoTableRequestAndWait(req, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "%q: got error %v.", stmt, err)
}

// ExecuteDDL executes the specified DDL statement using a SystemRequest.
func (suite *NoSQLTestSuite) ExecuteDDL(stmt string) {
	_, err := suite.Client.DoSystemRequestAndWait(stmt, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "failed to execute %q, got error %v.", stmt, err)
}

// ExecuteTableDDL executes the specified DDL statement using a TableRequest.
func (suite *NoSQLTestSuite) ExecuteTableDDL(stmt string) {
	req := &nosqldb.TableRequest{Statement: stmt}
	_, err := suite.Client.DoTableRequestAndWait(req, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "failed to execute %q, got error %v.", stmt, err)
}

// ExecuteQueryStmt executes the query statement.
func (suite *NoSQLTestSuite) ExecuteQueryStmt(stmt string) ([]*types.MapValue, error) {
	return suite.ExecuteQueryRequest(&nosqldb.QueryRequest{Statement: stmt})
}

// ExecuteQueryRequest executes the query request.
func (suite *NoSQLTestSuite) ExecuteQueryRequest(queryReq *nosqldb.QueryRequest) ([]*types.MapValue, error) {
	return ExecuteQueryRequest(suite.Client, queryReq)
}

// AddToTables adds the specified table into a table list, in which all tables
// would be dropped on TearDownSuite if DropTablesOnTearDown is specified in test configuration.
func (suite *NoSQLTestSuite) AddToTables(table string) {
	if suite.allTables == nil {
		suite.allTables = make([]string, 0, 5)
	}

	suite.allTables = append(suite.allTables, table)
}

// AssertZeroReadWriteKB asserts the operation consumed zero readKB/writeKB.
func (suite *NoSQLTestSuite) AssertZeroReadWriteKB(res nosqldb.Result) {
	cap, err := res.ConsumedCapacity()
	suite.NoErrorf(err, "Result.ConsumedCapacity() got error %v", err)
	AssertReadKB(suite.Assert(), 0, cap.ReadKB, cap.ReadUnits, 0, false)
	AssertWriteKB(suite.Assert(), 0, cap.WriteKB)
}

// AssertReadWriteKB checks if the readKB/writeKB are as expected.
func (suite *NoSQLTestSuite) AssertReadWriteKB(res nosqldb.Result, expReadKB, expWriteKB, prepCost int, isAbsolute bool) {
	cap, err := res.ConsumedCapacity()
	suite.NoErrorf(err, "Result.ConsumedCapacity() got error %v", err)
	AssertReadKB(suite.Assert(), expReadKB, cap.ReadKB, cap.ReadUnits, prepCost, isAbsolute)
	AssertWriteKB(suite.Assert(), expWriteKB, cap.WriteKB)
}
