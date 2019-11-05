//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
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

func NewNoSQLTestSuite() *NoSQLTestSuite {
	cfg, err := getConfig()
	if err != nil {
		panic(err)
	}

	client, err := getClient(cfg)
	if err != nil {
		panic(err)
	}

	return &NoSQLTestSuite{
		Config: cfg,
		Client: client,
	}
}

// This implements the suite.SetupAllSuite interface defined in testify package.
func (suite *NoSQLTestSuite) SetupSuite() {
	if interceptor != nil {
		err := interceptor.OnSetupTestSuite()
		suite.Require().NoErrorf(err, "interceptor.OnSetupTestSuite(): %v", err)
	}
}

// This implements the suite.TearDownAllSuite interface defined in testify package.
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

func (suite *NoSQLTestSuite) CreateTable(createStmt string, limits *nosqldb.TableLimits) {
	req := &nosqldb.TableRequest{
		Statement: createStmt,
	}

	if IsCloud() {
		req.TableLimits = limits
	}

	res, err := suite.Client.DoTableRequestAndWait(req, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "%q: got error %v.", createStmt, err)
	if res != nil {
		suite.AddToTables(res.TableName)
	}
}

func (suite *NoSQLTestSuite) ReCreateTable(table, createStmt string, limits *nosqldb.TableLimits) {
	suite.DropTable(table, true)
	suite.CreateTable(createStmt, limits)
}

// CreateTables creates the specified number of tables in namespace, returns
// table names and limits of the tables that are created.
// If the specified namespace nsName is empty, the tables will be created in
// the default namespace.
func (suite *NoSQLTestSuite) CreateTables(numTables int, nsName string) (tables []string, tableLimits []*nosqldb.TableLimits) {
	var limits *nosqldb.TableLimits
	tables = make([]string, 0, numTables)
	tableLimits = make([]*nosqldb.TableLimits, 0, numTables)

	for i := 1; i <= numTables; i++ {
		tableName := suite.GetNsTableName(nsName, fmt.Sprintf("Test%d", i))
		if IsCloud() {
			limits = OkTableLimits
		} else {
			limits = nil
		}

		stmt := fmt.Sprintf(OkCreateTableTmpl, tableName)
		// suite.CreateTable(tableName, stmt, limits)
		suite.ReCreateTable(tableName, stmt, limits)
		tables = append(tables, tableName)
		tableLimits = append(tableLimits, limits)
	}

	return
}

func (suite *NoSQLTestSuite) GetTableName(table string) string {
	return suite.TablePrefix + table
}

func (suite *NoSQLTestSuite) GetNsTableName(ns, table string) string {
	if len(ns) == 0 {
		return suite.GetTableName(table)
	}

	return ns + ":" + suite.GetTableName(table)
}

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

	// BUG(zehliu): A workaround for MiniCloud bug temporarily.
	if suite.Mode == "minicloud" {
		res, err := suite.Client.DoTableRequest(req)
		suite.Require().NoErrorf(err, "%q: got error %v.", stmt, err)
		_, err = res.WaitForState(suite.Client, types.Dropped, 30*time.Second, time.Second)
		suite.Require().NoErrorf(err, "%q: got error %v.", stmt, err)

	} else {
		_, err := suite.Client.DoTableRequestAndWait(req, 30*time.Second, time.Second)
		suite.Require().NoErrorf(err, "%q: got error %v.", stmt, err)
	}
}

func (suite *NoSQLTestSuite) ExecuteDDL(stmt string) {
	_, err := suite.Client.DoSystemRequestAndWait(stmt, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "failed to execute %q, got error %v.", stmt, err)
}

func (suite *NoSQLTestSuite) ExecuteTableDDL(stmt string) {
	req := &nosqldb.TableRequest{Statement: stmt}
	_, err := suite.Client.DoTableRequestAndWait(req, 30*time.Second, time.Second)
	suite.Require().NoErrorf(err, "failed to execute %q, got error %v.", stmt, err)
}

func (suite *NoSQLTestSuite) ExecuteQueryStmt(stmt string) ([]*types.MapValue, error) {
	return suite.ExecuteQueryRequest(&nosqldb.QueryRequest{Statement: stmt})
}

func (suite *NoSQLTestSuite) ExecuteQueryRequest(queryReq *nosqldb.QueryRequest) ([]*types.MapValue, error) {
	return ExecuteQueryRequest(suite.Client, queryReq)
}

// AddToTables adds the specified table into a table list, in which all tables
// would be dropped on TearDownSuite if TestConfig.dropTablesOnTearDown is set.
func (suite *NoSQLTestSuite) AddToTables(table string) {
	if suite.allTables == nil {
		suite.allTables = make([]string, 0, 5)
	}

	suite.allTables = append(suite.allTables, table)
}

func (suite *NoSQLTestSuite) AssertZeroReadWriteKB(res nosqldb.Result) {
	cap, err := res.ConsumedCapacity()
	suite.NoErrorf(err, "Result.ConsumedCapacity() got error %v", err)
	AssertReadKB(suite.Assert(), 0, cap.ReadKB, cap.ReadUnits, 0, false)
	AssertWriteKB(suite.Assert(), 0, cap.WriteKB)
}

func (suite *NoSQLTestSuite) AssertReadWriteKB(res nosqldb.Result, expReadKB, expWriteKB, prepCost int, isAbsolute bool) {
	cap, err := res.ConsumedCapacity()
	suite.NoErrorf(err, "Result.ConsumedCapacity() got error %v", err)
	AssertReadKB(suite.Assert(), expReadKB, cap.ReadKB, cap.ReadUnits, prepCost, isAbsolute)
	AssertWriteKB(suite.Assert(), expWriteKB, cap.WriteKB)
}

// DummyAccessTokenProvider represents a dummy access token provider, which is used by tests.
// It implements the AccessTokenProvider interface.
type DummyAccessTokenProvider struct {
	TenantId string
}

func (p DummyAccessTokenProvider) AuthorizationScheme() string {
	return auth.BearerToken
}

func (p DummyAccessTokenProvider) AuthorizationString(req auth.Request) (string, error) {
	return auth.BearerToken + " " + p.TenantId, nil
}

func (p DummyAccessTokenProvider) SignHttpRequest(req *http.Request) error {
	return nil
}

func (p DummyAccessTokenProvider) Close() error {
	return nil
}
