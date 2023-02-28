//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// +build cloud onprem

package nosqldb_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// Smoke test.
type SmokeTestSuite struct {
	*test.NoSQLTestSuite
}

func (suite SmokeTestSuite) TestSmoke() {
	var err error
	var stmt string

	tableName := suite.GetTableName("SmokeTest")
	baseTime := time.Now()

	// Drop table.
	stmt = "DROP TABLE IF EXISTS " + tableName
	tableReq := &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err := suite.Client.DoTableRequestAndWait(tableReq, 20*time.Second, 1*time.Second)
	suite.Require().NoErrorf(err, "\"%s\": %v", stmt, err)
	suite.Require().Equalf(types.Dropped, tableRes.State, "unexpected state for table \"%s\"", tableName)

	// Get table information.
	getTableReq := &nosqldb.GetTableRequest{
		TableName: tableName,
	}
	_, err = suite.Client.GetTable(getTableReq)
	suite.Require().Truef(nosqlerr.IsTableNotFound(err), "GetTable(table=%s) expect TableNotFound, got %v", tableName, err)

	// Create table.
	stmt = fmt.Sprintf("CREATE TABLE %s (id INTEGER, sid INTEGER, "+
		"cstr STRING, clong LONG, cdoub DOUBLE, cts TIMESTAMP(9), "+
		"PRIMARY KEY(SHARD(sid), id))", tableName)
	tableLimits := &nosqldb.TableLimits{
		ReadUnits:  6000,
		WriteUnits: 4000,
		StorageGB:  5,
	}

	tableReq = &nosqldb.TableRequest{
		Statement:   stmt,
		TableLimits: tableLimits,
	}
	tableRes, err = suite.Client.DoTableRequest(tableReq)
	suite.Require().NoErrorf(err, "\"%s\": %v", stmt, err)
	tableRes, err = tableRes.WaitForCompletion(suite.Client, 20*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "WaitForCompletion(table=%s): %v", tableName, err)
	suite.Require().Equalf(types.Active, tableRes.State, "unexpected state for table \"%s\"", tableName)

	// Try to get a non-exist row.
	id := 1
	sid := 101

	key := &types.MapValue{}
	key.Put("id", id).Put("sid", sid)
	getReq := &nosqldb.GetRequest{
		TableName: tableName,
		Key:       key,
	}
	getRes, err := suite.Client.Get(getReq)
	suite.Require().NoErrorf(err, "Get(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().Falsef(getRes.RowExists(), "Get(id=%d, sid=%d) should not return a non-exist row", id, sid)

	// Put a row.
	cstr := "row-Put-" + strconv.Itoa(id)
	value := &types.MapValue{}
	value.Put("id", id).Put("sid", sid)
	value.Put("clong", int64(id)).Put("cstr", cstr).Put("cdoub", 1.23)
	value.Put("cts", baseTime.Add(time.Duration(int64(id))))
	putReq := &nosqldb.PutRequest{
		TableName: tableName,
		Value:     value,
	}
	putRes, err := suite.Client.Put(putReq)
	suite.Require().NoErrorf(err, "Put(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().NotNilf(putRes.Version, "Put(id=%d, sid=%d) returns nil Version", id, sid)

	// Get the row.
	key = &types.MapValue{}
	key.Put("id", id).Put("sid", sid)
	getReq = &nosqldb.GetRequest{
		TableName: tableName,
		Key:       key,
	}
	getRes, err = suite.Client.Get(getReq)
	suite.Require().NoErrorf(err, "Get(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().Truef(getRes.RowExists(), "Get(id=%d, sid=%d) failed to get the row", id, sid)
	retStr, _ := getRes.Value.GetString("cstr")
	suite.Require().Equalf(cstr, retStr, "unexpected value for \"cstr\"")

	// PutIfPresent
	cstr = "row-PutIfPresent-" + strconv.Itoa(id)
	value.Put("cstr", cstr)
	putReq = &nosqldb.PutRequest{
		TableName: tableName,
		Value:     value,
		PutOption: types.PutIfPresent,
		TTL: &types.TimeToLive{
			Unit:  types.Hours,
			Value: int64(2),
		},
	}
	putRes, err = suite.Client.Put(putReq)
	suite.Require().NoErrorf(err, "PutIfPresent(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().NotNilf(putRes.Version, "PutIfPresent(id=%d, sid=%d) returns nil Version", id, sid)

	currVersion := putRes.Version

	// Get and check the updated row.
	getRes, err = suite.Client.Get(getReq)
	suite.Require().NoErrorf(err, "Get(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().Truef(getRes.RowExists(), "Get(id=%d, sid=%d): cannot find the row", id, sid)
	retStr, _ = getRes.Value.GetString("cstr")
	suite.Require().Equalf(cstr, retStr, "unexpected value for \"cstr\"")

	// PutIfVersion
	cstr = "row-PutIfVersion-" + strconv.Itoa(id)
	value.Put("cstr", cstr)
	putReq = &nosqldb.PutRequest{
		TableName:    tableName,
		Value:        value,
		PutOption:    types.PutIfVersion,
		MatchVersion: currVersion,
	}
	putRes, err = suite.Client.Put(putReq)
	suite.Require().NoErrorf(err, "PutIfVersion(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().NotNilf(putRes.Version, "PutIfVersion(id=%d, sid=%d) returns nil Version", id, sid)

	// PutIfAbsent
	id = 2
	cstr = "row-PutIfAbsent-" + strconv.Itoa(id)
	value.Put("id", id).Put("sid", sid).Put("cstr", cstr)
	putReq = &nosqldb.PutRequest{
		TableName: tableName,
		Value:     value,
		PutOption: types.PutIfAbsent,
	}
	putRes, err = suite.Client.Put(putReq)
	suite.Require().NoErrorf(err, "PutIfAbsent(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().NotNilf(putRes.Version, "PutIfAbsent(id=%d, sid=%d) returns nil Version", id, sid)

	currVersion = putRes.Version

	// Delete the row.
	id = 1
	sid = 101
	key = &types.MapValue{}
	key.Put("id", id).Put("sid", sid)
	delReq := &nosqldb.DeleteRequest{
		TableName: tableName,
		Key:       key,
	}
	delRes, err := suite.Client.Delete(delReq)
	suite.Require().NoErrorf(err, "Delete(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().Truef(delRes.Success, "Delete(id=%d, sid=%d) failed", id, sid)

	// DeleteIfVersion.
	id = 2
	sid = 101
	key = &types.MapValue{}
	key.Put("id", id).Put("sid", sid)
	delReq = &nosqldb.DeleteRequest{
		TableName:    tableName,
		Key:          key,
		MatchVersion: currVersion,
	}
	delRes, err = suite.Client.Delete(delReq)
	suite.Require().NoErrorf(err, "DeleteIfVersion(id=%d, sid=%d): %v", id, sid, err)
	suite.Require().Truef(delRes.Success, "DeleteIfVersion(id=%d, sid=%d) failed", id, sid)

	// Put more rows.
	n := 10
	id = 10
	sid = 102
	wmReq := &nosqldb.WriteMultipleRequest{
		TableName: tableName,
	}
	for i := 0; i < n; i++ {
		id++
		cstr = "row-WriteMultiple-" + strconv.Itoa(id)
		value = &types.MapValue{}
		value.Put("id", id).Put("sid", sid)
		value.Put("clong", int64(id)).Put("cstr", cstr).Put("cdoub", 1.23)
		value.Put("cts", baseTime.Add(time.Duration(int64(id))))

		putReq = &nosqldb.PutRequest{
			TableName: tableName,
			Value:     value,
		}
		wmReq.AddPutRequest(putReq, true)
	}

	wmRes, err := suite.Client.WriteMultiple(wmReq)
	suite.Require().NoErrorf(err, "WriteMultiple(numOps=%d, sid=%d): %v", n, sid, err)
	suite.Require().Equalf(-1, wmRes.FailedOperationIndex, "unexpected FailedOperationIndex")
	for i := 0; i < n; i++ {
		suite.Require().Truef(wmRes.ResultSet[i].Success, "WriteMultiple(opNum=%d) failed", i+1)
	}

	// Create indexes on the "cstr" and "clong" columns.
	idx1 := "idx1"
	stmt = fmt.Sprintf("CREATE INDEX %s ON %s(%s)", idx1, tableName, "cstr")
	tableReq = &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err = suite.Client.DoTableRequestAndWait(tableReq, 20*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "\"%s\": %v", stmt, err)
	suite.Require().Equalf(types.Active, tableRes.State, "unexpected table state")

	idx2 := "idx2"
	stmt = fmt.Sprintf("CREATE INDEX %s ON %s(%s)", idx2, tableName, "clong")
	tableReq = &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err = suite.Client.DoTableRequest(tableReq)
	suite.Require().NoErrorf(err, "\"%s\": %v", stmt, err)
	tableRes, err = tableRes.WaitForCompletion(suite.Client, 20*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "WaitForCompletion(op=createIndex(idxName=%s, tableName=%s)): %v", idx2, tableName, err)
	suite.Require().Equalf(types.Active, tableRes.State, "unexpected table state")

	// Query
	id = 11
	sid = 102
	stmt = fmt.Sprintf("SELECT clong, cstr FROM %s WHERE id=%d AND sid=%d", tableName, id, sid)
	queryReq := &nosqldb.QueryRequest{
		Statement:   stmt,
		Consistency: types.Absolute,
	}
	queryRes, err := suite.Client.Query(queryReq)
	suite.Require().NoErrorf(err, "Query(stmt=%s): %v", stmt, err)
	results, err := queryRes.GetResults()
	suite.Require().NoErrorf(err, "QueryResult.GetResults() got error %v", err)
	suite.Require().Equalf(1, len(results), "unexpected number of results")
	retLong, _ := results[0].GetInt64("clong")
	suite.Require().Equalf(int64(id), retLong, "unexpected value for \"clong\"")
	retStr, _ = results[0].GetString("cstr")
	cstr = "row-WriteMultiple-" + strconv.Itoa(id)
	suite.Require().Equalf(cstr, retStr, "unexpected value for \"cstr\"")

	// Prepare and query.
	id = 12
	sid = 102
	stmt = fmt.Sprintf("SELECT clong, cstr FROM %s WHERE id=%d AND sid=%d", tableName, id, sid)
	prepReq := &nosqldb.PrepareRequest{
		Statement: stmt,
	}
	prepRes, err := suite.Client.Prepare(prepReq)
	suite.Require().NoErrorf(err, "Prepare(stmt=%s): %v", stmt, err)

	queryReq = &nosqldb.QueryRequest{
		PreparedStatement: &prepRes.PreparedStatement,
		Consistency:       types.Absolute,
	}
	queryRes, err = suite.Client.Query(queryReq)
	suite.Require().NoErrorf(err, "PreparedQuery(stmt=%s): %v", stmt, err)

	results, err = queryRes.GetResults()
	suite.Require().NoErrorf(err, "PreparedQuery.GetResults() got error %v", err)

	suite.Require().Equalf(1, len(results), "unexpected number of results")
	retLong, _ = results[0].GetInt64("clong")
	suite.Require().Equalf(int64(id), retLong, "unexpected value for \"clong\"")
	retStr, _ = results[0].GetString("cstr")
	cstr = "row-WriteMultiple-" + strconv.Itoa(id)
	suite.Require().Equalf(cstr, retStr, "unexpected value for \"cstr\"")

	// Query by index.
	stmt = fmt.Sprintf("DECLARE $cl LONG; SELECT id, sid FROM %s WHERE clong=$cl", tableName)
	prepReq = &nosqldb.PrepareRequest{
		Statement: stmt,
	}
	prepRes, err = suite.Client.Prepare(prepReq)
	suite.Require().NoErrorf(err, "Prepare(stmt=%s): %v", stmt, err)

	// Bind the variables and query.
	clong := int64(11)
	for i := 0; i < 5; i++ {
		prepRes.PreparedStatement.SetVariable("$cl", clong)
		queryReq = &nosqldb.QueryRequest{
			PreparedStatement: &prepRes.PreparedStatement,
			Consistency:       types.Eventual,
		}
		queryRes, err = suite.Client.Query(queryReq)
		suite.Require().NoErrorf(err, "PreparedQuery(stmt=%s, clong=%d): %v", stmt, clong, err)

		results, err = queryRes.GetResults()
		suite.Require().NoErrorf(err, "PreparedQuery.GetResults() got error %v", err)

		suite.Require().Equalf(1, len(results), "unexpected number of results")
		retId, _ := results[0].GetInt("id")
		suite.Require().Equalf(int(clong), retId, "unexpected value for \"id\"")
		retSid, _ := results[0].GetInt("sid")
		suite.Require().Equalf(sid, retSid, "unexpected value for \"sid\"")

		clong++
	}

	// MultiDelete
	sid = 102
	key = &types.MapValue{}
	key.Put("sid", sid)
	fr := &types.FieldRange{
		FieldPath:      "id",
		Start:          11,
		End:            16,
		StartInclusive: true,
		EndInclusive:   false,
	}
	mdReq := &nosqldb.MultiDeleteRequest{
		TableName:  tableName,
		Key:        key,
		FieldRange: fr,
	}
	mdRes, err := suite.Client.MultiDelete(mdReq)
	suite.Require().NoErrorf(err, "MultiDelete(id=[11, 16), sid): %v", sid, err)
	suite.Require().Equalf(5, mdRes.NumDeleted, "unexpected number of rows deleted")

	// List indexes.
	getIdxReq := &nosqldb.GetIndexesRequest{
		TableName: tableName,
	}
	getIdxRes, err := suite.Client.GetIndexes(getIdxReq)
	suite.Require().NoErrorf(err, "GetIndexes(table=%s): %v", tableName, err)
	suite.Require().Equalf(2, len(getIdxRes.Indexes), "unexpected number of indexes on table %s", tableName)

	// List tables.
	var listTabRes *nosqldb.ListTablesResult
	listTabReq := &nosqldb.ListTablesRequest{}
	tableList := make([]string, 0, 5)
	for {
		listTabRes, err = suite.Client.ListTables(listTabReq)
		suite.Require().NoErrorf(err, "ListTables(): %v", err)
		if len(listTabRes.Tables) == 0 {
			break
		}

		tableList = append(tableList, listTabRes.Tables...)
		if listTabRes.LastIndexReturned <= 0 {
			break
		}

		listTabReq.StartIndex = listTabRes.LastIndexReturned
	}

	suite.Require().Containsf(tableList, tableName, "ListTables() does not return table %s", tableName)

	if test.IsCloud() {
		// Get table usage.
		tuReq := &nosqldb.TableUsageRequest{
			TableName: tableName,
			StartTime: baseTime,
			EndTime:   time.Now(),
			Limit:     10,
		}
		tuRes, err := suite.Client.GetTableUsage(tuReq)
		suite.Require().NoErrorf(err, "GetTableUsage(): %v", err)

		numUsageRecords := len(tuRes.UsageRecords)
		suite.Require().LessOrEqualf(numUsageRecords, int(tuReq.Limit), "unexpected number of usage records")
		suite.Require().Greaterf(numUsageRecords, 0, "unexpected number of usage records")

		for i, tu := range tuRes.UsageRecords {
			suite.Require().Greaterf(tu.ReadUnits, 0, "unexpected ReadUnits of table usage %d", i+1)
			suite.Require().Greaterf(tu.WriteUnits, 0, "unexpected WriteUnits of table usage %d", i+1)
		}

	} else if test.IsOnPrem() {
		suite.doOnPremTest()
	}
}

// Do tests specific to on-premise.
func (suite SmokeTestSuite) doOnPremTest() {
	var stmt string
	var err error

	ns := "Ns01"
	tableName := suite.GetNsTableName(ns, "SmokeTest")

	// Drop namespace.
	stmt = fmt.Sprintf("DROP NAMESPACE IF EXISTS %s CASCADE", ns)
	sysRes, err := suite.Client.DoSystemRequestAndWait(stmt, 10*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "DoSystemRequestAndWait(stmt=%s): %v", stmt, err)
	suite.Require().Equalf(types.Complete, sysRes.State, "unexpected operation state")

	// Create namespace.
	stmt = "CREATE NAMESPACE " + ns
	sysReq := &nosqldb.SystemRequest{
		Statement: stmt,
	}
	sysRes, err = suite.Client.DoSystemRequest(sysReq)
	suite.Require().NoErrorf(err, "DoSystemRequest(stmt=%s): %v", stmt, err)
	sysRes, err = sysRes.WaitForCompletion(suite.Client, 10*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "WaitForCompletion(stmt=%s): %v", stmt, err)
	suite.Require().Equalf(types.Complete, sysRes.State, "unexpected operation state")

	// List namespaces.
	nsNames, err := suite.Client.ListNamespaces()
	suite.Require().NoErrorf(err, "ListNamespaces(): %v", err)
	suite.Require().GreaterOrEqualf(len(nsNames), 1, "unexpected number of namespaces")
	suite.Require().Containsf(nsNames, ns, "ListNamespaces() does not return namespace %s", ns)

	stmt = fmt.Sprintf("CREATE TABLE %s (id INTEGER, name STRING, PRIMARY KEY(id))", tableName)
	tableReq := &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err := suite.Client.DoTableRequestAndWait(tableReq, 20*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "DoTableRequestAndWait(stmt=%s): %v", stmt, err)
	suite.Require().Equalf(types.Active, tableRes.State, "unexpected table state")

	// Create a child table.
	childTable := tableName + "." + "child"
	stmt = fmt.Sprintf("CREATE TABLE %s (cid INTEGER, cname STRING, PRIMARY KEY(cid))", childTable)
	tableReq = &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err = suite.Client.DoTableRequestAndWait(tableReq, 20*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "DoTableRequestAndWait(stmt=%s): %v", stmt, err)
	suite.Require().Equalf(types.Active, tableRes.State, "unexpected table state")

	// List tables in the specified namespace.
	listTabReq := &nosqldb.ListTablesRequest{
		Namespace: ns,
	}
	listTabRes, err := suite.Client.ListTables(listTabReq)
	suite.Require().NoErrorf(err, "ListTables(namespace=%s): %v", ns, err)
	suite.Require().Equalf(2, len(listTabRes.Tables), "unexpected number of tables in namespace %s", ns)
	suite.Require().Containsf(listTabRes.Tables, tableName, "ListTables() does not return table %s", tableName)
	suite.Require().Containsf(listTabRes.Tables, childTable, "ListTables() does not return table %s", childTable)

	if !test.IsOnPremSecureStore() {
		return
	}

	password := "NoSql00__123456"
	user1 := "user01"
	stmt = fmt.Sprintf("CREATE USER %s IDENTIFIED BY %q", user1, password)
	sysRes, err = suite.Client.DoSystemRequestAndWait(stmt, 10*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "DoSystemRequestAndWait(stmt=%s): %v", stmt, err)
	suite.Require().Equalf(types.Complete, sysRes.State, "\"%s\": unexpected operation state", stmt)

	role1 := "role01"
	stmt = fmt.Sprintf("CREATE ROLE %s", role1)
	sysRes, err = suite.Client.DoSystemRequestAndWait(stmt, 10*time.Second, 2*time.Second)
	suite.Require().NoErrorf(err, "DoSystemRequestAndWait(stmt=%s): %v", stmt, err)
	suite.Require().Equalf(types.Complete, sysRes.State, "\"%s\": unexpected operation state", stmt)

	userInfo, err := suite.Client.ListUsers()
	suite.Require().NoErrorf(err, "ListUsers(): %v", err)
	foundUser := false
	for _, u := range userInfo {
		if u.Name == user1 {
			foundUser = true
			break
		}
	}
	suite.Require().Truef(foundUser, "ListUsers() does not return user %s", user1)

	roles, err := suite.Client.ListRoles()
	suite.Require().NoErrorf(err, "ListRoles(): %v", err)
	suite.Require().Containsf(roles, role1, "ListRoles() does not return role %s", role1)
}

func TestSmoke(t *testing.T) {
	test := &SmokeTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
