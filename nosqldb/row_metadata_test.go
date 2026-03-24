//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

//go:build cloud || onprem
// +build cloud onprem

package nosqldb_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// RowMetadataTestSuite contains tests for Row Metadata in data operation APIs
// including put, delete, multiDelete, writeMultiple and query.

type RowMetadataTestSuite struct {
	*test.NoSQLTestSuite
	table string
}

var (
	createdBy  = "{\"user\":\"create\", \"time\":\"2026-02-10\"}"
	updatedBy1 = "{\"user\":\"update1\", \"time\":\"2026-03-10\"}"
	updatedBy2 = "{\"user\":\"update2\", \"time\":\"2026-03-20\"}"
	deletedBy  = "{\"user\":\"delete\", \"time\":\"2026-04-10\"}"
	sid        = 1
	verbose    = false
)

func (suite *RowMetadataTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	suite.table = suite.GetTableName("RowMetadataTest")

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sid INTEGER, id INTEGER, name STRING, "+
		"PRIMARY KEY(shard(sid), id))", suite.table)

	limits := &nosqldb.TableLimits{
		ReadUnits:  500,
		WriteUnits: 500,
		StorageGB:  1,
	}

	suite.ReCreateTable(suite.table, stmt, limits)
}

func (suite *RowMetadataTestSuite) SetupTest() {
	suite.deleteAll()
}

// Test Put with row metadata.
func (suite *RowMetadataTestSuite) TestPut() {
	var ret *nosqldb.PutResult

	// Put a row with metadata - createdBy
	ret = suite.put(0, createdBy)
	suite.NotNil(ret.Version, "Put failed")
	suite.assertRowMetadataByGet(0, createdBy)

	// PutIfPresent the row with metadata - updatedBy1
	ret = suite.putIfPresent(0, updatedBy1)
	suite.NotNil(ret.Version, "PutIfPresent failed")
	suite.assertRowMetadata(createdBy, ret.ExistingLastWriteMetadata)
	suite.assertRowMetadataByGet(0, updatedBy1)

	// PutIfAbsent the row with metadata, no row updated as row exists
	ret = suite.putIfAbsent(0, updatedBy2)
	suite.Nil(ret.Version, "PutIfAbsent should not actual update the row")
	suite.assertRowMetadata(updatedBy1, ret.ExistingLastWriteMetadata)
	suite.assertRowMetadataByGet(0, updatedBy1)

	// PutIfVersion the row with metadata - updatedBy2
	ifVersion := ret.ExistingVersion
	ret = suite.putIfVersion(0, updatedBy2, ifVersion)
	suite.NotNil(ret.Version, "PutIfVersion should suceed")
	suite.assertRowMetadataByGet(0, updatedBy2)
}

// Test Delete with row metadata.
func (suite *RowMetadataTestSuite) TestDelete() {
	var ret *nosqldb.DeleteResult
	var getRet *nosqldb.GetResult
	var putRet *nosqldb.PutResult

	// Put a row with metadata - createdBy
	suite.put(0, createdBy)
	suite.assertRowMetadataByGet(0, createdBy)

	// Delete a row with metadata - deletedBy
	ret = suite.delete(0, deletedBy)
	suite.True(ret.Success, "Delete should succeed")
	suite.assertRowMetadata(createdBy, ret.ExistingLastWriteMetadata)
	oldVersion := ret.ExistingVersion
	getRet = suite.getRow(0)
	suite.Nil(getRet.Value, "The row should have been deleted")

	// Put row with metadata - updateBy1
	putRet = suite.put(0, updatedBy1)
	suite.assertRowMetadataByGet(0, updatedBy1)
	version := putRet.Version

	// Delete row with unmatched version, row won't be deleted
	ret = suite.deleteIfVersion(0, deletedBy, oldVersion)
	suite.False(ret.Success, "Delete should not succeed")
	suite.assertRowMetadata(updatedBy1, ret.ExistingLastWriteMetadata)

	// Delete row with matched version, row deleted
	ret = suite.deleteIfVersion(0, deletedBy, version)
	suite.True(ret.Success, "Delete should succeed")
	getRet = suite.getRow(0)
	suite.Nil(getRet.Value, "The row should have been deleted")
}

// Test MultiDelete with row metadata.
func (suite *RowMetadataTestSuite) TestMultiDelete() {
	numRows := 3
	for i := 0; i < numRows; i++ {
		suite.put(i, createdBy)
	}

	skey := &types.MapValue{}
	skey.Put("sid", sid)
	req := &nosqldb.MultiDeleteRequest{
		TableName:         suite.table,
		Key:               skey,
		LastWriteMetadata: deletedBy,
	}
	ret, err := suite.Client.MultiDelete(req)
	suite.Nilf(err, "MultiDelete failed: %v", err)
	suite.Equalf(numRows, ret.NumDeleted, "Wrong number of rows delete: exp-%d, act-", numRows, ret.NumDeleted)

	numRows = suite.queryRowCount()
	suite.Equalf(0, numRows, "All rows should be deleted but get %d", numRows)
}

// Test INSERT, UPDATE, DELETE query with row metadata.
func (suite *RowMetadataTestSuite) TestQuery() {
	id := 0

	suite.put(id, createdBy)
	suite.assertRowMetadataByQuery(id, createdBy)

	insertStmt := fmt.Sprintf("upsert into %s values(%d, %d, %q)", suite.table, sid, id, "Name"+strconv.Itoa(id))
	suite.execQuery(insertStmt, updatedBy1)
	suite.assertRowMetadataByQuery(id, updatedBy1)

	updateStmt := fmt.Sprintf("update %s set name = upper(name) where sid = %d and id = %d", suite.table, sid, id)
	suite.execQuery(updateStmt, updatedBy2)
	suite.assertRowMetadataByQuery(id, updatedBy2)

	deleteStmt := fmt.Sprintf("delete from %s", suite.table)
	suite.execQuery(deleteStmt, deletedBy)

	count := suite.queryRowCount()
	suite.Equalf(0, count, "Wrong row count: exp-0, act-%d", count)
}

// Test writeMultiple with row metadata
func (suite *RowMetadataTestSuite) TestWriteMultiple() {
	var req *nosqldb.WriteMultipleRequest
	var res *nosqldb.WriteMultipleResult
	var put *nosqldb.PutRequest
	var delete *nosqldb.DeleteRequest
	var opRet nosqldb.OperationResult
	var numOps int
	var metadata string
	var metadataArr []string
	var err error

	numOps = 3
	metadataArr = make([]string, numOps)

	// Put operations
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("put", "id"+strconv.Itoa(i))
		put = &nosqldb.PutRequest{
			TableName:         suite.table,
			Value:             suite.createRow(i),
			LastWriteMetadata: metadata,
		}
		req.AddPutRequest(put, false)

		metadataArr[i] = metadata
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")
	for i := 0; i < numOps; i++ {
		suite.assertRowMetadataByGet(i, metadataArr[i])
	}

	// Backup the current Row matadata
	prevMetadtaArr := make([]string, len(metadataArr))
	copy(prevMetadtaArr, metadataArr)

	prevVersions := make([]types.Version, numOps)
	versions := make([]types.Version, numOps)

	// PutIfPresent ops
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("PutIfPresent", "id"+strconv.Itoa(i))
		put = &nosqldb.PutRequest{
			TableName:         suite.table,
			Value:             suite.createRow(i),
			PutOption:         types.PutIfPresent,
			ReturnRow:         true,
			LastWriteMetadata: metadata,
		}
		req.AddPutRequest(put, false)

		metadataArr[i] = metadata
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")

	suite.Equalf(numOps, len(res.ResultSet), "Unexpected number of operation results: exp-%d, act-%d", numOps, len(res.ResultSet))
	for i := 0; i < numOps; i++ {
		opRet = res.ResultSet[i]
		suite.assertRowMetadata(prevMetadtaArr[i], opRet.ExistingLastWriteMetadata)
		suite.assertRowMetadataByGet(i, metadataArr[i])
		versions[i] = opRet.Version
		prevVersions[i] = opRet.ExistingVersion
	}

	// PutIfAbsent ops
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("PutIfAbsent", "id"+strconv.Itoa(i))
		put = &nosqldb.PutRequest{
			TableName:         suite.table,
			Value:             suite.createRow(i),
			PutOption:         types.PutIfAbsent,
			ReturnRow:         true,
			LastWriteMetadata: metadata,
		}
		req.AddPutRequest(put, false)
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")

	suite.Equalf(numOps, len(res.ResultSet), "Unexpected number of operation results: exp-%d, act-%d", numOps, len(res.ResultSet))
	for i := 0; i < numOps; i++ {
		opRet = res.ResultSet[i]
		suite.Falsef(opRet.Success, "Operation should have failed")
		suite.assertRowMetadata(metadataArr[i], opRet.ExistingLastWriteMetadata)
	}

	// PutIfVersion ops with unmatched version
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("PutIfVersion", "id"+strconv.Itoa(i))
		put = &nosqldb.PutRequest{
			TableName:         suite.table,
			Value:             suite.createRow(i),
			PutOption:         types.PutIfVersion,
			MatchVersion:      prevVersions[i],
			ReturnRow:         true,
			LastWriteMetadata: metadata,
		}
		req.AddPutRequest(put, false)
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")

	suite.Equalf(numOps, len(res.ResultSet), "Unexpected number of operation results: exp-%d, act-%d", numOps, len(res.ResultSet))
	for i := 0; i < numOps; i++ {
		opRet = res.ResultSet[i]
		suite.Falsef(opRet.Success, "Operation should have failed")
		suite.assertRowMetadata(metadataArr[i], opRet.ExistingLastWriteMetadata)
	}

	// PutIfVersion ops with matched version
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("PutIfVersion1", "id"+strconv.Itoa(i))
		put = &nosqldb.PutRequest{
			TableName:         suite.table,
			Value:             suite.createRow(i),
			PutOption:         types.PutIfVersion,
			MatchVersion:      versions[i],
			ReturnRow:         true,
			LastWriteMetadata: metadata,
		}
		req.AddPutRequest(put, false)
		metadataArr[i] = metadata
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")

	copy(prevVersions, versions)

	suite.Equalf(numOps, len(res.ResultSet), "Unexpected number of operation results: exp-%d, act-%d", numOps, len(res.ResultSet))
	for i := 0; i < numOps; i++ {
		opRet = res.ResultSet[i]
		suite.Truef(opRet.Success, "Operation should have failed")
		suite.assertRowMetadataByGet(i, metadataArr[i])
		versions[i] = opRet.Version
	}

	// DeleteIfVersion operatoins with unmatched version
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("DeleteIfVersion", "id"+strconv.Itoa(i))
		delete = &nosqldb.DeleteRequest{
			TableName:         suite.table,
			Key:               suite.createKey(i),
			MatchVersion:      prevVersions[i],
			ReturnRow:         true,
			LastWriteMetadata: metadata,
		}
		req.AddDeleteRequest(delete, false)
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")
	suite.Equalf(numOps, len(res.ResultSet), "Unexpected number of operation results: exp-%d, act-%d", numOps, len(res.ResultSet))
	for i := 0; i < numOps; i++ {
		opRet = res.ResultSet[i]
		suite.Falsef(opRet.Success, "Operation should have failed")
		suite.assertRowMetadata(metadataArr[i], opRet.ExistingLastWriteMetadata)
	}

	copy(prevMetadtaArr, metadataArr)

	// Delete operatoins
	req = &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	for i := 0; i < numOps; i++ {
		metadata = suite.genMetadata("Delete", "id"+strconv.Itoa(i))
		delete = &nosqldb.DeleteRequest{
			TableName:         suite.table,
			Key:               suite.createKey(i),
			ReturnRow:         true,
			LastWriteMetadata: metadata,
		}
		req.AddDeleteRequest(delete, false)
		metadataArr[i] = metadata
	}
	res, err = suite.Client.WriteMultiple(req)
	suite.NoErrorf(err, "WriteMultiple failed, got error: %v", err)
	suite.True(res.IsSuccess(), "WriteMultiple failed")
	suite.Equalf(numOps, len(res.ResultSet), "Unexpected number of operation results: exp-%d, act-%d", numOps, len(res.ResultSet))
	for i := 0; i < numOps; i++ {
		opRet = res.ResultSet[i]
		suite.Truef(opRet.Success, "Operation should have failed")
		suite.assertRowMetadata(prevMetadtaArr[i], opRet.ExistingLastWriteMetadata)
	}
}

func (suite *RowMetadataTestSuite) TestInvalidRowMetadata() {

	invalidMetadata := "{\"a\":1}{\"b\":2}"
	var err error

	// Put
	putReq := &nosqldb.PutRequest{
		TableName:         suite.table,
		Value:             suite.createRow(0),
		LastWriteMetadata: invalidMetadata,
	}
	_, err = suite.Client.Put(putReq)
	suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "Invalid row metadata, expect get IllegalArgument but get %v", err)

	// Delete
	deleteReq := &nosqldb.DeleteRequest{
		TableName:         suite.table,
		Key:               suite.createKey(1),
		LastWriteMetadata: invalidMetadata,
	}
	_, err = suite.Client.Delete(deleteReq)
	suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "Invalid row metadata, expect get IllegalArgument but get %v", err)

	// MultiDelete
	skey := &types.MapValue{}
	skey.Put("sid", sid)
	multiDelReq := &nosqldb.MultiDeleteRequest{
		TableName:         suite.table,
		Key:               skey,
		LastWriteMetadata: invalidMetadata,
	}
	_, err = suite.Client.MultiDelete(multiDelReq)
	suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "Invalid row metadata, expect get IllegalArgument but get %v", err)

	// WriteMultiple
	wmReq := &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	err = wmReq.AddPutRequest(putReq, false)
	suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "Invalid row metadata, expect get IllegalArgument but get %v", err)

	err = wmReq.AddDeleteRequest(deleteReq, false)
	suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "Invalid row metadata, expect get IllegalArgument but get %v", err)

	// Query
	stmt := "insert into " + suite.table + " values(1, 1, 's')"
	queryReq := &nosqldb.QueryRequest{
		Statement:         stmt,
		LastWriteMetadata: invalidMetadata,
	}
	_, err = suite.Client.Query(queryReq)
	suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "Invalid row metadata, expect get IllegalArgument but get %v", err)
}

func (suite *RowMetadataTestSuite) TestValidateRowMetadata() {
	invalidMetadata := []string{
		"custom metadata",
		"",
		" ",
		"\n",
		"\t",
		"'abc'",
		"{{}}",
		"{}{}",
		"{}\n{}",
		"{}   {}",
		"{},{}",
		"{\"a\":'c'}", // single quoted string
		"{'a':1}",     // single quoted string
		"NULL",
		"Null",
		"True",
		"FALSE",
		"{\"a\":\"Invalid string \u0000\"",
		"\"abc\"\"def\"",
		"1true2null",
		"1,2,3",
		"[][]",
		"0123",
		"INF", // ??? since -INF is allowed
		"Inf",
		"-Inf",
		"NAN",
		"Not-A-Number",
		// Non-numerical numbers are not allowed
		"NaN",
		"{\"a\":NaN}",
		"-INF",
		"Infinity",
		"-Infinity",
	}

	validMetadata := []string{
		"{}",
		"{\"a\":1}",
		"   { \"a\"  :  1  }    ",
		"{\"a\":2, \"b\":\"a\"}",
		"{\"a\":[]}",
		"{\"a\":[1, 2, 3]}",
		" { } ",
		"\n{\n}\n",
		" \t\n{ \n \t } ",
		"{\"a\": { \"b\":\"a\"}}",
		"{\"a\":{ \"b\":{}}}",
		"{\"a\":1}",
		"{\"a\":true}",
		"{\"a\":[null,1,\"c\", true, [[], {}, null]]}",
		"\"abc\"",
		"\"\"",
		" 123  ",
		" \n\t 123.456",
		"null",
		"true",
		"false",
		"[]",
		"[1, \"s\", true]",
	}

	var err error
	for _, md := range invalidMetadata {
		err = nosqldb.ValidateRowMetadataForTest(md)
		suite.Truef(nosqlerr.Is(err, nosqlerr.IllegalArgument), "%s: expect get IllegalArgument but get %v", md, err)
	}
	for _, md := range validMetadata {
		err = nosqldb.ValidateRowMetadataForTest(md)
		suite.NoErrorf(err, "%s: expect no error but get %v", md, err)
	}
}

func (suite *RowMetadataTestSuite) put(id int, metadata string) *nosqldb.PutResult {
	return suite.putRow(id, metadata, 0, nil)
}

func (suite *RowMetadataTestSuite) putIfAbsent(id int, metadata string) *nosqldb.PutResult {
	return suite.putRow(id, metadata, types.PutIfAbsent, nil)
}

func (suite *RowMetadataTestSuite) putIfPresent(id int, metadata string) *nosqldb.PutResult {
	return suite.putRow(id, metadata, types.PutIfPresent, nil)
}

func (suite *RowMetadataTestSuite) putIfVersion(id int, metadata string, ifVersion types.Version) *nosqldb.PutResult {
	return suite.putRow(id, metadata, types.PutIfVersion, ifVersion)
}

func (suite *RowMetadataTestSuite) putRow(id int, metadata string, option types.PutOption, ifVersion types.Version) *nosqldb.PutResult {
	req := &nosqldb.PutRequest{
		TableName:         suite.table,
		Value:             suite.createRow(id),
		LastWriteMetadata: metadata,
		PutOption:         option,
		ReturnRow:         true,
		MatchVersion:      ifVersion,
	}

	ret, err := suite.Client.Put(req)
	suite.NoErrorf(err, "Put fail: %v", err)
	return ret
}

func (suite *RowMetadataTestSuite) getRow(id int) *nosqldb.GetResult {
	req := &nosqldb.GetRequest{
		TableName: suite.table,
		Key:       suite.createKey(id),
	}
	ret, err := suite.Client.Get(req)
	suite.NoErrorf(err, "Get fail: %v", err)
	return ret
}

func (suite *RowMetadataTestSuite) delete(id int, metadata string) *nosqldb.DeleteResult {
	return suite.deleteRow(id, metadata, nil)
}

func (suite *RowMetadataTestSuite) deleteIfVersion(id int, metadata string, ifVersion types.Version) *nosqldb.DeleteResult {
	return suite.deleteRow(id, metadata, ifVersion)
}

func (suite *RowMetadataTestSuite) deleteRow(id int, metadata string, ifVersion types.Version) *nosqldb.DeleteResult {
	req := &nosqldb.DeleteRequest{
		TableName:         suite.table,
		Key:               suite.createKey(id),
		LastWriteMetadata: metadata,
		MatchVersion:      ifVersion,
		ReturnRow:         true,
	}
	ret, err := suite.Client.Delete(req)
	if verbose {
		fmt.Printf("\ndeleteRow: %v", ret)
	}
	suite.NoErrorf(err, "Delete fail: %v", err)
	return ret
}

func (suite *RowMetadataTestSuite) deleteAll() {
	query := "delete from " + suite.table
	suite.query(query)
}

func (suite *RowMetadataTestSuite) queryRowCount() int {
	stmt := "select count(*) as cnt from " + suite.table
	results := suite.query(stmt)

	suite.Lenf(results, 1, "queryRowCount: query %q should return exactly one row", stmt)
	row := results[0]

	value, ok := row.Get("cnt")
	suite.Truef(ok, "queryRowCount: \"cnt\" column not found in query result %v", row)

	num, ok := value.(int64)
	suite.True(ok, "queryRowCount: unexpected type for count(*): %T", value)
	return int(num)
}

func (suite *RowMetadataTestSuite) queryRowMetadata(id int) *types.MapValue {
	stmt := "select last_write_metadata($t) as md from " + suite.table +
		"$t where sid = " + strconv.Itoa(sid) + " and id = " + strconv.Itoa(id)
	results := suite.query(stmt)

	suite.Lenf(results, 1, "queryRowMetadata: query %q should return exactly one row", stmt)
	row := results[0]

	value, ok := row.Get("md")
	suite.Truef(ok, "queryRowMetadata: column 'md' not found in query result %v", row)

	mv, ok := value.(*types.MapValue)
	suite.True(ok, "queryRowMetadata: unexpected metadata type")
	return mv
}

func (suite *RowMetadataTestSuite) query(stmt string) []*types.MapValue {
	return suite.execQuery(stmt, "")
}

func (suite *RowMetadataTestSuite) execQuery(stmt string, metadata string) []*types.MapValue {
	req := &nosqldb.QueryRequest{
		Statement:         stmt,
		LastWriteMetadata: metadata,
	}

	var results []*types.MapValue
	for {
		ret, err := suite.Client.Query(req)
		if !suite.NoErrorf(err, "Failed to execute query %q: %v", stmt, err) {
			break
		}

		batch, err := ret.GetResults()
		results = append(results, batch...)

		if req.IsDone() {
			break
		}
	}

	if verbose {
		fmt.Printf("%s: %d row returned\n", stmt, len(results))
		for _, r := range results {
			jbytes, _ := r.MarshalJSON()
			fmt.Printf("%s\n", string(jbytes))
		}
	}

	return results
}

func (suite *RowMetadataTestSuite) genMetadata(op string, tag string) string {
	md := &types.MapValue{}
	md.Put("operation", op)
	md.Put("tag", tag)

	jbytes, _ := md.MarshalJSON()
	return string(jbytes)
}

func (suite *RowMetadataTestSuite) createKey(id int) *types.MapValue {
	key := &types.MapValue{}
	key.Put("sid", sid)
	key.Put("id", id)
	return key
}

func (suite *RowMetadataTestSuite) createRow(id int) *types.MapValue {
	row := suite.createKey(id)
	row.Put("s", "s="+strconv.Itoa(id))
	return row
}

func (suite *RowMetadataTestSuite) assertRowMetadataByQuery(id int, expMetadata string) {
	retMD := suite.queryRowMetadata(id)
	suite.NotNil(retMD)

	expMD, err := types.NewMapValueFromJSON(expMetadata)
	suite.NoErrorf(err, "failed to parse expected metadata JSON %q", expMetadata)
	suite.Equalf(expMD.Map(), retMD.Map(), "Unexpected row metadata: exp-%v, act-%v", expMD.Map(), retMD.Map())
}

func (suite *RowMetadataTestSuite) assertRowMetadataByGet(id int, expMetadata string) {
	ret := suite.getRow(id)
	suite.NotNilf(ret.Value, "assertRowMetadataByGet: Key not found, key=%v", suite.createKey(id))
	suite.assertRowMetadata(expMetadata, ret.LastWriteMetadata)
}

func (suite *RowMetadataTestSuite) assertRowMetadata(expected string, actual string) {
	suite.Equalf(expected, actual, "Unexpected row metadata: exp-%s, act-%s", expected, actual)
}

func TestRowMetadata(t *testing.T) {
	test := &RowMetadataTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	// Row Metadata is supported since 26.1
	if test.Config.Version < "26.1" {
		fmt.Printf("TestRowMetadata suite: skipped. Row Metadata is supported since 26.1, Config.Version=%s\n", test.Config.Version)
		return
	}
	suite.Run(t, test)
}
