//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// +build cloudsim cloud

package nosqldb_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"testing"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

type BadProtocolTestSuite struct {
	*test.NoSQLTestSuite
	bpTestClient *nosqldb.Client
	wr           proto.Writer
	table        string
	index        string
	tableNameLen int
}

const (
	testErrCodeBadProtoMsg     = nosqlerr.BadProtocolMessage
	testErrCodeIllegalArgument = nosqlerr.IllegalArgument
)

func (suite *BadProtocolTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	var err error
	// Create a customized client for bad protocol test.
	suite.bpTestClient, err = nosqldb.NewClient(suite.Client.Config)
	suite.Require().NoErrorf(err, "failed to create a client, got error %v.", err)

	// Disable retry handling.
	suite.bpTestClient.RetryHandler = nil
	suite.bpTestClient.AuthorizationProvider = suite.Client.AuthorizationProvider
	// Set a customized response handler.
	suite.bpTestClient.SetResponseHandler(processTestResponse)

	suite.table = suite.GetTableName("Users")
	suite.index = "idx1"
	suite.createTableAndIndex()

	suite.wr = binary.NewWriter()
	// Calculate the number of bytes that would be written for table name.
	suite.tableNameLen, _ = suite.wr.WriteString(&suite.table)
}

// Create tables and indexes for bad protocol test.
func (suite *BadProtocolTestSuite) createTableAndIndex() {
	var stmt string
	table, index := suite.table, suite.index

	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, "+
		"name STRING, "+
		"count LONG, "+
		"avg DOUBLE, "+
		"sum NUMBER, "+
		"exp BOOLEAN, "+
		"key BINARY, "+
		"map MAP(INTEGER), "+
		"array ARRAY(STRING), "+
		"record RECORD(rid INTEGER, rs STRING), "+
		"PRIMARY KEY(id))", table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  20,
		WriteUnits: 20,
		StorageGB:  2,
	}
	suite.ReCreateTable(table, stmt, limits)

	stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (name)",
		index, table)
	suite.ExecuteTableDDL(stmt)
}

func createTestKey(id int) *types.MapValue {
	var m types.MapValue
	m.Put("id", id)
	return &m
}

func createTestValue() *types.MapValue {
	var m types.MapValue
	m.Put("id", 1).Put("name", "string value").Put("count", math.MaxInt64)
	return &m
}

// A custom postExecute function of the Client. This function checks returned
// error code from the response of request execution. It does not deserialize
// the response content to a result object.
func processTestResponse(httpResp *http.Response, req nosqldb.Request) (nosqldb.Result, error) {
	data, err := ioutil.ReadAll(httpResp.Body)
	httpResp.Body.Close()
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode == http.StatusOK {
		buf := bytes.NewBuffer(data)
		rd := binary.NewReader(buf)
		code, err := rd.ReadByte()
		if err != nil {
			return nil, err
		}

		if code == 0 {
			return nil, nil
		}

		s, _ := rd.ReadString()

		// Tests care about the error code only, the error message is ignored.
		errCode := nosqlerr.ErrorCode(int(code))
		err = nosqlerr.New(errCode, *s)
		return nil, err
	}

	if len(data) > 0 {
		return nil, fmt.Errorf("error response: %s", string(data))
	}
	return nil, fmt.Errorf("error response status code: %d", httpResp.StatusCode)
}

func writeByte(wr proto.Writer, b byte) {
	wr.Write([]byte{b})
}

func seekPos(lengths []int, fieldOff int) (off int) {
	if n := len(lengths); fieldOff >= n {
		panic(fmt.Errorf("the specified field offset %d is greater than the length of slice %d",
			fieldOff, n))
	}
	for i := 0; i < fieldOff; i++ {
		off += lengths[i]
	}
	return off
}

func (suite *BadProtocolTestSuite) doBadProtoTest(req nosqldb.Request, data []byte, desc string, expectErrCode nosqlerr.ErrorCode) {
	_, err := suite.bpTestClient.DoExecute(context.Background(), req, data)
	switch expectErrCode {
	case nosqlerr.NoError:
		suite.NoErrorf(err, "%q should have succeeded, got error %v.", desc, err)
	default:
		suite.Truef(nosqlerr.Is(err, expectErrCode),
			"%q failed, got error %v, want error %s", desc, err, expectErrCode)
	}
}

func (suite *BadProtocolTestSuite) TestBadGetRequest() {
	req := &nosqldb.GetRequest{
		TableName:   suite.table,
		Consistency: types.Absolute,
		Key:         createTestKey(1),
	}

	data, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)

	// Positive test.
	desc := "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	origData := make([]byte, len(data))
	copy(origData, data)

	pos := []int{
		2,                  // SerialVersion: short
		1,                  // OpCode: byte
		3,                  // RequestTimeout: packed int
		suite.tableNameLen, // TableName: string
		1,                  // Consistency: boolean
		14,                 // Key: 1(TYPE_MAP) + 4(length) + 4(size) + 3("id") + 1(TYPE_INT) + 1(1-value)
	}

	testCases := []struct {
		value   types.Consistency
		wantErr nosqlerr.ErrorCode
	}{
		// Valid consistency value.
		{types.Eventual, nosqlerr.ErrorCode(0)},
		// Invalid consistency values.
		{-1, testErrCodeBadProtoMsg},
		{3, testErrCodeBadProtoMsg},
	}

	off := seekPos(pos, 4)
	for i, r := range testCases {
		desc = fmt.Sprintf("invalid consistency value: %d", r.value)
		if i > 0 {
			copy(data, origData)
		}
		suite.wr.Reset()
		suite.wr.WriteConsistency(r.value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, r.wantErr)
	}

	off = seekPos(pos, 5)
	// Invalid primary key type.
	invalidPKTypes := []int{-1, int(types.Array)}
	for _, r := range invalidPKTypes {
		desc = fmt.Sprintf("invalid value type of PrimaryKey: %d", r)
		copy(data, origData)
		suite.wr.Reset()
		writeByte(suite.wr, byte(r))
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
	}
}

func (suite *BadProtocolTestSuite) TestBadGetIndexesRequest() {
	req := &nosqldb.GetIndexesRequest{
		TableName: suite.table,
		IndexName: suite.index,
	}

	data, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)

	origData := make([]byte, len(data))
	copy(origData, data)

	desc := "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	lengths := []int{
		2,                  // SerialVersion: short
		1,                  // OpCode: byte
		3,                  // RequestTimeout: packed int
		suite.tableNameLen, // TableName: string
		1,                  // HasIndex: boolean
		5,                  // IndexName: string
	}

	emptyString := ""
	testCases := []struct {
		value   *string
		wantErr nosqlerr.ErrorCode
	}{
		// index name is nil
		{nil, testErrCodeBadProtoMsg},
		// index name is an empty string, but HasIndex is set to true
		{&emptyString, testErrCodeBadProtoMsg},
	}

	off := seekPos(lengths, 5)
	for i, r := range testCases {
		if r.value == nil {
			desc = fmt.Sprintf("invalid index name: %v", r.value)
		} else {
			desc = fmt.Sprintf("invalid index name: %q", *r.value)
		}
		if i > 0 {
			copy(data, origData)
		}
		suite.wr.Reset()
		suite.wr.WriteString(r.value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, r.wantErr)
	}
}

func (suite *BadProtocolTestSuite) TestBadGetTableRequest() {
	req := &nosqldb.GetTableRequest{
		TableName: suite.table,
	}

	data, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)

	origData := make([]byte, len(data))
	copy(origData, data)

	desc := "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	lengths := []int{
		2,                  // SerialVersion: short
		1,                  // OpCode: byte
		3,                  // RequestTimeout: packed int
		suite.tableNameLen, // TableName: string
		6,                  // OperationId: string
	}

	off := seekPos(lengths, 4)
	opId := 5678
	desc = fmt.Sprintf("invalid operation id: %d", opId)

	suite.wr.Reset()
	// Write operation id as an Integer, rather than a string.
	suite.wr.WriteInt(opId)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
}

func (suite *BadProtocolTestSuite) TestBadListTablesRequest() {
	req := &nosqldb.ListTablesRequest{}
	data, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)

	origData := make([]byte, len(data))
	copy(origData, data)

	desc := "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	lengths := []int{
		2, // SerialVersion: short
		1, // OpCode: byte
		3, // RequestTimeout: packed int
		4, // StartIndex: int
		4, // Limit: int
	}

	// invalid start index
	off := seekPos(lengths, 3)
	value := -1
	desc = fmt.Sprintf("invalid start index : %d", value)
	suite.wr.Reset()
	suite.wr.WriteInt(value)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)

	// invalid limit
	off = seekPos(lengths, 4)
	desc = fmt.Sprintf("invalid limit : %d", value)
	copy(data, origData)
	suite.wr.Reset()
	suite.wr.WriteInt(value)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
}

func (suite *BadProtocolTestSuite) TestBadPrepareRequest() {
	stmt := "select * from " + suite.table
	req := &nosqldb.PrepareRequest{Statement: stmt}
	data, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)

	origData := make([]byte, len(data))
	copy(origData, data)

	desc := "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	stmtLen, _ := suite.wr.WriteString(&stmt)
	lengths := []int{
		2,       // SerialVersion: short
		1,       // OpCode: byte
		3,       // RequestTimeout: packed int
		stmtLen, // Statement: string
	}

	off := seekPos(lengths, 3)

	emptyStr := ""
	testStmts := []*string{
		nil,
		&emptyStr,
	}
	for i, s := range testStmts {
		if s == nil {
			desc = "nil statement"
		} else {
			desc = *s + " statement"
		}

		if i > 0 {
			copy(data, origData)
		}
		suite.wr.Reset()
		suite.wr.WriteString(s)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
	}

	// invalid length of statement
	testStmtLengths := []int{
		len(stmt) + 1,
		-2,
		test.MaxQuerySizeLimit,
	}
	for _, value := range testStmtLengths {
		desc = fmt.Sprintf("invalid statement, its length is %d", value)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WritePackedInt(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
	}
}

func (suite *BadProtocolTestSuite) TestBadQueryRequest() {
	stmt := fmt.Sprintf("declare $id integer; "+
		"select * from %s where id = $id", suite.table)
	prepReq := &nosqldb.PrepareRequest{
		Statement: stmt,
	}
	// Use the default Client to execute the prepare request.
	prepRes, err := suite.Client.Prepare(prepReq)
	suite.Require().NoError(err)

	err = prepRes.PreparedStatement.SetVariable("$id", 1)
	suite.Require().NoError(err)

	req := &nosqldb.QueryRequest{
		PreparedStatement: &prepRes.PreparedStatement,
		MaxReadKB:         1024,
		Limit:             100,
	}

	//TODO:
	prepStmtLen := 100
	// prepStmtLen := 4 /* int, length of PreparedStatement */ +
	// len(req.PreparedStatement.statement)
	lengths := []int{
		2,           // SerialVersion: short
		1,           // OpCode: byte
		3,           // RequestTimeout: packed int
		1,           // Consistency: byte
		1,           // NumberLimit: packed int
		3,           // MaxReadKB: packed int
		1,           // ContinuationKey: byte array
		1,           // IsPreparedStatement: boolean
		2,           // QueryVersion: short
		prepStmtLen, // PreparedStatement: byte array
		1,           // VariablesNumber: packed int
		4,           // VariableName: string
		2,           // VariableValue: INT_TYPE + packed int
	}

	data, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)

	origData := make([]byte, len(data))
	copy(origData, data)

	desc := "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// invalid length of statements.
	off := seekPos(lengths, 9)
	testStmtLengths := []int{
		0,
		-1,
	}
	for _, value := range testStmtLengths {
		desc = fmt.Sprintf("invalid statement, its length is %d", value)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteInt(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
	}

	// invalid number of variables
	off = seekPos(lengths, 10)
	testVarNumLengths := []int{
		-1,
		0,
		2,
	}
	for _, value := range testVarNumLengths {
		desc = fmt.Sprintf("invalid number of variables: %d", value)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteInt(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
	}

	// invalid variable names
	emptyStr := ""
	off = seekPos(lengths, 11)
	testVarNames := []*string{
		nil,
		&emptyStr,
	}
	for _, value := range testVarNames {
		if value == nil {
			desc = "nil variable name"
		} else {
			desc = fmt.Sprintf("invalid variable name: %q", *value)
		}
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteString(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, testErrCodeBadProtoMsg)
	}

	// invalid variable values
	off = seekPos(lengths, 12)
	testVarTypes := []struct {
		valueType types.DbType
		wantErr   nosqlerr.ErrorCode
	}{
		{-1, testErrCodeBadProtoMsg},
		{types.Array, testErrCodeIllegalArgument},
	}
	for _, r := range testVarTypes {
		desc = fmt.Sprintf("invalid variable value type: %s", r.valueType)
		copy(data, origData)
		suite.wr.Reset()
		writeByte(suite.wr, byte(r.valueType))
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, r.wantErr)
	}
}

func TestBadProtocol(t *testing.T) {
	test := &BadProtocolTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
