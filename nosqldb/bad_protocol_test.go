//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

//go:build cloud || onprem
// +build cloud onprem

package nosqldb_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// BadProtocolTestSuite builds and sends invalid request content to the server,
// checks whether the server could handle the bad request.
type BadProtocolTestSuite struct {
	*test.NoSQLTestSuite

	// A client designated for the bad protocol test.
	bpTestClient *nosqldb.Client
	wr           proto.Writer

	// Sample table name, index name, key, value.
	table, index string
	key, value   *types.MapValue

	// The number of bytes required for the encoding of table/index name, key and value.
	tableLen, indexLen, keyLen, valueLen int
}

func (suite *BadProtocolTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	var err error
	// Create a specific client for bad protocol test.
	suite.bpTestClient, err = nosqldb.NewClient(suite.Client.Config)
	suite.Require().NoErrorf(err, "failed to create a client, got error %v", err)

	// Currently this test requires V3 or lower
	suite.bpTestClient.SetSerialVersion(3)

	// this will set the serial protocol version. Ignore errors from it.
	suite.bpTestClient.VerifyConnection()

	// Disable retry handling.
	suite.bpTestClient.RetryHandler = nil
	suite.bpTestClient.AuthorizationProvider = suite.Client.AuthorizationProvider
	// Specify a custom response handler.
	suite.bpTestClient.SetResponseHandler(processTestResponse)

	suite.table = suite.GetTableName("Users")
	suite.index = "idx1"
	suite.createTableAndIndex()

	suite.wr = binary.NewWriter()
	// Calculate the number of bytes that would be written for table/index name.
	suite.tableLen, _ = suite.wr.WriteString(&suite.table)
	suite.indexLen, _ = suite.wr.WriteString(&suite.index)

	suite.key = &types.MapValue{}
	suite.key.Put("id", 1)
	suite.keyLen, _ = suite.wr.WriteFieldValue(suite.key)

	suite.value = &types.MapValue{}
	suite.value.Put("id", 1).Put("name", "string value").Put("count", math.MaxInt64)
	suite.valueLen, _ = suite.wr.WriteFieldValue(suite.value)
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

// processTestResponse is a custom handleResponse function for the Client.
// It checks error code from the response, does not parse the response content.
func processTestResponse(httpResp *http.Response, req nosqldb.Request, serialVerUsed int16, queryVerUsed int16) (nosqldb.Result, error) {
	data, err := io.ReadAll(httpResp.Body)
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

		var errMsg string
		s, _ := rd.ReadString()
		if s != nil {
			errMsg = *s
		}
		errCode := nosqlerr.ErrorCode(int(code))
		err = nosqlerr.New(errCode, errMsg)
		return nil, err
	}

	if len(data) > 0 {
		return nil, fmt.Errorf("error response: %s", string(data))
	}
	return nil, fmt.Errorf("error response status code: %d", httpResp.StatusCode)
}

func stringPtr(s string) *string {
	return &s
}

func seekPos(lengths []int, fieldOff int) (off int) {
	if n := len(lengths); fieldOff >= n {
		panic(fmt.Errorf("invalid field offset: index %d out of bounds for length %d",
			fieldOff, n))
	}
	for i := 0; i < fieldOff; i++ {
		off += lengths[i]
	}
	return off
}

func (suite *BadProtocolTestSuite) doBadProtoTest(req nosqldb.Request, data []byte, desc string, expectErrCode nosqlerr.ErrorCode) {
	serialVerUsed := suite.bpTestClient.GetSerialVersion()
	queryVerUsed := suite.bpTestClient.GetQueryVersion()
	_, err := suite.bpTestClient.DoExecute(context.Background(), req, data, serialVerUsed, queryVerUsed)
	switch expectErrCode {
	case nosqlerr.NoError:
		suite.NoErrorf(err, "%q should have succeeded, got error %v", desc, err)
	default:
		suite.Truef(nosqlerr.Is(err, expectErrCode),
			"%q failed, got error %v, want error %s", desc, err, expectErrCode)
	}
}

func (suite *BadProtocolTestSuite) doBadProtoTest2(req nosqldb.Request, data []byte, desc string, expectErrCode1 nosqlerr.ErrorCode, expectErrCode2 nosqlerr.ErrorCode) {
	serialVerUsed := suite.bpTestClient.GetSerialVersion()
	queryVerUsed := suite.bpTestClient.GetQueryVersion()
	_, err := suite.bpTestClient.DoExecute(context.Background(), req, data, serialVerUsed, queryVerUsed)
	suite.Truef((nosqlerr.Is(err, expectErrCode1) || nosqlerr.Is(err, expectErrCode2)),
		"%q failed, got error %v, want error %s or %s", desc, err, expectErrCode1, expectErrCode2)
}

func (suite *BadProtocolTestSuite) TestBadGetRequest() {
	req := &nosqldb.GetRequest{
		TableName:   suite.table,
		Consistency: types.Absolute,
		Key:         suite.key,
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: string
		1,              // Consistency: boolean
		suite.keyLen,   // Key: map
	}

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, nosqlerr.NoError)

	// Invalid serial version.
	desc = "Invalid serial version"
	off = seekPos(lengths, 0)
	suite.wr.Reset()
	suite.wr.WriteSerialVersion(0)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)

	// Invalid table name.
	off = seekPos(lengths, 3)
	tests := map[string]*string{
		"nil table name":   nil,
		"empty table name": stringPtr(""),
	}
	for k, v := range tests {
		copy(data, origData)
		desc = k
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// Invalid consistency value.
	copy(data, origData)
	off = seekPos(lengths, 4)
	invalidConsistencies := []types.Consistency{-1, 3}
	for _, r := range invalidConsistencies {
		desc = fmt.Sprintf("invalid consistency value: %d", r)
		suite.wr.Reset()
		suite.wr.WriteConsistency(r)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// Invalid primary key type.
	copy(data, origData)
	off = seekPos(lengths, 5)
	invalidPKTypes := []int{-1, int(types.Array)}
	for _, r := range invalidPKTypes {
		desc = fmt.Sprintf("invalid type of PrimaryKey: %d", r)
		suite.wr.Reset()
		suite.wr.WriteByte(byte(r))
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}
}

func (suite *BadProtocolTestSuite) TestBadGetIndexesRequest() {
	req := &nosqldb.GetIndexesRequest{
		TableName: suite.table,
		IndexName: suite.index,
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: string
		1,              // HasIndex: boolean
		suite.indexLen, // IndexName: string
	}

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, nosqlerr.NoError)

	// HasIndex = true but IndexName is nil or empty
	off = seekPos(lengths, 5)
	tests := map[string]*string{
		"nil index name":   nil,
		"empty index name": stringPtr(""),
	}
	for k, v := range tests {
		copy(data, origData)
		desc = k
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}
}

func (suite *BadProtocolTestSuite) TestBadGetTableRequest() {
	req := &nosqldb.GetTableRequest{
		TableName: suite.table,
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: string
		6,              // OperationID: string
	}

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, nosqlerr.NoError)

	// Invalid type of operation id
	off = seekPos(lengths, 4)
	var opID int = 5678
	desc = fmt.Sprintf("invalid type of operation id: %T, want string", opID)
	suite.wr.Reset()
	// Write operation id as an Integer, rather than a string.
	suite.wr.WriteInt(opID)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
}

func (suite *BadProtocolTestSuite) TestBadListTablesRequest() {
	ns := "Namespace001"
	req := &nosqldb.ListTablesRequest{
		Namespace: ns,
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	nsLen, _ := suite.wr.WriteString(&ns)
	var desc string
	var off int
	lengths := []int{
		2,     // SerialVersion: short
		1,     // OpCode: byte
		3,     // RequestTimeout: packed int
		4,     // StartIndex: int
		4,     // Limit: int
		nsLen, // Namespace: string
	}

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// invalid start index: -1
	off = seekPos(lengths, 3)
	desc = "invalid start index : -1"
	suite.wr.Reset()
	suite.wr.WriteInt(-1)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)

	// invalid limit: -1
	off = seekPos(lengths, 4)
	desc = "invalid limit : -1"
	copy(data, origData)
	suite.wr.Reset()
	suite.wr.WriteInt(-1)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)

	// Specify an invalid length for the namespace string.
	off = seekPos(lengths, 5)
	desc = "invalid namespace string length"
	copy(data, origData)
	suite.wr.Reset()
	suite.wr.WritePackedInt(len(ns) + 1)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
}

func (suite *BadProtocolTestSuite) TestBadPrepareRequest() {
	stmt := "select * from " + suite.table
	req := &nosqldb.PrepareRequest{Statement: stmt}
	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	stmtLen, _ := suite.wr.WriteString(&stmt)
	var desc string
	var off int
	lengths := []int{
		2,       // SerialVersion: short
		1,       // OpCode: byte
		3,       // RequestTimeout: packed int
		stmtLen, // Statement: string
		2,       // QueryVersion: short
		1,       // GetQueryPlan: boolean
	}

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// Invalid query statements.
	off = seekPos(lengths, 3)
	tests := map[string]*string{
		"nil query statement":   nil,
		"empty query statement": stringPtr(""),
	}
	for k, v := range tests {
		copy(data, origData)
		desc = k
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// Specify an invalid length for the query statement.
	testStmtLengths := []int{
		len(stmt) + 1,
		-2,
		test.MaxQuerySizeLimit,
		test.MaxQuerySizeLimit + 1,
	}
	for _, value := range testStmtLengths {
		desc = fmt.Sprintf("invalid statement, its length is %d", value)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WritePackedInt(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// TODO(zehliu): validate query version in the database proxy.
	// Invalid query version.
	//
	// off = seekPos(lengths, 4)
	// copy(data, origData)
	// desc = "invalid query version"
	// suite.wr.Reset()
	// suite.wr.WriteInt16(proto.QueryVersion + 1)
	// copy(data[off:], suite.wr.Bytes())
	// suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
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

	prepStmtLen := 4 + len(prepRes.PreparedStatement.GetStatement())
	var desc string
	var off int
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
		1,           // traceLevel: packed int
		1,           // MaxWriteKB: packed int
		1,           // MathContext: byte
		1,           // ToplogySeqNum: packed int
		1,           // ShardID: packed int
		1,           // isSimpleQuery: boolean
		prepStmtLen, // PreparedStatement: byte array
		1,           // VariablesNumber: packed int
		4,           // VariableName: string
		2,           // VariableValue: INT_TYPE + packed int
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// invalid length of prepared statement.
	off = seekPos(lengths, 15)
	testStmtLengths := []int{0, -1}
	for _, value := range testStmtLengths {
		desc = fmt.Sprintf("invalid statement, its length is %d", value)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteInt(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// invalid number of variables
	off = seekPos(lengths, 16)
	testVarNumLengths := []int{-1, 0, 2}
	for _, value := range testVarNumLengths {
		desc = fmt.Sprintf("invalid number of variables: %d", value)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteInt(value)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// invalid variable names
	off = seekPos(lengths, 17)
	testVarNames := map[string]*string{
		"nil variable name":   nil,
		"empty variable name": stringPtr(""),
	}
	for k, v := range testVarNames {
		copy(data, origData)
		desc = k
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// invalid variable values
	off = seekPos(lengths, 18)
	testVarTypes := []struct {
		valueType types.DbType
		wantErr   nosqlerr.ErrorCode
	}{
		{-1, nosqlerr.BadProtocolMessage},
		{types.Array, nosqlerr.IllegalArgument},
	}
	for _, r := range testVarTypes {
		desc = fmt.Sprintf("invalid variable value type: %s", r.valueType)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteByte(byte(r.valueType))
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, r.wantErr)
	}
}

func (suite *BadProtocolTestSuite) TestBadPutRequest() {
	ttlValue := &types.TimeToLive{
		Value: 1,
		Unit:  types.Days,
	}
	ttlLen, _ := suite.wr.WriteTTL(ttlValue)

	req := &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     suite.value,
		TTL:       ttlValue,
	}

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: String
		1,              // ReturnRow: boolean
		1,              // Durability: 1 byte (serialVersion > 2)
		1,              // ExactMatch: boolean
		1,              // IdentityCacheSize: packed int
		suite.valueLen, // Record: MapValue
		1,              // UseTableTTL: boolean
		ttlLen,         // TTL: value(packed long) + unit(byte)
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// Invalid opcode.
	off = seekPos(lengths, 1)
	desc = "invalid put option"
	suite.wr.Reset()
	suite.wr.WriteOpCode(proto.OpCode(-1))
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)

	// Invalid request timeout.
	off = seekPos(lengths, 2)
	desc = "invalid request timeout"
	copy(data, origData)
	suite.wr.Reset()
	suite.wr.WriteTimeout(time.Duration(-1))
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)

	// Invalid table name.
	off = seekPos(lengths, 3)
	tests := map[string]*string{
		"nil table name":   nil,
		"empty table name": stringPtr(""),
	}
	for k, v := range tests {
		desc = k
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// Invalid TTL value/unit.
	off = seekPos(lengths, 10)
	if suite.bpTestClient.GetSerialVersion() < 3 {
		off -= 1 // Durability
	}
	desc = "invalid TTL value"
	copy(data, origData)
	suite.wr.Reset()
	// Use a negative value other than -1, because -1 is a valid value
	// that represents the TTL is not specified.
	suite.wr.WritePackedLong(-2)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)

	off += 1
	desc = "invalid TTL unit"
	copy(data, origData)
	suite.wr.Reset()
	suite.wr.WriteByte(byte(types.Hours - 1))
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
}

func (suite *BadProtocolTestSuite) TestBadDeleteRequest() {
	req := &nosqldb.DeleteRequest{
		TableName: suite.table,
		Key:       suite.key,
	}

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: string
		1,              // ReturnRow: boolean
		suite.keyLen,   // Key: map
		0,              // MatchVersion: bytes
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// Invalid table name.
	off = seekPos(lengths, 3)
	tests := map[string]*string{
		"nil table name":   nil,
		"empty table name": stringPtr(""),
	}
	for k, v := range tests {
		copy(data, origData)
		desc = k
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// OpCode=DeleteIfVersion but MatchVersion is not specified.
	off = seekPos(lengths, 1)
	copy(data, origData)
	desc = "DeleteIfVersion without specifying a MatchVersion"
	suite.wr.Reset()
	suite.wr.WriteOpCode(proto.DeleteIfVersion)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
}

func (suite *BadProtocolTestSuite) TestBadWriteMultipleRequest() {
	putReq := &nosqldb.PutRequest{
		TableName: suite.table,
		Value:     suite.value,
	}
	req := &nosqldb.WriteMultipleRequest{
		TableName: suite.table,
	}
	req.AddPutRequest(putReq, true)

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: string
		1,              // OperationNum: packed int
		1,              // Durability: 1 byte (serialVersion > 2)
		1,              // abortOnFail: boolean
		0,              // Sub requests: the size does not matter for this test.
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// Wrong number of operations.
	off = seekPos(lengths, 4)
	tests := []int{-1, 2}
	for _, v := range tests {
		desc = fmt.Sprintf("wrong number of operations %d", v)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WritePackedInt(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// Invalid opcode for sub requests.
	off = seekPos(lengths, 7)
	if suite.bpTestClient.GetSerialVersion() < 3 {
		off -= 1 // Durability
	}
	testOpCodes := []proto.OpCode{proto.OpCode(-1), proto.Get}
	for _, v := range testOpCodes {
		desc = fmt.Sprintf("invalid opcode %v", v)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteOpCode(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest2(req, data, desc, nosqlerr.BadProtocolMessage, nosqlerr.IllegalArgument)
	}

}

func (suite *BadProtocolTestSuite) TestBadMultiDeleteRequest() {
	req := &nosqldb.MultiDeleteRequest{
		TableName:       suite.table,
		Key:             suite.key,
		MaxWriteKB:      1024,
		ContinuationKey: test.GenBytes(20),
	}

	var desc string
	var off int
	lengths := []int{
		2,              // SerialVersion: short
		1,              // OpCode: byte
		3,              // RequestTimeout: packed int
		suite.tableLen, // TableName: string
		1,              // Durability: 1 byte (serialVersion > 2)
		suite.keyLen,   // Key: MapValue
		1,              // HasFieldRange: boolean
		3,              // MaxWriteKB: packed int
		21,             // ContinuationKey: byte array
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// Invalid MaxWriteKB.
	off = seekPos(lengths, 7)
	if suite.bpTestClient.GetSerialVersion() < 3 {
		off -= 1 // Durability
	}
	var tests []int
	if test.IsOnPrem() {
		// There is no limit on MaxWriteKB for the on-premise server.
		tests = []int{-1}
	} else {
		tests = []int{-1, test.MaxWriteKBLimit + 1}
	}

	for _, v := range tests {
		desc = fmt.Sprintf("invalid MaxWriteKB %v", v)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WritePackedInt(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}

	// Invalid length of ContinuationKey.
	off = seekPos(lengths, 8)
	if suite.bpTestClient.GetSerialVersion() < 3 {
		off -= 1 // Durability
	}
	tests = []int{-2, 100}
	for _, v := range tests {
		desc = fmt.Sprintf("invalid length of ContinuationKey %v", v)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WritePackedInt(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}
}

func (suite *BadProtocolTestSuite) TestBadTableRequest() {
	newTable := "ABCD"
	stmt := "create table if not exists " + newTable + " (id integer, primary key(id))"
	stmtLen, _ := suite.wr.WriteString(&stmt)
	limits := &nosqldb.TableLimits{
		ReadUnits:  50,
		WriteUnits: 50,
		StorageGB:  2,
	}
	req := &nosqldb.TableRequest{
		Statement:   stmt,
		TableLimits: limits,
	}

	var desc string
	var off int
	lengths := []int{
		2,       // SerialVersion: short
		1,       // OpCode: byte
		3,       // RequestTimeout: packed int
		stmtLen, // Statement: string
		1,       // HasLimit: boolean
		4,       // ReadKB: int
		4,       // WriteKB: int
		4,       // StorageGB: int
		1,       // LimitMode: byte (serialVersion > 2)
		1,       // HasTableName: boolean
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	suite.AddToTables(newTable)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Good request.
	desc = "OK test"
	suite.doBadProtoTest(req, data, desc, 0)

	// invalid readKB/writeKB/storageGB
	tests := []struct {
		desc  string
		index int
		value int
	}{
		{"invalid readKB", 5, 0},
		{"invalid readKB", 5, -1},
		{"invalid writeKB", 6, 0},
		{"invalid writeKB", 6, -1},
		{"invalid storageGB", 7, 0},
		{"invalid storageGB", 7, -1},
	}

	for _, r := range tests {
		desc = r.desc
		off = seekPos(lengths, r.index)
		copy(data, origData)
		suite.wr.Reset()
		suite.wr.WriteInt(r.value)
		copy(data[off:], suite.wr.Bytes())
		// depending on server version, these may return
		// BadProtocolMessage (older) or IllegalArgument (newer).
		suite.doBadProtoTest2(req, data, desc,
			nosqlerr.BadProtocolMessage,
			nosqlerr.IllegalArgument)
	}
}

func (suite *BadProtocolTestSuite) TestBadSystemRequest() {
	if !test.IsOnPrem() {
		suite.T().Skip("TestBadSystemRequest() can only run with an on-premise server")
	}

	ns := "namespace1"
	stmt := "create namespace if not exists " + ns
	stmtLen, _ := suite.wr.WriteString(&stmt)
	req := &nosqldb.SystemRequest{
		Statement: stmt,
	}

	var desc string
	var off int
	lengths := []int{
		2,       // SerialVersion: short
		1,       // OpCode: byte
		3,       // RequestTimeout: packed int
		stmtLen, // Statement: string
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Invalid statement.
	off = seekPos(lengths, 3)
	tests := map[string]*string{
		// BUG(zehliu) : A nil statement results in an UnknownError, which
		// should have resulted in a BadProtocolMessage error.
		// Disable this test until the bug gets fixed.
		//
		// "nil statement":   nil,
		"empty statement": stringPtr(""),
	}
	for k, v := range tests {
		copy(data, origData)
		desc = k
		suite.wr.Reset()
		suite.wr.WriteString(v)
		copy(data[off:], suite.wr.Bytes())
		suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
	}
}

func (suite *BadProtocolTestSuite) TestBadSystemStatusRequest() {
	if !test.IsOnPrem() {
		suite.T().Skip("TestBadSystemStatusRequest() can only run with an on-premise server")
	}

	ns := "namespace1"
	stmt := "create namespace if not exists " + ns
	stmtLen, _ := suite.wr.WriteString(&stmt)
	opID := "1234"
	opIDLen, _ := suite.wr.WriteString(&opID)
	req := &nosqldb.SystemStatusRequest{
		Statement:   stmt,
		OperationID: opID,
	}

	var desc string
	var off int
	lengths := []int{
		2,       // SerialVersion: short
		1,       // OpCode: byte
		3,       // RequestTimeout: packed int
		opIDLen, // OperationID: string
		stmtLen, // Statement: string
	}

	data, _, _, err := suite.bpTestClient.ProcessRequest(req)
	suite.Require().NoError(err)
	origData := make([]byte, len(data))
	copy(origData, data)

	// Invalid type of operation id
	off = seekPos(lengths, 3)
	var id int = 1234
	desc = fmt.Sprintf("invalid type of operation id: %T, want string", id)
	suite.wr.Reset()
	// Write operation id as an Integer, rather than a string.
	suite.wr.WritePackedInt(id)
	copy(data[off:], suite.wr.Bytes())
	suite.doBadProtoTest(req, data, desc, nosqlerr.BadProtocolMessage)
}

func TestBadProtocol(t *testing.T) {
	test := &BadProtocolTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
