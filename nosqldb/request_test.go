//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

var (
	testErrInvalidTimeout       = nosqlerr.NewIllegalArgument("Timeout must be greater than or equal to 1 millisecond")
	testErrInvalidConsistency   = nosqlerr.NewIllegalArgument("Consistency must be either Absolute or Eventual")
	testErrInvalidTableName     = nosqlerr.NewIllegalArgument("TableName must be non-empty")
	testErrNilKey               = nosqlerr.NewIllegalArgument("Key must be non-nil")
	testErrEmptyKey             = nosqlerr.NewIllegalArgument("Key must be non-empty")
	testErrNilTableLimits       = nosqlerr.NewIllegalArgument("TableLimits must be non-nil")
	testErrInvalidTableGB       = nosqlerr.NewIllegalArgument("TableLimits StorageGB must be positive")
	testErrInvalidTableUnits    = nosqlerr.NewIllegalArgument("TableLimits read/write units must be positive")
	testErrInvalidTableMode     = nosqlerr.NewIllegalArgument("TableLimits CapacityMode must be one of Provisioned or OnDemand")
	testErrInvalidOnDemandUnits = nosqlerr.NewIllegalArgument("TableLimits read/write units must be zero for OnDemand table")
)

// RequestTestSuite contains tests for the operation requests.
type RequestTestSuite struct {
	suite.Suite
}

func TestOpRequests(t *testing.T) {
	suite.Run(t, new(RequestTestSuite))
}

func TestQueryRequestCopyInternalPreservesQueryIntent(t *testing.T) {
	fpSpec := Decimal64
	structType := reflect.TypeOf(struct {
		ID int
	}{})
	req := &QueryRequest{
		Timeout:                    3 * time.Second,
		Limit:                      17,
		MaxReadKB:                  11,
		MaxWriteKB:                 12,
		MaxMemoryConsumption:       13,
		MaxServerMemoryConsumption: 14,
		FPArithSpec:                &fpSpec,
		Consistency:                types.Absolute,
		Durability: types.Durability{
			MasterSync:  types.SyncPolicySync,
			ReplicaSync: types.SyncPolicyNoSync,
			ReplicaAck:  types.ReplicaAckPolicySimpleMajority,
		},
		PreparedStatement: &PreparedStatement{},
		traceLevel:        4,
		TableName:         "T1",
		Namespace:         "ns1",
		LastWriteMetadata: "created-by:tester",
		StructType:        structType,
	}

	internal := req.copyInternal()

	if !internal.isInternal {
		t.Fatal("copyInternal() did not mark the copied request as internal")
	}
	if internal.Namespace != req.Namespace {
		t.Fatalf("copyInternal() Namespace=%q, want %q", internal.Namespace, req.Namespace)
	}
	if internal.MaxServerMemoryConsumption != req.MaxServerMemoryConsumption {
		t.Fatalf("copyInternal() MaxServerMemoryConsumption=%d, want %d",
			internal.MaxServerMemoryConsumption, req.MaxServerMemoryConsumption)
	}
	if internal.FPArithSpec != req.FPArithSpec {
		t.Fatal("copyInternal() did not preserve FPArithSpec")
	}
	if internal.StructType != req.StructType {
		t.Fatal("copyInternal() did not preserve StructType")
	}
}

func TestQueryRequestRejectsVirtualScanQueryVersionDowngrade(t *testing.T) {
	req := &QueryRequest{
		Statement:   "select * from T1",
		virtualScan: &virtualScan{shardID: 1, partitionID: 2},
	}

	err := req.serialize(binary.NewWriter(), 4, proto.QueryV3)
	if err == nil {
		t.Fatal("expected virtual scan query version downgrade to fail")
	}
	if !nosqlerr.Is(err, nosqlerr.IllegalArgument) {
		t.Fatalf("got error %v, want IllegalArgument", err)
	}
}

func TestQueryRequestRejectsSerialV3UnsupportedIntent(t *testing.T) {
	tests := []struct {
		name string
		req  *QueryRequest
	}{
		{
			name: "namespace",
			req:  &QueryRequest{Statement: "select * from T1", Namespace: "ns1"},
		},
		{
			name: "server memory",
			req:  &QueryRequest{Statement: "select * from T1", MaxServerMemoryConsumption: 1},
		},
		{
			name: "last write metadata",
			req:  &QueryRequest{Statement: "delete from T1 where id = 1", LastWriteMetadata: "deleted-by:tester"},
		},
		{
			name: "virtual scan",
			req:  &QueryRequest{Statement: "select * from T1", virtualScan: &virtualScan{shardID: 1}},
		},
		{
			name: "durability",
			req: &QueryRequest{
				Statement: "delete from T1 where id = 1",
				Durability: types.Durability{
					MasterSync:  types.SyncPolicySync,
					ReplicaSync: types.SyncPolicyNoSync,
					ReplicaAck:  types.ReplicaAckPolicySimpleMajority,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.serializeV3(binary.NewWriter(), 3)
			if err == nil {
				t.Fatal("expected serial V3 serialization to reject unsupported query intent")
			}
			if !nosqlerr.Is(err, nosqlerr.IllegalArgument) {
				t.Fatalf("got error %v, want IllegalArgument", err)
			}
		})
	}
}

func TestQueryRequestSerialV3AllowsRepresentableIntent(t *testing.T) {
	req := &QueryRequest{
		Statement:   "select * from T1",
		Limit:       10,
		MaxReadKB:   20,
		MaxWriteKB:  30,
		Consistency: types.Eventual,
	}

	if err := req.serializeV3(binary.NewWriter(), 3); err != nil {
		t.Fatalf("serializeV3() got unexpected error: %v", err)
	}
}

func (suite *RequestTestSuite) TestValidateTimeout() {
	tests := []struct {
		timeout time.Duration
		want    error
	}{
		{0, testErrInvalidTimeout},
		{time.Duration(0), testErrInvalidTimeout},
		{time.Duration(-1), testErrInvalidTimeout},
		{time.Millisecond - 1, testErrInvalidTimeout},
		{time.Millisecond, nil},
		{time.Second, nil},
		{time.Minute, nil},
	}

	for i, r := range tests {
		err := validateTimeout(r.timeout)
		suite.Equalf(r.want, err, "Testcase %d: validateTimeout(%v) got unexpected error", i+1, r.timeout)
	}
}

func (suite *RequestTestSuite) TestValidateConsistency() {
	tests := []struct {
		c    types.Consistency
		want error
	}{
		{0, testErrInvalidConsistency},
		{3, testErrInvalidConsistency},
		{1, nil},
		{2, nil},
		{types.Eventual, nil},
		{types.Absolute, nil},
	}

	for i, r := range tests {
		err := validateConsistency(r.c)
		suite.Equalf(r.want, err, "Testcase %d: validateConsistency(%v) got unexpected error", i+1, r.c)
	}
}

func (suite *RequestTestSuite) TestValidateTableName() {
	tests := []struct {
		table string
		want  error
	}{
		{"", testErrInvalidTableName},
		{" ", nil},
		{"T", nil},
	}

	for i, r := range tests {
		err := validateTableName(r.table)
		suite.Equalf(r.want, err, "Testcase %d: validateTableName(%v) got unexpected error", i+1, r.table)
	}
}

func (suite *RequestTestSuite) TestValidateQueryRequestSource() {
	prepStmt := &PreparedStatement{}
	tests := []struct {
		name string
		req  *QueryRequest
		want error
	}{
		{
			name: "neither statement nor prepared statement",
			req:  &QueryRequest{Timeout: time.Millisecond, Consistency: types.Absolute},
			want: nosqlerr.NewIllegalArgument("QueryRequest: either Statement or PreparedStatement should be set"),
		},
		{
			name: "both statement and prepared statement",
			req: &QueryRequest{
				Statement:         "select * from users",
				PreparedStatement: prepStmt,
				Timeout:           time.Millisecond,
				Consistency:       types.Absolute,
			},
			want: nosqlerr.NewIllegalArgument("QueryRequest: Statement and PreparedStatement cannot both be set"),
		},
		{
			name: "only statement",
			req: &QueryRequest{
				Statement:   "select * from users",
				Timeout:     time.Millisecond,
				Consistency: types.Absolute,
			},
		},
		{
			name: "only prepared statement",
			req: &QueryRequest{
				PreparedStatement: prepStmt,
				Timeout:           time.Millisecond,
				Consistency:       types.Absolute,
			},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.Equal(tt.want, tt.req.validate())
		})
	}
}

func (suite *RequestTestSuite) TestValidateKey() {
	tests := []struct {
		key  *types.MapValue
		want error
	}{
		{nil, testErrNilKey},
		{&types.MapValue{}, testErrEmptyKey},
		{types.NewMapValue(map[string]interface{}{"id": 1}), nil},
		{types.NewMapValue(map[string]interface{}{"id": 1, "desc": "testing"}), nil},
	}

	for i, r := range tests {
		err := validateKey(r.key)
		suite.Equalf(r.want, err, "Testcase %d: validateKey(%v) got unexpected error", i+1, r.key)
	}
}

func (suite *RequestTestSuite) TestValidateTableLimits() {
	tests := []struct {
		limits *TableLimits
		want   error
	}{
		{nil, testErrNilTableLimits},
		{&TableLimits{}, testErrInvalidTableGB},
		{&TableLimits{0, 0, 0, 2}, testErrInvalidTableMode},
		{&TableLimits{1, 1, 1, 0}, nil},
		{&TableLimits{1, 1, 1, 1}, testErrInvalidOnDemandUnits},
		{&TableLimits{0, 0, 1, 1}, nil},
		{ProvisionedTableLimits(0, 0, 0), testErrInvalidTableGB},
		{ProvisionedTableLimits(0, 1, 1), testErrInvalidTableUnits},
		{ProvisionedTableLimits(1, 0, 1), testErrInvalidTableUnits},
		{ProvisionedTableLimits(1, 1, 0), testErrInvalidTableGB},
		{ProvisionedTableLimits(1, 1, 1), nil},
		{OnDemandTableLimits(0), testErrInvalidTableGB},
		{OnDemandTableLimits(1), nil},
	}

	for i, r := range tests {
		err := validateTableLimits(r.limits)
		suite.Equalf(r.want, err, "Testcase %d: validateTableLimits(%v) got unexpected error", i+1, r.limits)
	}
}

func (suite *RequestTestSuite) TestGetRequest() {
	invalidC0 := types.Consistency(0)
	invalidC3 := types.Consistency(3)
	goodPK := types.NewMapValue(map[string]interface{}{"id": 1})

	tests := []struct {
		req  *GetRequest
		want error
		cfg  *RequestConfig
		set  bool // whether to call setDefaults
	}{
		// check TableName
		{
			req:  &GetRequest{},
			want: testErrInvalidTableName,
		},
		{
			req:  &GetRequest{TableName: ""},
			want: testErrInvalidTableName,
		},
		// check Key
		{
			req:  &GetRequest{TableName: "T"},
			want: testErrNilKey,
		},
		{
			req:  &GetRequest{TableName: "T", Key: nil},
			want: testErrNilKey,
		},
		{
			req:  &GetRequest{TableName: "T", Key: &types.MapValue{}},
			want: testErrEmptyKey,
		},
		{
			req:  &GetRequest{TableName: "T", Key: types.NewMapValue(nil)},
			want: testErrEmptyKey,
		},
		// check Timeout
		{
			req:  &GetRequest{TableName: "T", Key: goodPK},
			want: testErrInvalidTimeout,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: 0},
			want: testErrInvalidTimeout,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond - 1},
			want: testErrInvalidTimeout,
		},
		// check Timeout after setDefaults
		// GetRequest.Timeout as zero value
		{
			req:  &GetRequest{TableName: "T", Key: goodPK},
			want: nil,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK},
			want: nil,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK},
			want: nil,
			cfg:  &RequestConfig{RequestTimeout: time.Millisecond},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK},
			want: testErrInvalidTimeout,
			cfg:  &RequestConfig{RequestTimeout: time.Millisecond - 1},
			set:  true,
		},
		// GetRequest.Timeout = 1s
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Second},
			want: nil,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Second},
			want: nil,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Second},
			want: nil,
			cfg:  &RequestConfig{RequestTimeout: time.Millisecond},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Second},
			want: nil,
			cfg:  &RequestConfig{RequestTimeout: time.Millisecond - 1},
			set:  true,
		},
		// GetRequest.Timeout = 1ms - 1
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond - 1},
			want: testErrInvalidTimeout,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond - 1},
			want: testErrInvalidTimeout,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond - 1},
			want: testErrInvalidTimeout,
			cfg:  &RequestConfig{RequestTimeout: time.Millisecond},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond - 1},
			want: testErrInvalidTimeout,
			cfg:  &RequestConfig{RequestTimeout: time.Millisecond - 1},
			set:  true,
		},
		// check Consistency
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond},
			want: testErrInvalidConsistency,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: invalidC0},
			want: testErrInvalidConsistency,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: invalidC3},
			want: testErrInvalidConsistency,
		},
		// check Consistency after setDefaults
		// GetRequest.Consistency not specified
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond},
			want: nil,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond},
			want: nil,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond},
			want: nil,
			cfg:  &RequestConfig{Consistency: types.Eventual},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond},
			want: testErrInvalidConsistency,
			cfg:  &RequestConfig{Consistency: 3},
			set:  true,
		},
		// GetRequest.Consistency = Absolute
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Absolute},
			want: nil,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Absolute},
			want: nil,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Absolute},
			want: nil,
			cfg:  &RequestConfig{Consistency: types.Eventual},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Absolute},
			want: nil,
			cfg:  &RequestConfig{Consistency: 3},
			set:  true,
		},
		// GetRequest.Consistency = Eventual
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Eventual},
			want: nil,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Eventual},
			want: nil,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Eventual},
			want: nil,
			cfg:  &RequestConfig{Consistency: types.Absolute},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Eventual},
			want: nil,
			cfg:  &RequestConfig{Consistency: 3},
			set:  true,
		},
		// GetRequest.Consistency = 3 (invalid)
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: invalidC3},
			want: testErrInvalidConsistency,
			cfg:  nil,
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: invalidC3},
			want: testErrInvalidConsistency,
			cfg:  &RequestConfig{},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: invalidC3},
			want: testErrInvalidConsistency,
			cfg:  &RequestConfig{Consistency: types.Absolute},
			set:  true,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: invalidC3},
			want: testErrInvalidConsistency,
			cfg:  &RequestConfig{Consistency: 3},
			set:  true,
		},
		// positive
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Absolute},
			want: nil,
		},
		{
			req:  &GetRequest{TableName: "T", Key: goodPK, Timeout: time.Millisecond, Consistency: types.Eventual},
			want: nil,
		},
	}

	var wantTO time.Duration
	var wantC types.Consistency
	for i, r := range tests {
		// timeout and consistency values before setDefaults
		origTO := r.req.Timeout
		origC := r.req.Consistency
		if r.set {
			r.req.setDefaults(r.cfg)
		}
		err := r.req.validate()
		suite.Equalf(r.want, err, "Testcase %d: validate(GetRequest=%#v) got unexpected error", i+1, r.req)

		// check if timeout and consistency values are correct after setDefaults
		if r.set && err == nil && r.want == nil {
			if origTO != 0 {
				wantTO = origTO
			} else {
				wantTO = r.cfg.DefaultRequestTimeout()
			}
			suite.Equalf(wantTO, r.req.Timeout, "Testcase %d: unexpected Timeout value for GetRequest", i+1)

			if origC != 0 {
				wantC = origC
			} else {
				wantC = r.cfg.DefaultConsistency()
			}
			suite.Equalf(wantC, r.req.Consistency, "Testcase %d: unexpected Consistency value for GetRequest", i+1)
		}
	}

}

func (suite *RequestTestSuite) TestReflectStruct() {
	r := GetRequest{}
	t := reflect.TypeOf(r)
	if t.Kind() != reflect.Struct {
		suite.Fail("GetRequest not a struct")
	}
	fmt.Printf("Type=%v Kind=%v numField=%d\n", t.Name(), t.Kind(), t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		if field.Type.Kind() == reflect.Pointer || field.Type.Kind() == reflect.Array {
			elem := field.Type.Elem()
			fmt.Printf("%d %v (kind=%v to %v) tag='%v'\n", i+1, field.Name, field.Type.Kind(), elem.Name(), tag)
		} else {
			fmt.Printf("%d %v (%v, kind=%v) tag='%v'\n", i+1, field.Name, field.Type.Name(), field.Type.Kind(), tag)
		}
	}
}
