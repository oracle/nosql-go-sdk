//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

var (
	testErrInvalidTimeout     = nosqlerr.NewIllegalArgument("Timeout must be greater than or equal to 1 millisecond")
	testErrInvalidConsistency = nosqlerr.NewIllegalArgument("Consistency must be either Absolute or Eventual")
	testErrInvalidTableName   = nosqlerr.NewIllegalArgument("TableName must be non-empty")
	testErrNilKey             = nosqlerr.NewIllegalArgument("Key must be non-nil")
	testErrEmptyKey           = nosqlerr.NewIllegalArgument("Key must be non-empty")
	testErrNilTableLimits     = nosqlerr.NewIllegalArgument("TableLimits must be non-nil")
	testErrInvalidTableLimits = nosqlerr.NewIllegalArgument("TableLimits values must be positive.")
)

// RequestTestSuite contains tests for the operation requests.
type RequestTestSuite struct {
	suite.Suite
}

func TestOpRequests(t *testing.T) {
	suite.Run(t, new(RequestTestSuite))
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
		{&TableLimits{}, testErrInvalidTableLimits},
		{&TableLimits{0, 0, 0}, testErrInvalidTableLimits},
		{&TableLimits{0, 1, 1}, testErrInvalidTableLimits},
		{&TableLimits{1, 0, 1}, testErrInvalidTableLimits},
		{&TableLimits{1, 1, 0}, testErrInvalidTableLimits},
		{&TableLimits{1, 1, 1}, nil},
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
