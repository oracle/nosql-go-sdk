//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// This software is licensed with the Universal Permissive License (UPL) version 1.0
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// +build cloud onprem

package nosqldb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// MultiDeleteTestSuite tests the MultiDelete API.
type MultiDeleteTestSuite struct {
	*test.NoSQLTestSuite
	table string
}

func (suite *MultiDeleteTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()

	suite.table = suite.GetTableName("MultiDeleteTable")
	// Create a test table.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sid INTEGER, "+
		"id INTEGER, "+
		"name STRING, "+
		"longString STRING, "+
		"PRIMARY KEY(SHARD(sid), id))", suite.table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  10000,
		WriteUnits: 10000,
		StorageGB:  50,
	}
	suite.ReCreateTable(suite.table, stmt, limits)
}

func (suite *MultiDeleteTestSuite) TestMultiDelete() {
	numMajor, numPerMajor, recordKB := 5, 100, 2
	suite.loadRows(numMajor, numPerMajor, recordKB)

	testCases := []struct {
		key           *types.MapValue
		fr            *types.FieldRange
		maxWriteKB    uint // maximum write KB
		expNumDeleted int  // expected number of rows deleted
		recordKB      int
	}{
		// Deletes rows with a non-exists key
		{types.ToMapValue("sid", -numMajor), nil, 0, 0, recordKB},
		// Deletes rows with the shard key {"sid":0}, maxWriteKB = 0
		{types.ToMapValue("sid", 0), nil, 0, numPerMajor, recordKB},
		// Deletes rows with the shard key {"sid":1}, maxWriteKB = 10
		{types.ToMapValue("sid", 1), nil, 10, numPerMajor, recordKB},
		// Deletes rows with the shard key {"sid":2}, maxWriteKB = 51
		{types.ToMapValue("sid", 2), nil, 51, numPerMajor, recordKB},
		// Deletes rows with shard key {"sid":3} and "id" < 10, maxWriteKB = 8
		{
			types.ToMapValue("sid", 3),
			&types.FieldRange{
				FieldPath:    "id",
				End:          10,
				EndInclusive: false,
			},
			8,
			10,
			recordKB,
		},
		// Deletes rows with shard key {"sid":3} and 10 <= "id" <= 19, maxWriteKB = 18
		{
			types.ToMapValue("sid", 3),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          10,
				StartInclusive: true,
				End:            19,
				EndInclusive:   true,
			},
			8,
			10,
			recordKB,
		},
		// Deletes rows with shard key {"sid":3} and 20 <= "id" < 31, maxWriteKB = 20
		{
			types.ToMapValue("sid", 3),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          20,
				StartInclusive: true,
				End:            31,
				EndInclusive:   false,
			},
			20,
			11,
			recordKB,
		},
		// Deletes rows with shard key {"sid":3} and "id" >= 31, maxWriteKB = 25
		{
			types.ToMapValue("sid", 3),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          31,
				StartInclusive: true,
			},
			25,
			numPerMajor - 31,
			recordKB,
		},
		// Try delete again, expect 0 rows deleted.
		// Deletes rows with shard key {"sid":3} and "id" >= 31, maxWriteKB = 25
		{
			types.ToMapValue("sid", 3),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          31,
				StartInclusive: true,
			},
			25,
			0, // expNumDeleted
			recordKB,
		},
		// Deletes rows with shard key {"sid":4} and "id" <= 10, maxWriteKB = 0
		{
			types.ToMapValue("sid", 4),
			&types.FieldRange{
				FieldPath:    "id",
				End:          10,
				EndInclusive: true,
			},
			0,
			11,
			recordKB,
		},
		// Deletes rows with shard key {"sid":4} and 10 < "id" <= 20, maxWriteKB = 0
		{
			types.ToMapValue("sid", 4),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          10,
				StartInclusive: false,
				End:            20,
				EndInclusive:   true,
			},
			0,
			10,
			recordKB,
		},
		// Deletes rows with shard key {"sid":4} and "id" > 20, maxWriteKB = 0
		{
			types.ToMapValue("sid", 4),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          20,
				StartInclusive: false,
			},
			0,
			numPerMajor - 21,
			recordKB,
		},
		// Deletes rows with shard key {"sid": 4} and "id" > numPerMajor
		// Expect 0 rows deleted.
		{
			types.ToMapValue("sid", 4),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          numPerMajor,
				StartInclusive: false,
			},
			0,
			0,
			recordKB,
		},
	}

	for _, r := range testCases {
		suite.runMultiDelete(r.key, r.fr, r.maxWriteKB, r.expNumDeleted, r.recordKB)
	}

}

func (suite *MultiDeleteTestSuite) TestIllegalMultiDelete() {
	okKey := types.ToMapValue("sid", 1)
	table := suite.table
	type multiDeleteTestCase struct {
		table      string
		key        *types.MapValue
		fr         *types.FieldRange
		maxWriteKB uint
		timeout    time.Duration
		expErr     nosqlerr.ErrorCode
	}

	var test01, test02 []*multiDeleteTestCase
	test01 = []*multiDeleteTestCase{
		// invalid table name
		//
		// table name is empty
		{"", okKey, nil, 0, test.OkTimeout, nosqlerr.IllegalArgument},
		// table does not exist
		{"not_exist_table", okKey, nil, 0, test.OkTimeout, nosqlerr.TableNotFound},
		// invalid primary key
		//
		// nil primary key
		{table, nil, nil, 0, test.OkTimeout, nosqlerr.IllegalArgument},
		// the specified "name" field is not a primary key
		{table, types.ToMapValue("name", "n"), nil, 0, test.OkTimeout, nosqlerr.IllegalArgument},
		// missing shard field "sid" from the primary key
		{table, types.ToMapValue("id", 0), nil, 0, test.OkTimeout, nosqlerr.IllegalArgument},
		// invalid FieldRange
		//
		// the specified "name" field does not exist in primary key
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath:      "name",
				Start:          "Joan",
				StartInclusive: false,
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// Do not specify Start or End value for the field range
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath: "id",
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// Start value does not match the type of specified "id" field
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath:    "id",
				End:          "First ID",
				EndInclusive: false,
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// End value does not match the type of specified "id" field
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath:    "id",
				End:          "First ID",
				EndInclusive: false,
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// invalid FieldRange, the types of Start/End value do not match
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          1,
				StartInclusive: false,
				End:            "TestEnd",
				EndInclusive:   true,
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// invalid FieldRange, End < Start
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          1,
				StartInclusive: false,
				End:            0,
				EndInclusive:   true,
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// invalid FieldRange, End == Start
		{
			table,
			types.ToMapValue("sid", 0),
			&types.FieldRange{
				FieldPath:      "id",
				Start:          1,
				StartInclusive: false,
				End:            1,
				EndInclusive:   false,
			},
			0,
			test.OkTimeout,
			nosqlerr.IllegalArgument,
		},
		// invalid timeout
		{table, types.ToMapValue("sid", 0), nil, 0, test.BadTimeout, nosqlerr.IllegalArgument},
	}

	if test.IsCloud() {
		test02 = []*multiDeleteTestCase{
			// maxWriteKB exceeds the maximum value
			{table, types.ToMapValue("sid", 0), nil, test.MaxWriteKBLimit + 1, test.OkTimeout, nosqlerr.IllegalArgument},
		}
	} else {
		test02 = []*multiDeleteTestCase{
			// This request should be OK as on-prem does not enforce maxWriteKB limits.
			{table, types.ToMapValue("sid", 0), nil, test.MaxWriteKBLimit + 1, test.OkTimeout, nosqlerr.NoError},
		}
	}

	test01 = append(test01, test02...)
	for i, r := range test01 {
		req := &nosqldb.MultiDeleteRequest{
			TableName:  r.table,
			Key:        r.key,
			FieldRange: r.fr,
			MaxWriteKB: r.maxWriteKB,
			Timeout:    r.timeout,
		}
		_, err := suite.Client.MultiDelete(req)
		switch r.expErr {
		case nosqlerr.NoError:
			suite.NoErrorf(err, "Testcase %d: MultiDelete(req=%#v) should have succeeded, got error: %v.",
				i+1, req, err)

		default:
			suite.Truef(nosqlerr.Is(err, r.expErr),
				"Testcase %d: MultiDelete(req=%#v) should have failed with %v, got error: %v.",
				i+1, req, r.expErr, err)
		}
	}
}

func (suite *MultiDeleteTestSuite) loadRows(numMajor, numPerMajor, nKB int) {
	n := (nKB - 1) * 1024
	for i := 0; i < numMajor; i++ {
		v := &types.MapValue{}
		v.Put("sid", i)

		for j := 0; j < numPerMajor; j++ {
			v.Put("id", j)
			v.Put("name", fmt.Sprintf("name_%d_%d", i, j))
			v.Put("longString", test.GenString(n))
			req := &nosqldb.PutRequest{
				TableName: suite.table,
				Value:     v,
			}
			res, err := suite.Client.Put(req)
			if suite.NoError(err) {
				suite.NotNil(res.Version)
			}
		}
	}
}

func (suite *MultiDeleteTestSuite) runMultiDelete(key *types.MapValue, fieldRange *types.FieldRange,
	maxWriteKB uint, expNumDeleted, recordKB int) {

	var nDeleted, totalReadKB, totalWriteKB, totalReadUnits int
	var expReadKB, expWriteKB, writeKBLimit int

	minRead := test.MinReadKB
	if maxWriteKB > 0 {
		writeKBLimit = int(maxWriteKB)
	} else {
		writeKBLimit = test.MaxWriteKBLimit
	}

	var req *nosqldb.MultiDeleteRequest
	var res *nosqldb.MultiDeleteResult
	var err error

	req = &nosqldb.MultiDeleteRequest{
		TableName:  suite.table,
		Key:        key,
		FieldRange: fieldRange,
		MaxWriteKB: maxWriteKB,
	}

	for {

		res, err = suite.Client.MultiDelete(req)
		if !suite.NoErrorf(err, "MultiDelete(req=%#v) failed, got error %v.", err) {
			break
		}

		nDeleted += res.NumDeleted
		totalReadKB += res.ReadKB
		totalWriteKB += res.WriteKB
		totalReadUnits += res.ReadUnits

		if res.NumDeleted > 0 {
			if test.IsCloud() {
				suite.Greaterf(res.ReadKB, 0, "readKB should be > 0")
				suite.Greaterf(res.WriteKB, 0, "writeKB should be > 0")
			} else {
				suite.Equalf(0, res.ReadKB, "readKB should be 0")
				suite.Equalf(0, res.WriteKB, "writeKB should be 0")
			}

		} else {
			expReadKB += minRead
		}

		if res.ContinuationKey == nil {
			break
		}

		req.ContinuationKey = res.ContinuationKey

		if test.IsCloud() {
			// assert writeKBLimit <= writeKB < writeKBLimit+recordKB
			suite.GreaterOrEqualf(res.WriteKB, writeKBLimit, "writeKB should be >= %d.", writeKBLimit)
			suite.LessOrEqualf(res.WriteKB, writeKBLimit+recordKB, "writeKB should be < %d.", writeKBLimit+recordKB)
		}
	}

	suite.Equalf(expNumDeleted, nDeleted, "unexpected number of rows deleted.")

	if test.IsCloud() {
		expWriteKB += nDeleted * recordKB
		expReadKB += nDeleted * minRead
	} else {
		expWriteKB = 0
		expReadKB = 0
	}

	test.AssertReadKB(suite.Assert(), expReadKB, totalReadKB, totalReadUnits,
		0, /* prepare cost */
		true /* isAbsolute */)
	test.AssertWriteKB(suite.Assert(), expWriteKB, totalWriteKB)
}

func TestMultiDelete(t *testing.T) {
	test := &MultiDeleteTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
