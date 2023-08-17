//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/assert"
)

func TestCompareAtomicValues(t *testing.T) {

	var nilStrPtr *string
	emptyStr, hello, world := "", "Hello", "World"
	time1 := time.Now()
	time2 := time1.Add(time.Hour)

	// numerics < timestamps < strings < booleans < binaries < empty < json null < null

	test01 := []struct {
		v1, v2       types.FieldValue
		comp         int
		incompatible bool
	}{
		// int vs int
		{int(-2), int(0), -1, false},
		{int(-2), int(-2), 0, false},
		{int(2), int(-2), 1, false},
		// int vs int64
		{int(-2), int64(0), -1, false},
		{int(-2), int64(-2), 0, false},
		{int(2), int64(-2), 1, false},
		// int vs float64
		{int(-2), float64(0), -1, false},
		{int(-2), float64(-2), 0, false},
		{int(2), float64(-2), 1, false},
		// int vs *big.Rat
		{int(-2), new(big.Rat).SetInt64(int64(0)), -1, false},
		{int(-2), new(big.Rat).SetInt64(int64(-2)), 0, false},
		{int(2), new(big.Rat).SetInt64(int64(-2)), 1, false},
		// int vs string
		{int(-2), emptyStr, -1, false},
		{int(-2), hello, -1, false},
		{int(2), world, -1, false},
		// int vs *string
		{int(-2), nilStrPtr, -1, false},
		{int(-2), &emptyStr, -1, false},
		{int(-2), &hello, -1, false},
		{int(2), &world, -1, false},
		// int vs bool
		{int(-2), true, -1, false},
		{int(-2), false, -1, false},
		// int vs time
		{int(2), time1, -1, false},
		// int vs Empty
		{int(2), types.EmptyValueInstance, -1, false},
		// int vs JSONNull
		{int(2), types.JSONNullValueInstance, -1, false},
		// int vs Null
		{int(2), types.NullValueInstance, -1, false},

		// int64 vs int
		{int64(-2), int(0), -1, false},
		{int64(-2), int(-2), 0, false},
		{int64(2), int(-2), 1, false},
		// int64 vs int64
		{int64(-2), int64(0), -1, false},
		{int64(-2), int64(-2), 0, false},
		{int64(2), int64(-2), 1, false},
		// int64 vs float64
		{int64(-2), float64(0), -1, false},
		{int64(-2), float64(-2), 0, false},
		{int64(2), float64(-2), 1, false},
		// int64 vs *big.Rat
		{int64(-2), new(big.Rat).SetInt64(int64(0)), -1, false},
		{int64(-2), new(big.Rat).SetInt64(int64(-2)), 0, false},
		{int64(2), new(big.Rat).SetInt64(int64(-2)), 1, false},
		// int64 vs string
		{int64(-2), emptyStr, -1, false},
		{int64(-2), hello, -1, false},
		{int64(2), world, -1, false},
		// int64 vs *string
		{int64(-2), nilStrPtr, -1, false},
		{int64(-2), &emptyStr, -1, false},
		{int64(-2), &hello, -1, false},
		{int64(2), &world, -1, false},
		// int64 vs bool
		{int64(-2), true, -1, false},
		{int64(-2), false, -1, false},
		{int64(2), false, -1, false},
		// int64 vs time
		{int64(2), time1, -1, false},
		{int64(2), time1, -1, false},
		// int64 vs Empty
		{int64(2), types.EmptyValueInstance, -1, false},
		// int64 vs JSONNull
		{int64(2), types.JSONNullValueInstance, -1, false},
		// int64 vs Null
		{int64(2), types.NullValueInstance, -1, false},

		// float64 vs int
		{float64(-2), int(0), -1, false},
		{float64(-2), int(-2), 0, false},
		{float64(2), int(-2), 1, false},
		// float64 vs int64
		{float64(-2), int64(0), -1, false},
		{float64(-2), int64(-2), 0, false},
		{float64(2), int64(-2), 1, false},
		// float64 vs float64
		{float64(-2), float64(0), -1, false},
		{float64(-2), float64(-2), 0, false},
		{float64(2), float64(-2), 1, false},
		// float64 vs *big.Rat
		{float64(-2), new(big.Rat).SetInt64(int64(0)), -1, false},
		{float64(-2), new(big.Rat).SetInt64(int64(-2)), 0, false},
		{float64(2), new(big.Rat).SetInt64(int64(-2)), 1, false},
		// float64 vs string
		{float64(-2), emptyStr, -1, false},
		{float64(-2), hello, -1, false},
		{float64(2), world, -1, false},
		// float64 vs *string
		{float64(-2), nilStrPtr, -1, false},
		{float64(-2), &emptyStr, -1, false},
		{float64(-2), &hello, -1, false},
		{float64(2), &world, -1, false},
		// float64 vs bool
		{float64(-2), true, -1, false},
		{float64(-2), false, -1, false},
		{float64(2), false, -1, false},
		// float64 vs time
		{float64(2), time1, -1, false},
		{float64(2), time1, -1, false},
		// float64 vs Empty
		{float64(2), types.EmptyValueInstance, -1, false},
		// float64 vs JSONNull
		{float64(2), types.JSONNullValueInstance, -1, false},
		// float64 vs Null
		{float64(2), types.NullValueInstance, -1, false},

		// *big.Rat vs int
		{new(big.Rat).SetInt64(int64(-2)), int(0), -1, false},
		{new(big.Rat).SetInt64(int64(-2)), int(-2), 0, false},
		{new(big.Rat).SetInt64(int64(2)), int(-2), 1, false},
		// *big.Rat vs int64
		{new(big.Rat).SetInt64(int64(-2)), int64(0), -1, false},
		{new(big.Rat).SetInt64(int64(-2)), int64(-2), 0, false},
		{new(big.Rat).SetInt64(int64(2)), int64(-2), 1, false},
		// *big.Rat vs float64
		{new(big.Rat).SetInt64(int64(-2)), float64(0), -1, false},
		{new(big.Rat).SetInt64(int64(-2)), float64(-2), 0, false},
		{new(big.Rat).SetInt64(int64(2)), float64(-2), 1, false},
		// *big.Rat vs *big.Rat
		{new(big.Rat).SetInt64(int64(-2)), new(big.Rat).SetInt64(int64(0)), -1, false},
		{new(big.Rat).SetInt64(int64(-2)), new(big.Rat).SetInt64(int64(-2)), 0, false},
		{new(big.Rat).SetInt64(int64(2)), new(big.Rat).SetInt64(int64(-2)), 1, false},
		// *big.Rat vs string
		{new(big.Rat).SetInt64(int64(-2)), emptyStr, -1, false},
		{new(big.Rat).SetInt64(int64(-2)), hello, -1, false},
		{new(big.Rat).SetInt64(int64(2)), world, -1, false},
		// *big.Rat vs *string
		{new(big.Rat).SetInt64(int64(-2)), nilStrPtr, -1, false},
		{new(big.Rat).SetInt64(int64(-2)), &emptyStr, -1, false},
		{new(big.Rat).SetInt64(int64(-2)), &hello, -1, false},
		{new(big.Rat).SetInt64(int64(2)), &world, -1, false},
		// *big.Rat vs bool
		{new(big.Rat).SetInt64(int64(-2)), true, -1, false},
		{new(big.Rat).SetInt64(int64(-2)), false, -1, false},
		// *big.Rat vs time
		{new(big.Rat).SetInt64(int64(2)), time1, -1, false},
		// *big.Rat vs Empty
		{new(big.Rat).SetInt64(int64(2)), types.EmptyValueInstance, -1, false},
		// *big.Rat vs JSONNull
		{new(big.Rat).SetInt64(int64(2)), types.JSONNullValueInstance, -1, false},
		// *big.Rat vs Null
		{new(big.Rat).SetInt64(int64(2)), types.NullValueInstance, -1, false},

		// string vs int
		{emptyStr, int(0), 1, false},
		{hello, int(-2), 1, false},
		{world, int(-2), 1, false},
		// string vs int64
		{emptyStr, int64(0), 1, false},
		{hello, int64(-2), 1, false},
		{world, int64(-2), 1, false},
		// string vs float64
		{emptyStr, float64(0), 1, false},
		{hello, float64(-2), 1, false},
		{world, float64(-2), 1, false},
		// string vs *big.Rat
		{emptyStr, new(big.Rat).SetInt64(int64(0)), 1, false},
		{hello, new(big.Rat).SetInt64(int64(-2)), 1, false},
		{world, new(big.Rat).SetInt64(int64(-2)), 1, false},
		// string vs string
		{emptyStr, hello, -1, false},
		{hello, hello, 0, false},
		{world, hello, 1, false},
		// string vs *string
		{emptyStr, nilStrPtr, 1, false},
		{emptyStr, &hello, -1, false},
		{hello, &hello, 0, false},
		{world, &hello, 1, false},
		// string vs bool
		{emptyStr, true, -1, false},
		{hello, false, -1, false},
		{world, false, -1, false},
		// string vs time
		{hello, time1, 1, false},
		{world, time1, 1, false},
		// string vs Empty
		{hello, types.EmptyValueInstance, -1, false},
		// string vs JSONNull
		{hello, types.JSONNullValueInstance, -1, false},
		// string vs Null
		{hello, types.NullValueInstance, -1, false},

		// bool vs int
		{true, int(0), 1, false},
		{false, int(-2), 1, false},
		{false, int(-2), 1, false},
		// bool vs int64
		{true, int64(0), 1, false},
		{false, int64(-2), 1, false},
		{false, int64(-2), 1, false},
		// bool vs float64
		{true, float64(0), 1, false},
		{false, float64(-2), 1, false},
		{false, float64(-2), 1, false},
		// bool vs *big.Rat
		{true, new(big.Rat).SetInt64(int64(0)), 1, false},
		{false, new(big.Rat).SetInt64(int64(-2)), 1, false},
		{false, new(big.Rat).SetInt64(int64(-2)), 1, false},
		// bool vs string
		{true, hello, 1, false},
		{false, hello, 1, false},
		{false, hello, 1, false},
		// bool vs *string
		{true, nilStrPtr, 1, false},
		{false, &hello, 1, false},
		{false, &hello, 1, false},
		// bool vs bool
		{true, true, 0, false},
		{false, false, 0, false},
		{true, false, 1, false},
		{false, true, -1, false},
		// bool vs time
		{false, time1, 1, false},
		// bool vs Empty
		{true, types.EmptyValueInstance, -1, false},
		// bool vs JSONNull
		{true, types.JSONNullValueInstance, -1, false},
		// bool vs Null
		{true, types.NullValueInstance, -1, false},

		// time vs int
		{time1, int(0), 1, false},
		{time1, int(-2), 1, false},
		// time vs int64
		{time1, int64(0), 1, false},
		{time1, int64(-2), 1, false},
		// time vs float64
		{time1, float64(0), 1, false},
		{time1, float64(-2), 1, false},
		// time vs *big.Rat
		{time1, new(big.Rat).SetInt64(int64(0)), 1, false},
		{time1, new(big.Rat).SetInt64(int64(-2)), 1, false},
		// time vs string
		{time1, hello, -1, false},
		// time vs *string
		{time1, nilStrPtr, -1, false},
		{time1, &hello, -1, false},
		// time vs bool
		{time1, true, -1, false},
		{time1, false, -1, false},
		// time vs time
		{time1, time2, -1, false},
		{time1, time1, 0, false},
		{time2, time1, 1, false},
		// time vs Empty
		{time1, types.EmptyValueInstance, -1, false},
		// time vs JSONNull
		{time1, types.JSONNullValueInstance, -1, false},
		// time vs Null
		{time1, types.NullValueInstance, -1, false},

		// JSONNullValue vs int
		{types.JSONNullValueInstance, int(2), 1, false},
		// JSONNullValue vs int64
		{types.JSONNullValueInstance, int64(2), 1, false},
		// JSONNullValue vs float64
		{types.JSONNullValueInstance, float64(2), 1, false},
		// JSONNullValue vs *big.Rat
		{types.JSONNullValueInstance, new(big.Rat).SetInt64(int64(2)), 1, false},
		// JSONNullValue vs string
		{types.JSONNullValueInstance, hello, 1, false},
		// JSONNullValue vs *string
		{types.JSONNullValueInstance, &hello, 1, false},
		// JSONNullValue vs bool
		{types.JSONNullValueInstance, true, 1, false},
		{types.JSONNullValueInstance, false, 1, false},
		// JSONNullValue vs time
		{types.JSONNullValueInstance, time1, 1, false},
		// JSONNullValue vs EmptyValue
		{types.JSONNullValueInstance, types.EmptyValueInstance, 1, false},
		// JSONNullValue vs NullValue
		{types.JSONNullValueInstance, types.NullValueInstance, -1, false},
		// JSONNullValue vs JSONNullValue
		{types.JSONNullValueInstance, types.JSONNullValueInstance, 0, false},

		// EmptyValue vs int
		{types.EmptyValueInstance, int(2), 1, false},
		// EmptyValue vs int64
		{types.EmptyValueInstance, int64(2), 1, false},
		// EmptyValue vs float64
		{types.EmptyValueInstance, float64(2), 1, false},
		// EmptyValue vs *big.Rat
		{types.EmptyValueInstance, new(big.Rat).SetInt64(int64(2)), 1, false},
		// EmptyValue vs string
		{types.EmptyValueInstance, hello, 1, false},
		// EmptyValue vs *string
		{types.EmptyValueInstance, &hello, 1, false},
		// EmptyValue vs bool
		{types.EmptyValueInstance, true, 1, false},
		{types.EmptyValueInstance, false, 1, false},
		// EmptyValue vs time
		{types.EmptyValueInstance, time1, 1, false},
		// EmptyValue vs JSONNullValue
		{types.EmptyValueInstance, types.JSONNullValueInstance, -1, false},
		// EmptyValue vs NullValue
		{types.EmptyValueInstance, types.NullValueInstance, -1, false},
		// EmptyValue vs EmptyValue
		{types.EmptyValueInstance, types.EmptyValueInstance, 0, false},

		// NullValue vs int
		{types.NullValueInstance, int(2), 1, false},
		// NullValue vs int64
		{types.NullValueInstance, int64(2), 1, false},
		// NullValue vs float64
		{types.NullValueInstance, float64(2), 1, false},
		// NullValue vs *big.Rat
		{types.NullValueInstance, new(big.Rat).SetInt64(int64(2)), 1, false},
		// NullValue vs string
		{types.NullValueInstance, hello, 1, false},
		// NullValue vs *string
		{types.NullValueInstance, &hello, 1, false},
		// NullValue vs bool
		{types.NullValueInstance, true, 1, false},
		{types.NullValueInstance, false, 1, false},
		// NullValue vs time
		{types.NullValueInstance, time1, 1, false},
		// NullValue vs EmptyValue
		{types.NullValueInstance, types.EmptyValueInstance, 1, false},
		// NullValue vs JSONNullValue
		{types.NullValueInstance, types.JSONNullValueInstance, 1, false},
		// NullValue vs NullValue
		{types.NullValueInstance, types.NullValueInstance, 0, false},
	}

	var desc string
	const msgTemplate string = "compareAtomicsTotalOrder(v1=%T(%[1]v), v2=%T(%[2]v)) "
	for _, r := range test01 {
		desc = fmt.Sprintf(msgTemplate, r.v1, r.v2)
		actualRes, err := compareAtomicsTotalOrder(nil, r.v1, r.v2)
		if r.incompatible {
			assert.Errorf(t, err, desc+"should have failed")
			continue
		}

		if assert.NoErrorf(t, err, desc+"got error %v", err) {
			assert.Equalf(t, r.comp, actualRes, desc+"got unexpected result")
		}
	}

}
