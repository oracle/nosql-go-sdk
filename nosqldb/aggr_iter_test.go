//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
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

	test01 := []struct {
		v1, v2       types.FieldValue
		comp         int
		forSort      bool
		incompatible bool
	}{
		// int vs int
		{int(-2), int(0), -1, true, false},
		{int(-2), int(-2), 0, true, false},
		{int(2), int(-2), 1, true, false},
		// int vs int64
		{int(-2), int64(0), -1, true, false},
		{int(-2), int64(-2), 0, true, false},
		{int(2), int64(-2), 1, true, false},
		// int vs float64
		{int(-2), float64(0), -1, true, false},
		{int(-2), float64(-2), 0, true, false},
		{int(2), float64(-2), 1, true, false},
		// int vs *big.Rat
		{int(-2), new(big.Rat).SetInt64(int64(0)), -1, true, false},
		{int(-2), new(big.Rat).SetInt64(int64(-2)), 0, true, false},
		{int(2), new(big.Rat).SetInt64(int64(-2)), 1, true, false},
		// int vs string
		{int(-2), emptyStr, -1, true, false},
		{int(-2), hello, -1, true, false},
		{int(2), world, 0, false, true},
		// int vs *string
		{int(-2), nilStrPtr, -1, true, false},
		{int(-2), &emptyStr, -1, true, false},
		{int(-2), &hello, -1, true, false},
		{int(2), &world, 0, false, true},
		// int vs bool
		{int(-2), true, -1, true, false},
		{int(-2), false, -1, true, false},
		{int(2), false, 0, false, true},
		// int vs time
		{int(2), time1, 0, true, true},
		{int(2), time1, 0, false, true},
		// int vs JSONNull
		{int(2), types.JSONNullValueInstance, 0, true, true},
		{int(2), types.JSONNullValueInstance, 0, false, true},

		// int64 vs int
		{int64(-2), int(0), -1, true, false},
		{int64(-2), int(-2), 0, true, false},
		{int64(2), int(-2), 1, true, false},
		// int64 vs int64
		{int64(-2), int64(0), -1, true, false},
		{int64(-2), int64(-2), 0, true, false},
		{int64(2), int64(-2), 1, true, false},
		// int64 vs float64
		{int64(-2), float64(0), -1, true, false},
		{int64(-2), float64(-2), 0, true, false},
		{int64(2), float64(-2), 1, true, false},
		// int64 vs *big.Rat
		{int64(-2), new(big.Rat).SetInt64(int64(0)), -1, true, false},
		{int64(-2), new(big.Rat).SetInt64(int64(-2)), 0, true, false},
		{int64(2), new(big.Rat).SetInt64(int64(-2)), 1, true, false},
		// int64 vs string
		{int64(-2), emptyStr, -1, true, false},
		{int64(-2), hello, -1, true, false},
		{int64(2), world, 0, false, true},
		// int64 vs *string
		{int64(-2), nilStrPtr, -1, true, false},
		{int64(-2), &emptyStr, -1, true, false},
		{int64(-2), &hello, -1, true, false},
		{int64(2), &world, 0, false, true},
		// int64 vs bool
		{int64(-2), true, -1, true, false},
		{int64(-2), false, -1, true, false},
		{int64(2), false, 0, false, true},
		// int64 vs time
		{int64(2), time1, 0, true, true},
		{int64(2), time1, 0, false, true},
		// int64 vs JSONNull
		{int64(2), types.JSONNullValueInstance, 0, true, true},
		{int64(2), types.JSONNullValueInstance, 0, false, true},

		// float64 vs int
		{float64(-2), int(0), -1, true, false},
		{float64(-2), int(-2), 0, true, false},
		{float64(2), int(-2), 1, true, false},
		// float64 vs int64
		{float64(-2), int64(0), -1, true, false},
		{float64(-2), int64(-2), 0, true, false},
		{float64(2), int64(-2), 1, true, false},
		// float64 vs float64
		{float64(-2), float64(0), -1, true, false},
		{float64(-2), float64(-2), 0, true, false},
		{float64(2), float64(-2), 1, true, false},
		// float64 vs *big.Rat
		{float64(-2), new(big.Rat).SetInt64(int64(0)), -1, true, false},
		{float64(-2), new(big.Rat).SetInt64(int64(-2)), 0, true, false},
		{float64(2), new(big.Rat).SetInt64(int64(-2)), 1, true, false},
		// float64 vs string
		{float64(-2), emptyStr, -1, true, false},
		{float64(-2), hello, -1, true, false},
		{float64(2), world, 0, false, true},
		// float64 vs *string
		{float64(-2), nilStrPtr, -1, true, false},
		{float64(-2), &emptyStr, -1, true, false},
		{float64(-2), &hello, -1, true, false},
		{float64(2), &world, 0, false, true},
		// float64 vs bool
		{float64(-2), true, -1, true, false},
		{float64(-2), false, -1, true, false},
		{float64(2), false, 0, false, true},
		// float64 vs time
		{float64(2), time1, 0, true, true},
		{float64(2), time1, 0, false, true},
		// float64 vs JSONNull
		{float64(2), types.JSONNullValueInstance, 0, true, true},
		{float64(2), types.JSONNullValueInstance, 0, false, true},

		// *big.Rat vs int
		{new(big.Rat).SetInt64(int64(-2)), int(0), -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), int(-2), 0, true, false},
		{new(big.Rat).SetInt64(int64(2)), int(-2), 1, true, false},
		// *big.Rat vs int64
		{new(big.Rat).SetInt64(int64(-2)), int64(0), -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), int64(-2), 0, true, false},
		{new(big.Rat).SetInt64(int64(2)), int64(-2), 1, true, false},
		// *big.Rat vs float64
		{new(big.Rat).SetInt64(int64(-2)), float64(0), -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), float64(-2), 0, true, false},
		{new(big.Rat).SetInt64(int64(2)), float64(-2), 1, true, false},
		// *big.Rat vs *big.Rat
		{new(big.Rat).SetInt64(int64(-2)), new(big.Rat).SetInt64(int64(0)), -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), new(big.Rat).SetInt64(int64(-2)), 0, true, false},
		{new(big.Rat).SetInt64(int64(2)), new(big.Rat).SetInt64(int64(-2)), 1, true, false},
		// *big.Rat vs string
		{new(big.Rat).SetInt64(int64(-2)), emptyStr, -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), hello, -1, true, false},
		{new(big.Rat).SetInt64(int64(2)), world, 0, false, true},
		// *big.Rat vs *string
		{new(big.Rat).SetInt64(int64(-2)), nilStrPtr, -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), &emptyStr, -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), &hello, -1, true, false},
		{new(big.Rat).SetInt64(int64(2)), &world, 0, false, true},
		// *big.Rat vs bool
		{new(big.Rat).SetInt64(int64(-2)), true, -1, true, false},
		{new(big.Rat).SetInt64(int64(-2)), false, -1, true, false},
		{new(big.Rat).SetInt64(int64(2)), false, 0, false, true},
		// *big.Rat vs time
		{new(big.Rat).SetInt64(int64(2)), time1, 0, true, true},
		{new(big.Rat).SetInt64(int64(2)), time1, 0, false, true},
		// *big.Rat vs JSONNull
		{new(big.Rat).SetInt64(int64(2)), types.JSONNullValueInstance, 0, true, true},
		{new(big.Rat).SetInt64(int64(2)), types.JSONNullValueInstance, 0, false, true},

		// string vs int
		{emptyStr, int(0), 1, true, false},
		{hello, int(-2), 1, true, false},
		{world, int(-2), 0, false, true},
		// string vs int64
		{emptyStr, int64(0), 1, true, false},
		{hello, int64(-2), 1, true, false},
		{world, int64(-2), 0, false, true},
		// string vs float64
		{emptyStr, float64(0), 1, true, false},
		{hello, float64(-2), 1, true, false},
		{world, float64(-2), 0, false, true},
		// string vs *big.Rat
		{emptyStr, new(big.Rat).SetInt64(int64(0)), 1, true, false},
		{hello, new(big.Rat).SetInt64(int64(-2)), 1, true, false},
		{world, new(big.Rat).SetInt64(int64(-2)), 0, false, true},
		// string vs string
		{emptyStr, hello, -1, true, false},
		{hello, hello, 0, true, false},
		{world, hello, 1, true, false},
		// string vs *string
		{emptyStr, nilStrPtr, 1, true, false},
		{emptyStr, &hello, -1, true, false},
		{hello, &hello, 0, true, false},
		{world, &hello, 1, true, false},
		// string vs bool
		{emptyStr, true, -1, true, false},
		{hello, false, -1, true, false},
		{world, false, 0, false, true},
		// string vs time
		{hello, time1, 0, true, true},
		{world, time1, 0, false, true},
		// string vs JSONNull
		{hello, types.JSONNullValueInstance, 0, true, true},
		{world, types.JSONNullValueInstance, 0, false, true},

		// bool vs int
		{true, int(0), 1, true, false},
		{false, int(-2), 1, true, false},
		{false, int(-2), 0, false, true},
		// bool vs int64
		{true, int64(0), 1, true, false},
		{false, int64(-2), 1, true, false},
		{false, int64(-2), 0, false, true},
		// bool vs float64
		{true, float64(0), 1, true, false},
		{false, float64(-2), 1, true, false},
		{false, float64(-2), 0, false, true},
		// bool vs *big.Rat
		{true, new(big.Rat).SetInt64(int64(0)), 1, true, false},
		{false, new(big.Rat).SetInt64(int64(-2)), 1, true, false},
		{false, new(big.Rat).SetInt64(int64(-2)), 0, false, true},
		// bool vs string
		{true, hello, 1, true, false},
		{false, hello, 1, true, false},
		{false, hello, 0, false, true},
		// bool vs *string
		{true, nilStrPtr, 1, true, false},
		{false, &hello, 1, true, false},
		{false, &hello, 0, false, true},
		// bool vs bool
		{true, true, 0, true, false},
		{false, false, 0, true, false},
		{true, false, 1, true, false},
		{false, true, -1, true, false},
		// bool vs time
		{false, time1, 0, true, true},
		{true, time1, 0, false, true},
		// bool vs JSONNull
		{true, types.JSONNullValueInstance, 0, true, true},
		{false, types.JSONNullValueInstance, 0, false, true},

		// time vs int
		{time1, int(0), 0, true, true},
		{time1, int(-2), 0, true, true},
		{time1, int(-2), 0, false, true},
		// time vs int64
		{time1, int64(0), 0, true, true},
		{time1, int64(-2), 0, true, true},
		{time1, int64(-2), 0, false, true},
		// time vs float64
		{time1, float64(0), 0, true, true},
		{time1, float64(-2), 0, true, true},
		{time1, float64(-2), 0, false, true},
		// time vs *big.Rat
		{time1, new(big.Rat).SetInt64(int64(0)), 0, true, true},
		{time1, new(big.Rat).SetInt64(int64(-2)), 0, true, true},
		{time1, new(big.Rat).SetInt64(int64(-2)), 0, false, true},
		// time vs string
		{time1, hello, 0, true, true},
		{time1, hello, 0, true, true},
		{time1, hello, 0, false, true},
		// time vs *string
		{time1, nilStrPtr, 0, true, true},
		{time1, &hello, 0, true, true},
		{time1, &hello, 0, false, true},
		// time vs bool
		{time1, true, 0, true, true},
		{time1, false, 0, true, true},
		{time1, false, 0, false, true},
		{time1, true, 0, false, true},
		// time vs time
		{time1, time2, -1, true, false},
		{time1, time1, 0, true, false},
		{time2, time1, 1, true, false},
		// time vs JSONNull
		{time1, types.JSONNullValueInstance, 0, true, true},
		{time2, types.JSONNullValueInstance, 0, false, true},

		// JSONNullValue vs int
		{types.JSONNullValueInstance, int(2), 0, true, true},
		// JSONNullValue vs int64
		{types.JSONNullValueInstance, int64(2), 0, true, true},
		// JSONNullValue vs float64
		{types.JSONNullValueInstance, float64(2), 0, true, true},
		// JSONNullValue vs *big.Rat
		{types.JSONNullValueInstance, new(big.Rat).SetInt64(int64(2)), 0, true, true},
		// JSONNullValue vs string
		{types.JSONNullValueInstance, hello, 0, true, true},
		// JSONNullValue vs *string
		{types.JSONNullValueInstance, &hello, 0, true, true},
		// JSONNullValue vs bool
		{types.JSONNullValueInstance, true, 0, true, true},
		{types.JSONNullValueInstance, false, 0, true, true},
		// JSONNullValue vs time
		{types.JSONNullValueInstance, time1, 0, true, true},
		// JSONNullValue vs JSONNullValue
		{types.JSONNullValueInstance, types.JSONNullValueInstance, 0, true, false},
	}

	var desc string
	const msgTemplate string = "compareAtomicValues(forSort=%t, v1=%T(%[2]v), v2=%T(%[3]v)) "
	for _, r := range test01 {
		desc = fmt.Sprintf(msgTemplate, r.forSort, r.v1, r.v2)
		actualRes, err := compareAtomicValues(nil, r.forSort, r.v1, r.v2)
		if assert.NoErrorf(t, err, desc+"got error %v", err) {
			wantRes := &compareResult{
				comp:         r.comp,
				incompatible: r.incompatible,
			}
			assert.Equalf(t, wantRes, actualRes, desc+"got unexpected result")
		}
	}

	// Test NullValue with other values.
	test02 := []struct {
		v1, v2       types.FieldValue
		comp         int
		forSort      bool
		incompatible bool
		hasNull      bool
	}{
		{types.NullValueInstance, int(2), 0, true, false, true},
		{types.NullValueInstance, int64(2), 0, true, false, true},
		{float64(2), types.NullValueInstance, 0, true, false, true},
		{types.NullValueInstance, types.NullValueInstance, 0, true, false, true},
	}

	for _, r := range test02 {
		desc = fmt.Sprintf(msgTemplate, r.forSort, r.v1, r.v2)
		actualRes, err := compareAtomicValues(nil, r.forSort, r.v1, r.v2)
		if assert.NoErrorf(t, err, desc+"got error %v", err) {
			wantRes := &compareResult{
				comp:         r.comp,
				incompatible: r.incompatible,
				hasNull:      r.hasNull,
			}
			assert.Equalf(t, wantRes, actualRes, desc+"got unexpected result")
		}
	}
}
