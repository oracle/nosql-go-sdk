//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/assert"
)

func TestSortBySpec(t *testing.T) {
	now := time.Now()

	tests := []struct {
		desc       string
		input      []map[string]interface{}
		output     []map[string]interface{}
		sortFields []string
		sortSpecs  []*sortSpec
	}{
		{
			desc: "sort special values in ascending order",
			input: []map[string]interface{}{
				{"c1": types.NullValueInstance, "c2": int(1)},
				{"c1": types.EmptyValueInstance, "c2": int(2)},
				{"c1": types.JSONNullValueInstance, "c2": types.NullValueInstance},
				{"c1": types.JSONNullValueInstance, "c2": int(4)},
			},
			output: []map[string]interface{}{
				{"c1": types.EmptyValueInstance, "c2": int(2)},
				{"c1": types.JSONNullValueInstance, "c2": types.NullValueInstance},
				{"c1": types.JSONNullValueInstance, "c2": int(4)},
				{"c1": types.NullValueInstance, "c2": int(1)},
			},
			sortFields: []string{"c1", "c2"},
			sortSpecs: []*sortSpec{
				{isDesc: false, nullsFirst: true},
				{isDesc: false, nullsFirst: true},
			},
		},
		{
			desc: "sort special values in descending order",
			input: []map[string]interface{}{
				{"c1": types.NullValueInstance, "c2": int(1)},
				{"c1": types.EmptyValueInstance, "c2": int(2)},
				{"c1": types.JSONNullValueInstance, "c2": types.NullValueInstance},
				{"c1": types.JSONNullValueInstance, "c2": int(4)},
				{"c1": types.JSONNullValueInstance, "c2": int(5)},
			},
			output: []map[string]interface{}{
				{"c1": types.NullValueInstance, "c2": int(1)},
				{"c1": types.JSONNullValueInstance, "c2": int(5)},
				{"c1": types.JSONNullValueInstance, "c2": int(4)},
				{"c1": types.JSONNullValueInstance, "c2": types.NullValueInstance},
				{"c1": types.EmptyValueInstance, "c2": int(2)},
			},
			sortFields: []string{"c1", "c2"},
			sortSpecs: []*sortSpec{
				{isDesc: true, nullsFirst: true},
				{isDesc: true, nullsFirst: false},
			},
		},
		{
			desc: "sort special values and strings in ascending order",
			input: []map[string]interface{}{
				{"c1": types.NullValueInstance},
				{"c1": types.EmptyValueInstance},
				{"c1": types.JSONNullValueInstance},
				{"c1": "valueY"},
				{"c1": "valueX"},
				{"c1": ""},
			},
			output: []map[string]interface{}{
				{"c1": types.EmptyValueInstance},
				{"c1": types.JSONNullValueInstance},
				{"c1": types.NullValueInstance},
				{"c1": ""},
				{"c1": "valueX"},
				{"c1": "valueY"},
			},
			sortFields: []string{"c1"},
			sortSpecs: []*sortSpec{
				{isDesc: false, nullsFirst: true},
			},
		},
		{
			desc: "sort special values and integers in descending order",
			input: []map[string]interface{}{
				{"c1": types.NullValueInstance, "c2": int64(1)},
				{"c1": types.EmptyValueInstance, "c2": int64(2)},
				{"c1": types.JSONNullValueInstance, "c2": int64(3)},
				{"c1": 1001, "c2": int64(4)},
				{"c1": 1001, "c2": int64(5)},
				{"c1": 2001, "c2": int64(6)},
			},
			output: []map[string]interface{}{
				{"c1": types.NullValueInstance, "c2": int64(1)},
				{"c1": types.JSONNullValueInstance, "c2": int64(3)},
				{"c1": types.EmptyValueInstance, "c2": int64(2)},
				{"c1": 2001, "c2": int64(6)},
				{"c1": 1001, "c2": int64(5)},
				{"c1": 1001, "c2": int64(4)},
			},
			sortFields: []string{"c1", "c2"},
			sortSpecs: []*sortSpec{
				{isDesc: true, nullsFirst: true},
				{isDesc: true, nullsFirst: false},
			},
		},
		{
			desc: "sort bools and float64 in ascending order",
			input: []map[string]interface{}{
				{"c1": true, "c2": float64(1)},
				{"c1": true, "c2": float64(2)},
				{"c1": false, "c2": float64(3)},
				{"c1": false, "c2": float64(4)},
			},
			output: []map[string]interface{}{
				{"c1": false, "c2": float64(3)},
				{"c1": false, "c2": float64(4)},
				{"c1": true, "c2": float64(1)},
				{"c1": true, "c2": float64(2)},
			},
			sortFields: []string{"c1", "c2"},
			sortSpecs: []*sortSpec{
				{isDesc: false, nullsFirst: false},
				{isDesc: false, nullsFirst: false},
			},
		},
		{
			desc: "sort time.Time and integers in descending/ascending order",
			input: []map[string]interface{}{
				{"c1": now.Add(time.Hour), "c2": 1},
				{"c1": now.Add(2 * time.Hour), "c2": 2},
				{"c1": now.Add(3 * time.Hour), "c2": 3},
				{"c1": now, "c2": 4},
				{"c1": now, "c2": 5},
			},
			output: []map[string]interface{}{
				{"c1": now.Add(3 * time.Hour), "c2": 3},
				{"c1": now.Add(2 * time.Hour), "c2": 2},
				{"c1": now.Add(time.Hour), "c2": 1},
				{"c1": now, "c2": 4},
				{"c1": now, "c2": 5},
			},
			sortFields: []string{"c1", "c2"},
			sortSpecs: []*sortSpec{
				{isDesc: true, nullsFirst: false},
				{isDesc: false, nullsFirst: false},
			},
		},
		{
			desc: "sort integers and *big.Rat in ascending/descending order",
			input: []map[string]interface{}{
				{"c1": 1, "c2": new(big.Rat).SetFloat64(3.11)},
				{"c1": 2, "c2": new(big.Rat).SetFloat64(3.12)},
				{"c1": 2, "c2": new(big.Rat).SetFloat64(3.13)},
				{"c1": 3, "c2": new(big.Rat).SetFloat64(3.14)},
				{"c1": 3, "c2": new(big.Rat).SetFloat64(3.15)},
				{"c1": 4, "c2": new(big.Rat).SetFloat64(3.16)},
			},
			output: []map[string]interface{}{
				{"c1": 1, "c2": new(big.Rat).SetFloat64(3.11)},
				{"c1": 2, "c2": new(big.Rat).SetFloat64(3.13)},
				{"c1": 2, "c2": new(big.Rat).SetFloat64(3.12)},
				{"c1": 3, "c2": new(big.Rat).SetFloat64(3.15)},
				{"c1": 3, "c2": new(big.Rat).SetFloat64(3.14)},
				{"c1": 4, "c2": new(big.Rat).SetFloat64(3.16)},
			},
			sortFields: []string{"c1", "c2"},
			sortSpecs: []*sortSpec{
				{isDesc: false, nullsFirst: false},
				{isDesc: true, nullsFirst: false},
			},
		},
	}

	for _, r := range tests {
		n := len(r.input)
		rows := make([]*types.MapValue, n)
		for i, in := range r.input {
			rows[i] = types.NewMapValue(in)
		}

		results := &resultsBySortSpec{
			results:    rows,
			sortFields: r.sortFields,
			sortSpecs:  r.sortSpecs,
		}

		sort.Sort(results)
		// Verify the rows are in expected order after sorting.
		for j, out := range rows {
			// check the underlying map values.
			assert.Equalf(t, r.output[j], out.Map(), r.desc+" got unexpect result")
		}
	}
}
