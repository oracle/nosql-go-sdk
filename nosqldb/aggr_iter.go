//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

var (
	_ planIterState = (*aggrIterState)(nil)
	_ aggrPlanIter  = (*funcSumIter)(nil)
	_ aggrPlanIter  = (*funcMinMaxIter)(nil)
)

// aggrIterState represents the state for an aggregate plan iterator.
type aggrIterState struct {
	*iterState

	// The sum value.
	// This is used by the sum(*) and avg(*) functions.
	sum interface{}

	// The number of input values for the aggregate operation.
	// This is used by the avg(*) function.
	count int

	// The min or max value.
	// This is used by the min(*) and max(*) functions.
	minMax interface{}

	// nullInputOnly indicates if input values supplied to the aggregate
	// operation are all NULLs.
	nullInputOnly bool
}

func newAggrIterState() *aggrIterState {
	return &aggrIterState{
		iterState:     newIterState(),
		sum:           int64(0),
		count:         0,
		minMax:        types.NullValueInstance,
		nullInputOnly: true,
	}
}

func (st *aggrIterState) reset() error {
	st.sum = int64(0)
	st.count = 0
	st.minMax = types.NullValueInstance
	st.nullInputOnly = true
	return st.iterState.reset()
}

// funcSumIter implements the built-in SQL sum(*) aggregate function.
// It is used to re-sum partial sums and counts received from the Oracle NoSQL database servers.
type funcSumIter struct {
	*planIterDelegate

	// The input plan iterator.
	input planIter
}

func newFuncSumIter(r proto.Reader) (iter *funcSumIter, err error) {
	delegate, err := newPlanIterDelegate(r, sumFunc)
	if err != nil {
		return
	}

	input, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	iter = &funcSumIter{
		planIterDelegate: delegate,
		input:            input,
	}
	return
}

func (iter *funcSumIter) open(rcb *runtimeControlBlock) (err error) {
	rcb.setState(iter.statePos, newAggrIterState())
	return iter.input.open(rcb)
}

// reset resets the input iterator so that the next input value can be computed.
//
// This method does not reset the state for funcSumIter, it is the getAggrValue()
// method that resets the state.
func (iter *funcSumIter) reset(rcb *runtimeControlBlock) (err error) {
	return iter.input.reset(rcb)
}

func (iter *funcSumIter) close(rcb *runtimeControlBlock) (err error) {
	state := rcb.getState(iter.statePos)
	if state == nil {
		return nil
	}

	if err = iter.input.close(rcb); err != nil {
		return
	}

	return state.close()
}

// next does not actually return a value, it just adds a new value
// (if it is of a numeric type) to the current sum kept in the state.
func (iter *funcSumIter) next(rcb *runtimeControlBlock) (more bool, err error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*aggrIterState)
	if !ok {
		return false, fmt.Errorf("wrong iterator state type for funcSumIter, "+
			"expect *aggrIterState, got %T", st)
	}

	if state.isDone() {
		return false, nil
	}

	var value types.FieldValue

	for {
		more, err = iter.input.next(rcb)
		if err != nil {
			return false, err
		}

		if !more {
			return true, nil
		}

		value = iter.input.getResult(rcb)
		rcb.trace(2, "funcSumIter.next() : summing up value %v", value)

		if value == types.NullValueInstance {
			continue
		}

		state.nullInputOnly = false
		err = iter.computeSum(state, value)
		if err != nil {
			return false, err
		}
	}
}

// computeSum computes the new sum by adding the specified value.
func (iter *funcSumIter) computeSum(state *aggrIterState, value types.FieldValue) (err error) {
	switch v := value.(type) {
	case int:
		err = iter.addInt(state, v)
	case int64:
		err = iter.addInt64(state, v)
	case float64:
		err = iter.addFloat64(state, v)
	case *big.Rat:
		err = iter.addBigRat(state, v)
	default:
		return fmt.Errorf("unsupported type of input value for the sum(*) function: %T", value)
	}

	if err != nil {
		return
	}

	state.count++
	return
}

// addInt computes the new sum by adding the specified int value v.
func (iter *funcSumIter) addInt(state *aggrIterState, v int) error {
	switch sum := state.sum.(type) {
	case int:
		sum += v
		state.sum = sum

	case int64:
		sum += int64(v)
		state.sum = sum

	case float64:
		sum += float64(v)
		state.sum = sum

	case *big.Rat:
		newVal := new(big.Rat).SetInt64(int64(v))
		sum.Add(sum, newVal)
		state.sum = sum

	default:
		return fmt.Errorf("unsupported sum type for the sum(*) function: %T", state.sum)
	}

	return nil
}

// addInt64 computes the new sum by adding the specified int64 value v.
func (iter *funcSumIter) addInt64(state *aggrIterState, v int64) error {
	switch sum := state.sum.(type) {
	case int:
		state.sum = int64(sum) + v

	case int64:
		state.sum = sum + v

	case float64:
		sum += float64(v)
		state.sum = sum

	case *big.Rat:
		newVal := new(big.Rat).SetInt64(v)
		sum.Add(sum, newVal)
		state.sum = sum

	default:
		return fmt.Errorf("unsupported sum type for the sum(*) function: %T", state.sum)
	}

	return nil
}

// addFloat64 computes the new sum by adding the specified float64 value v.
func (iter *funcSumIter) addFloat64(state *aggrIterState, v float64) error {
	switch sum := state.sum.(type) {
	case int:
		state.sum = float64(sum) + v

	case int64:
		state.sum = float64(sum) + v

	case float64:
		sum += v
		state.sum = sum

	case *big.Rat:
		newVal := new(big.Rat).SetFloat64(v)
		sum.Add(sum, newVal)
		state.sum = sum

	default:
		return fmt.Errorf("unsupported sum type for the sum(*) function: %T", state.sum)
	}

	return nil
}

// addBigRat computes the new sum by adding the specified big.Rat value v.
func (iter *funcSumIter) addBigRat(state *aggrIterState, v *big.Rat) error {
	switch sum := state.sum.(type) {
	case int:
		newSum := new(big.Rat).SetInt64(int64(sum))
		newSum.Add(newSum, v)
		state.sum = newSum

	case int64:
		newSum := new(big.Rat).SetInt64(sum)
		newSum.Add(newSum, v)
		state.sum = newSum

	case float64:
		newSum := new(big.Rat).SetFloat64(sum)
		newSum.Add(newSum, v)
		state.sum = newSum

	case *big.Rat:
		sum.Add(sum, v)
		state.sum = sum

	default:
		return fmt.Errorf("unsupported sum type for the sum(*) function: %T", state.sum)
	}

	return nil
}

// getAggrValue returns the final result of the aggregate operation, it also
// resets the iterator state if the reset flag is set.
//
// This implements the aggrPlanIter interface.
func (iter *funcSumIter) getAggrValue(rcb *runtimeControlBlock, reset bool) (v types.FieldValue, err error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*aggrIterState)
	if !ok {
		return nil, fmt.Errorf("wrong iterator state type for funcSumIter, "+
			"expect *aggrIterState, got %T", st)
	}

	if state.nullInputOnly {
		return types.NullValueInstance, nil
	}

	sum := state.sum
	if reset {
		state.reset()
	}

	rcb.trace(4, "funcSumIter.getAggrValue() : got sum=%v", state.sum)
	return sum, nil
}

func (iter *funcSumIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *funcSumIter) displayContent(sb *strings.Builder, f *planFormatter) {
	iter.planIterDelegate.displayPlan(iter.input, sb, f)
}

// funcMinMaxIter implements the built-in SQL min() and max() aggregate functions.
//
// It is required by the driver to compute the total min/max from the partial
// mins/maxs received from the NoSQL database servers.
type funcMinMaxIter struct {
	*planIterDelegate

	// The input plan iterator.
	input planIter

	// fnCode indicates if this is min() or max() function.
	fnCode funcCode
}

func newFuncMinMaxIter(r proto.Reader) (iter *funcMinMaxIter, err error) {
	delegate, err := newPlanIterDelegate(r, minMaxFunc)
	if err != nil {
		return
	}

	fnCode, err := r.ReadInt16()
	if err != nil {
		return
	}

	input, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	iter = &funcMinMaxIter{
		planIterDelegate: delegate,
		input:            input,
		fnCode:           funcCode(int(fnCode)),
	}
	return
}

// getFuncCode returns the function code.
//
// This implements the funcPlanIter interface.
func (iter *funcMinMaxIter) getFuncCode() funcCode {
	return iter.fnCode
}

func (iter *funcMinMaxIter) open(rcb *runtimeControlBlock) (err error) {
	rcb.setState(iter.statePos, newAggrIterState())
	return iter.input.open(rcb)
}

func (iter *funcMinMaxIter) reset(rcb *runtimeControlBlock) (err error) {
	return iter.input.reset(rcb)
}

func (iter *funcMinMaxIter) close(rcb *runtimeControlBlock) (err error) {
	state := rcb.getState(iter.statePos)
	if state == nil {
		return nil
	}

	if err = iter.input.close(rcb); err != nil {
		return
	}

	return state.close()
}

func (iter *funcMinMaxIter) next(rcb *runtimeControlBlock) (more bool, err error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*aggrIterState)
	if !ok {
		return false, fmt.Errorf("wrong iterator state type for funcMinMaxIter, "+
			"expect *aggrIterState, got %T", st)
	}

	if state.isDone() {
		return false, nil
	}

	var value types.FieldValue

	for {
		more, err = iter.input.next(rcb)
		if err != nil {
			return false, err
		}

		if !more {
			return true, nil
		}

		value = iter.input.getResult(rcb)

		err = iter.computeMinMax(rcb, state, value)
		if err != nil {
			return false, err
		}
	}
}

// computeMinMax computes the min or max value.
func (iter *funcMinMaxIter) computeMinMax(rcb *runtimeControlBlock, state *aggrIterState, value types.FieldValue) (err error) {

	// Do not compare values of BINARY, MAP, ARRAY, EMPTY, NULL and JSON_NULL.
	switch value.(type) {
	case []byte, *types.MapValue, map[string]interface{}, []types.FieldValue, []interface{},
		*types.EmptyValue, *types.NullValue, *types.JSONNullValue:
		return nil
	}

	if state.minMax == types.NullValueInstance {
		state.minMax = value
		return nil
	}

	cmp, err := compareAtomicValues(rcb, true, value, state.minMax)
	if err != nil {
		return
	}

	rcb.trace(3, "funcMinMaxIter.computeMinMax() : compared values %v and %v, result=%v",
		value, state.minMax, cmp)

	if (iter.fnCode == fnMin && cmp < 0) || (iter.fnCode == fnMax && cmp > 0) {
		rcb.trace(2, "funcMinMaxIter.computeMinMax(): setting min/max to %v", value)
		state.minMax = value
	}

	return
}

// getAggrValue returns the final result of the aggregate operation, it also
// resets the iterator state if the reset flag is set.
//
// This implements the aggrPlanIter interface.
func (iter *funcMinMaxIter) getAggrValue(rcb *runtimeControlBlock, reset bool) (v types.FieldValue, err error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*aggrIterState)
	if !ok {
		return nil, fmt.Errorf("wrong iterator state type for funcMinMaxIter, "+
			"expect *aggrIterState, got %T", st)
	}

	minMax := state.minMax
	if reset {
		state.reset()
	}

	return minMax, nil
}

func (iter *funcMinMaxIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *funcMinMaxIter) displayContent(sb *strings.Builder, f *planFormatter) {
	iter.displayPlan(iter.input, sb, f)
}

// compareAtomicValues compares two atomic values and returns the result of comparison.
// This function returns an error if either of the two values is non-atomic or
// the values are not comparable. Otherwise, the returned res is:
//
//   0 if v1 == v2
//   1 if v1 > v2
//   -1 if v1 < v2
//
// Whether the two values are comparable depends on the "forSort" parameter.
// If true, then values that would otherwise be considered non-comparable are
// assumed to have the following order:
//
//   NUMERICS < TIMESTAMP < STRING < BOOLEAN < EMPTY < JSON_NULL < NULL
//
func compareAtomicValues(rcb *runtimeControlBlock, forSort bool, v1, v2 types.FieldValue) (res int, err error) {
	if rcb != nil {
		rcb.trace(4, "compareAtomicValues() : comparing values %v and %v", v1, v2)
	}

	switch v1 := v1.(type) {
	case *types.NullValue:
		if forSort {
			switch v2.(type) {
			case *types.NullValue:
				res = 0
			default:
				res = 1
			}
			return
		}

	case *types.JSONNullValue:
		if _, ok := v2.(*types.JSONNullValue); ok {
			res = 0
			return
		}

		if forSort {
			switch v2.(type) {
			case *types.NullValue:
				res = -1
			default:
				res = 1
			}
			return
		}

	case *types.EmptyValue:
		if _, ok := v2.(*types.EmptyValue); ok {
			res = 0
			return
		}

		if forSort {
			switch v2.(type) {
			case *types.JSONNullValue, *types.NullValue:
				res = -1
			default:
				res = 1
			}
			return
		}

	case int:
		switch v2 := v2.(type) {
		case int:
			res = compareInts(v1, v2)
			return
		case int64:
			res = compareInt64s(int64(v1), v2)
			return
		case float64:
			res = compareFloat64s(float64(v1), v2)
			return
		case *big.Rat:
			rat1 := new(big.Rat).SetInt64(int64(v1))
			res = rat1.Cmp(v2)
			return
		case time.Time, string, *string, bool,
			*types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			// INTEGER value is less than TIMESTAMP/STRING/BOOLEAN/EMPTY/JSON_NULL/NULL
			if forSort {
				res = -1
				return
			}
		}

	case int64:
		switch v2 := v2.(type) {
		case int:
			res = compareInt64s(v1, int64(v2))
			return
		case int64:
			res = compareInt64s(v1, v2)
			return
		case float64:
			res = compareFloat64s(float64(v1), v2)
			return
		case *big.Rat:
			rat1 := new(big.Rat).SetInt64(v1)
			res = rat1.Cmp(v2)
			return
		case time.Time, string, *string, bool,
			*types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			// LONG value is less than TIMESTAMP/STRING/BOOLEAN/EMPTY/JSON_NULL/NULL
			if forSort {
				res = -1
				return
			}
		}

	case float64:
		switch v2 := v2.(type) {
		case int:
			res = compareFloat64s(float64(v1), float64(v2))
			return
		case int64:
			res = compareFloat64s(float64(v1), float64(v2))
			return
		case float64:
			res = compareFloat64s(float64(v1), v2)
			return
		case *big.Rat:
			rat1 := new(big.Rat).SetFloat64(v1)
			res = rat1.Cmp(v2)
			return
		case time.Time, string, *string, bool,
			*types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			// DOUBLE value is less than TIMESTAMP/STRING/BOOLEAN/EMPTY/JSON_NULL/NULL
			if forSort {
				res = -1
				return
			}
		}

	case *big.Rat:
		rat2 := new(big.Rat)
		switch v2 := v2.(type) {
		case int:
			rat2.SetInt64(int64(v2))
			res = v1.Cmp(rat2)
			return
		case int64:
			rat2.SetInt64(v2)
			res = v1.Cmp(rat2)
			return
		case float64:
			rat2.SetFloat64(v2)
			res = v1.Cmp(rat2)
			return
		case *big.Rat:
			res = v1.Cmp(v2)
			return
		case time.Time, string, *string, bool,
			*types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			// NUMBER value is less than TIMESTAMP/STRING/BOOLEAN/EMPTY/JSON_NULL/NULL
			if forSort {
				res = -1
				return
			}
		}

	case time.Time:
		switch v2 := v2.(type) {
		case time.Time:
			switch {
			case v1.Equal(v2):
				res = 0
			case v1.Before(v2):
				res = -1
			default:
				res = 1
			}
			return

		case int, int64, float64, *big.Rat:
			if forSort {
				res = 1
				return
			}

		case string, *string, bool,
			*types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			for forSort {
				res = -1
				return
			}
		}

	case *string, string:
		str1, ok1 := stringValue(v1)
		switch v2 := v2.(type) {
		case *string, string:
			str2, ok2 := stringValue(v2)
			switch {
			case ok1 && ok2:
				res = compareStrings(str1, str2)
			case !ok1 && ok2:
				// v1 is a nil string pointer
				res = -1
			case ok1 && !ok2:
				// v2 is a nil string pointer
				res = 1
			default:
				res = 0
			}
			return
		case int, int64, float64, *big.Rat, time.Time:
			// STRING value is greater than INTEGER/LONG/DOUBLE/NUMBER/TIMESTAMP
			if forSort {
				res = 1
				return
			}
		case bool, *types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			// STRING value is less than BOOLEAN/EMPTY/JSON_NULL/NULL
			if forSort {
				res = -1
				return
			}
		}

	case bool:
		switch v2 := v2.(type) {
		case bool:
			res = compareBools(v1, v2)
			return
		case int, int64, float64, *big.Rat, time.Time, string, *string:
			if forSort {
				res = 1
				return
			}
		case *types.EmptyValue, *types.JSONNullValue, *types.NullValue:
			if forSort {
				res = -1
				return
			}
		}
	}

	err = nosqlerr.NewIllegalState("cannot compare value of type %T with value of type %T", v1, v2)
	return
}

// stringValue returns the string that v represents or v points to when v is a
// pointer to string. If v is a string or a non-nil string pointer, the returned
// ok flag is true, otherwise it is false.
func stringValue(v interface{}) (value string, ok bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case *string:
		if s != nil {
			return *s, true
		}
	}
	return "", false
}

func compareInts(x, y int) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func compareInt64s(x, y int64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func compareFloat64s(x, y float64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func compareStrings(x, y string) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func compareBools(x, y bool) int {
	switch {
	case x == y:
		return 0
	case x == false:
		return -1
	default:
		return 1
	}
}
