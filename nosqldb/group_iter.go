//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"crypto/md5"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// groupTuple holds values for the columns listed in group-by clause.
type groupTuple struct {
	values []interface{}
}

func newGroupTuple(numGBColumns int) *groupTuple {
	return &groupTuple{
		values: make([]interface{}, numGBColumns),
	}
}

// checksum returns the MD5 checksum for the group tuple.
// The values in the group tuple are serialized as bytes into a buffer,
// in which the data are then used to calculate the MD5 checksum.
func (t *groupTuple) checksum() (chksumValue chksum, err error) {
	w := binary.NewWriter()
	for _, v := range t.values {
		switch v := v.(type) {
		case *types.NullValue:
			w.WriteByte(byte(types.Null))
		case *types.JSONNullValue:
			w.WriteByte(byte(types.JSONNull))
		case *types.EmptyValue:
			w.WriteByte(byte(types.Empty))
		case int:
			w.WriteByte(byte(types.Integer))
			w.WritePackedInt(v)
		case int64:
			w.WriteByte(byte(types.Long))
			w.WritePackedLong(v)
		case float64:
			w.WriteByte(byte(types.Double))
			w.WriteDouble(v)
		case *big.Rat:
			w.WriteByte(byte(types.Number))
			strNum := v.RatString()
			w.WriteString(&strNum)
		case string:
			w.WriteByte(byte(types.String))
			w.WriteString(&v)
		case time.Time:
			w.WriteByte(byte(types.Timestamp))
			s := v.UTC().Format(time.RFC3339Nano)
			w.WriteString(&s)
		case bool:
			w.WriteByte(byte(types.Boolean))
			w.WriteBoolean(v)
		case *types.MapValue:
			w.WriteByte(byte(types.Map))
			w.WriteMap(v)
		case map[string]interface{}:
			w.WriteByte(byte(types.Map))
			w.WriteMap(types.NewMapValue(v))
		case []types.FieldValue:
			w.WriteByte(byte(types.Array))
			w.WriteArray(v)
		case []interface{}:
			w.WriteByte(byte(types.Array))
			array := make([]types.FieldValue, len(v))
			for i, value := range v {
				array[i] = value
			}
			w.WriteArray(array)
		case []byte:
			w.WriteByte(byte(types.Binary))
			w.WriteByteArray(v)
		default:
			err = fmt.Errorf("unsupported value %v of type %[1]T", v)
			return
		}
	}

	data := w.Bytes()
	checksum := md5.Sum(data)
	return checksum, nil
}

// aggrValue represents the result of aggregate operation.
type aggrValue struct {
	value           interface{}
	gotNumericInput bool
}

// newAggrValue creates an aggrValue depending on the aggregate operation kind.
func newAggrValue(kind funcCode) (v *aggrValue, err error) {
	v = &aggrValue{}

	switch kind {
	case fnCount, fnCountNumbers, fnCountStar, fnSum:
		v.value = int64(0)
		return
	case fnMax, fnMin:
		v.value = types.NullValueInstance
		return
	default:
		return nil, nosqlerr.NewIllegalArgument("unsupported aggregate operation kind %v", kind)
	}
}

// add updates the aggregate result by adding the specified value.
func (aggr *aggrValue) add(rcb *runtimeControlBlock, countMemory bool, val interface{}, fpSpec *FPArithSpec) (err error) {
	var sz int64
	var t reflect.Type

	switch val.(type) {
	case int, int64, float64, *big.Rat:
		if countMemory {
			sz = int64(sizeOf(aggr.value))
			t = reflect.TypeOf(aggr.value)
		}
		aggr.gotNumericInput = true
		switch v := val.(type) {
		case int:
			aggr.value, err = calcInt('+', aggr.value, v)
		case int64:
			aggr.value, err = calcInt64('+', aggr.value, v)
		case float64:
			aggr.value, err = calcFloat64('+', aggr.value, v)
		case *big.Rat:
			aggr.value, err = calcBigRat('+', aggr.value, v, fpSpec)
		}

		if err != nil {
			return
		}

		// The old and new aggr.value are of different types.
		if countMemory && t != reflect.TypeOf(aggr.value) {
			sz = int64(sizeOf(aggr.value)) - sz
			err = rcb.incMemoryConsumption(sz)
			if err != nil {
				return
			}
		}
	}

	return
}

// groupResult represents a group-by result.
type groupResult struct {
	gbTuple   *groupTuple
	aggrTuple []*aggrValue
}

// String returns a string representation of the group result.
// This implements the fmt.Stringer interface.
func (g groupResult) String() string {
	buf := &strings.Builder{}
	buf.WriteByte('[')
	if g.gbTuple != nil {
		for i, v := range g.gbTuple.values {
			if i > 0 {
				buf.WriteByte(' ')
			}
			fmt.Fprintf(buf, "%v", v)
		}
	}

	if len(g.aggrTuple) > 0 {
		buf.WriteString(" -")
		for _, v := range g.aggrTuple {
			fmt.Fprintf(buf, " %v", v)
		}
	}

	buf.WriteByte(']')
	return buf.String()
}

type groupIterState struct {
	*iterState
	results      map[chksum]*groupResult
	gbTuple      *groupTuple
	hasMoreInput bool
}

func newGroupIterState(iter *groupIter) *groupIterState {
	return &groupIterState{
		iterState:    newIterState(),
		results:      make(map[chksum]*groupResult),
		gbTuple:      newGroupTuple(iter.numGBColumns),
		hasMoreInput: true,
	}
}

func (st *groupIterState) done() (err error) {
	err = st.iterState.done()
	if err != nil {
		return
	}

	return st.clear(false)
}

func (st *groupIterState) reset() (err error) {
	err = st.iterState.reset()
	if err != nil {
		return
	}

	return st.clear(true)
}

func (st *groupIterState) close() (err error) {
	err = st.iterState.close()
	if err != nil {
		return
	}

	return st.clear(false)
}

func (st *groupIterState) clear(reset bool) (err error) {
	for k := range st.results {
		delete(st.results, k)
	}

	if reset {
		st.results = make(map[chksum]*groupResult)
	} else {
		st.results = nil
		st.gbTuple = nil
	}

	st.hasMoreInput = true
	return nil
}

type groupIter struct {
	*planIterDelegate

	// the input plan iterator.
	input planIter

	// number of group by columns
	numGBColumns int

	// column names
	columnNames []string

	// aggregation function codes
	aggrFuncs []funcCode

	// if this is a SELECT DISTINCT query
	isDistinct bool

	// if cached result should be removed
	//
	// This is sent by server, intended to avoid the potential expensive cost of
	// removing an entry from a map when iterate over cached results on client.
	// Go client does not use this flag, the next() method always deletes cached
	// results.
	removeProducedResult bool

	// if memory consumed for caching group-by results should be counted
	countMemory bool
}

func newGroupIter(r proto.Reader) (iter *groupIter, err error) {
	delegate, err := newPlanIterDelegate(r, group)
	if err != nil {
		return
	}

	input, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	numGBColumns, err := r.ReadInt()
	if err != nil {
		return
	}

	columnNames, err := readStringArray(r)
	if err != nil {
		return
	}

	numAggrs := len(columnNames) - numGBColumns
	aggrFuncs := make([]funcCode, numAggrs)

	var fc int16
	for i := 0; i < numAggrs; i++ {
		fc, err = r.ReadInt16()
		if err != nil {
			return
		}
		aggrFuncs[i] = funcCode(fc)
	}

	isDistinct, err := r.ReadBoolean()
	if err != nil {
		return
	}

	removeProducedResult, err := r.ReadBoolean()
	if err != nil {
		return
	}

	countMemory, err := r.ReadBoolean()
	if err != nil {
		return
	}

	iter = &groupIter{
		planIterDelegate:     delegate,
		input:                input,
		numGBColumns:         numGBColumns,
		columnNames:          columnNames,
		aggrFuncs:            aggrFuncs,
		isDistinct:           isDistinct,
		removeProducedResult: removeProducedResult,
		countMemory:          countMemory,
	}
	return
}

func (iter *groupIter) getIterState(rcb *runtimeControlBlock) (*groupIterState, error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*groupIterState)
	if ok {
		return state, nil
	}
	return nil, fmt.Errorf("wrong iterator state type, expect *groupIterState, got %T", st)
}

func (iter *groupIter) open(rcb *runtimeControlBlock) (err error) {
	state := newGroupIterState(iter)
	rcb.setState(iter.statePos, state)
	return iter.input.open(rcb)
}

func (iter *groupIter) reset(rcb *runtimeControlBlock) (err error) {
	state, err := iter.getIterState(rcb)
	if err != nil {
		return
	}

	err = state.reset()
	if err != nil {
		return
	}

	err = iter.input.reset(rcb)
	return
}

func (iter *groupIter) close(rcb *runtimeControlBlock) (err error) {
	state, err := iter.getIterState(rcb)
	if err != nil {
		return
	}

	err = iter.input.close(rcb)
	if err != nil {
		return
	}

	err = state.close()
	return
}

func (iter *groupIter) next(rcb *runtimeControlBlock) (more bool, err error) {
	state, err := iter.getIterState(rcb)
	if err != nil {
		return false, err
	}

	if state.isDone() {
		return false, nil
	}

	for {

		if !state.hasMoreInput {
			for k, v := range state.results {
				res := &types.MapValue{}
				for i, name := range iter.columnNames {
					if i < iter.numGBColumns {
						res.Put(name, v.gbTuple.values[i])
					} else {
						aggrVal := iter.getAggrValue(rcb, v.aggrTuple, i)
						res.Put(name, aggrVal)
					}
				}

				rcb.setRegValue(iter.resultReg, res)
				delete(state.results, k)
				return true, nil
			}

			err = state.done()
			return false, err
		}

		more, err = iter.input.next(rcb)
		if err != nil {
			return false, err
		}

		if !more {
			if rcb.reachedLimit {
				return false, nil
			}

			// There is no aggregate functions in SELECT list.
			if iter.numGBColumns == len(iter.columnNames) {
				err = state.done()
				return false, err
			}

			state.hasMoreInput = false
			continue
		}

		tempRes := iter.input.getResult(rcb)
		inTuple, ok := tempRes.(*types.MapValue)
		if !ok {
			return false, fmt.Errorf("wrong result type, expect *MapValue, got %T", tempRes)
		}

		i := 0
		for i < iter.numGBColumns {
			colValue, ok := inTuple.Get(iter.columnNames[i])
			if !ok {
				break
			}

			if _, ok := colValue.(*types.EmptyValue); ok {
				if !iter.isDistinct {
					break
				}
				// converts Empty to Null
				colValue = types.NullValueInstance
			}
			state.gbTuple.values[i] = colValue
			i++
		}

		if i < iter.numGBColumns {
			continue
		}

		checksum, err := state.gbTuple.checksum()
		if err != nil {
			return false, err
		}
		gbResult, ok := state.results[checksum]
		// Update aggregate value for an existing group.
		if ok {
			for i := iter.numGBColumns; i < len(iter.columnNames); i++ {
				newValue, ok := inTuple.Get(iter.columnNames[i])
				if !ok {
					continue
				}

				err = iter.aggregate(rcb, state, gbResult.aggrTuple, i, newValue)
				if err != nil {
					return false, err
				}
			}

			rcb.trace(3, "groupIter.next(): updated an existing group: %s", gbResult)
			continue
		}

		// Start a new group.
		numAggrColumns := len(iter.columnNames) - iter.numGBColumns
		gbTuple := newGroupTuple(iter.numGBColumns)
		aggrTuple := make([]*aggrValue, numAggrColumns)

		for i := 0; i < numAggrColumns; i++ {
			aggrTuple[i], err = newAggrValue(iter.aggrFuncs[i])
			if err != nil {
				return false, err
			}
		}

		for i := 0; i < iter.numGBColumns; i++ {
			gbTuple.values[i] = state.gbTuple.values[i]
		}

		for i := iter.numGBColumns; i < len(iter.columnNames); i++ {
			v, ok := inTuple.Get(iter.columnNames[i])
			if !ok {
				continue
			}

			err = iter.aggregate(rcb, state, aggrTuple, i, v)
			if err != nil {
				return false, err
			}
		}

		checksum, err = gbTuple.checksum()
		if err != nil {
			return false, err
		}

		gbRes := &groupResult{
			gbTuple:   gbTuple,
			aggrTuple: aggrTuple,
		}

		// cache the group-by result
		state.results[checksum] = gbRes
		if iter.countMemory {
			sz := sizeOf(checksum) + sizeOf(gbRes)
			err = rcb.incMemoryConsumption(int64(sz))
			if err != nil {
				return false, err
			}
		}

		rcb.trace(3, "groupIter.next(): started a new group: %s", gbRes)

		if iter.numGBColumns == len(iter.columnNames) {
			res := &types.MapValue{}
			for i, name := range iter.columnNames {
				res.Put(name, gbTuple.values[i])
			}

			rcb.setRegValue(iter.resultReg, res)
			return true, nil
		}
	}

}

func (iter *groupIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *groupIter) displayContent(sb *strings.Builder, f *planFormatter) {
	f.printIndent(sb)
	sb.WriteString("Grouping Columns : ")
	for i := 0; i < iter.numGBColumns; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(iter.columnNames[i])
	}
	sb.WriteByte('\n')

	f.printIndent(sb)
	sb.WriteString("Aggregate Functions : ")
	for i := 0; i < len(iter.aggrFuncs); i++ {
		if i > 0 {
			sb.WriteString(",\n")
		}
		fmt.Fprintf(sb, "%s", iter.aggrFuncs[i])
	}
	sb.WriteByte('\n')

	iter.input.displayContent(sb, f)
}

func (iter *groupIter) aggregate(rcb *runtimeControlBlock, state *groupIterState,
	aggrValues []*aggrValue, column int, val types.FieldValue) (err error) {

	const one int64 = 1
	idx := column - iter.numGBColumns
	aggrVal := aggrValues[idx]
	aggrKind := iter.aggrFuncs[idx]

	switch aggrKind {
	case fnCount:
		if _, ok := val.(*types.NullValue); ok {
			return
		}

		return aggrVal.add(rcb, iter.countMemory, one, rcb.getRequest().getFPArithSpec())

	case fnCountNumbers:
		switch val.(type) {
		// Numeric values: INTEGER/LONG/DOUBLE/NUMBER
		case int, int64, float64, *big.Rat:
			return aggrVal.add(rcb, iter.countMemory, one, rcb.getRequest().getFPArithSpec())
		}

	case fnCountStar:
		return aggrVal.add(rcb, iter.countMemory, one, rcb.getRequest().getFPArithSpec())

	case fnSum:
		switch val.(type) {
		// Numeric values: INTEGER/LONG/DOUBLE/NUMBER
		case int, int64, float64, *big.Rat:
			return aggrVal.add(rcb, iter.countMemory, val, rcb.getRequest().getFPArithSpec())
		}

	case fnMin, fnMax:
		switch val.(type) {
		// min/max function does not apply to BINARY/MAP/ARRAY/EMPTY/NULL/JSON_NULL values.
		case []byte, map[string]interface{}, *types.MapValue,
			[]interface{}, []types.FieldValue, *types.EmptyValue,
			*types.NullValue, *types.JSONNullValue:
			return
		}

		// This is the very first input to min/max function.
		if _, ok := aggrVal.value.(*types.NullValue); ok {
			rcb.trace(3, "groupIter.aggregate() : setting min/max to %v", val)
			if iter.countMemory {
				sz := int64(sizeOf(val) - sizeOf(aggrVal.value))
				err = rcb.incMemoryConsumption(sz)
				if err != nil {
					return err
				}
			}
			aggrVal.value = val
			return
		}

		cmp, err := compareAtomicValues(rcb, true, val, aggrVal.value)
		if err != nil {
			return err
		}
		rcb.trace(3, "groupIter.aggregate() : compared values %v and %v, result = %v",
			val, aggrVal.value, cmp)

		if (aggrKind == fnMin && cmp < 0) || (aggrKind == fnMax && cmp > 0) {
			rcb.trace(3, "groupIter.aggregate() : setting min/max to %v", val)
			if iter.countMemory && reflect.TypeOf(val) != reflect.TypeOf(aggrVal.value) {
				sz := int64(sizeOf(val) - sizeOf(aggrVal.value))
				err = rcb.incMemoryConsumption(sz)
				if err != nil {
					return err
				}
			}
			aggrVal.value = val
			return nil
		}

	default:
		return fmt.Errorf("aggregate method of kind %v is not implemented", aggrKind)
	}

	return nil
}

func (iter *groupIter) getAggrValue(rcb *runtimeControlBlock, aggrTuple []*aggrValue, column int) interface{} {
	aggrValue := aggrTuple[column-iter.numGBColumns]
	aggrKind := iter.aggrFuncs[column-iter.numGBColumns]

	// If none of the inputs to sum() is numeric value, return NULL as result.
	if aggrKind == fnSum && !aggrValue.gotNumericInput {
		return types.NullValueInstance
	}

	return aggrValue.value
}
