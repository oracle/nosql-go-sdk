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
	"math"
	"reflect"
	"strings"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

var _ planIter = (*sfwIter)(nil)

// sfwIterState represents the state for sfwIter.
type sfwIterState struct {
	*iterState

	// The value specified in OFFSET clause that indicates at what offset to
	// start returning the results.
	offset int64

	// The value specified in LIMIT clause that limits the number of results.
	limit int64

	// The number of results returned.
	numResults int64

	gbTuples   []types.FieldValue
	hasGBTuple bool
}

func newSfwIterState(iter *sfwIter) *sfwIterState {
	return &sfwIterState{
		iterState: newIterState(),
		gbTuples:  make([]types.FieldValue, len(iter.columnIters)),
	}
}

func (st *sfwIterState) reset() (err error) {
	if err = st.iterState.reset(); err != nil {
		return
	}

	st.numResults = 0
	st.hasGBTuple = false
	return nil
}

// sfwIter represents the query plan iterator for Select-From-Where expression.
//
// It is used for the following cases:
//
// (a) Project out result columns that do not appear in the SELECT list of the
// query, but are included in the results fetched from the Oracle NoSQL database
// servers, because there are order-by columns or primary-key columns used for
// duplicate elimination.
//
// (b) For group-by and aggregation queries, re-group and re-aggregate the
// partial groups/aggregates received from the Oracle NoSQL database servers.
//
// (c) Implement the OFFSET and LIMIT clauses.
type sfwIter struct {
	*planIterDelegate

	// Query plan iterator that represents the target table specified in the FROM clause.
	fromIter planIter

	// Table name or table alias name specified in the FROM clause.
	fromVarName string

	// Query plan iterators that represent the columns/expressions specified in the SELECT list.
	columnIters []planIter

	// Column names specified in the SELECT list.
	columnNames []string

	// A bool flag indicates if this is a "SELECT *" query.
	isSelectStar bool

	// Number of group-by columns.
	numGBColumns int

	// Query plan iterator for the offset clause.
	offsetIter planIter

	// Query plan iterator for the limit clause.
	limitIter planIter
}

// newSFWIter deserializes the byte streams read from the specified protocol
// reader r and creates an sfwIter instance.
func newSFWIter(r proto.Reader) (iter *sfwIter, err error) {
	delegate, err := newPlanIterDelegate(r, sfw)
	if err != nil {
		return
	}

	columnNames, err := readStringArray(r)
	if err != nil {
		return
	}

	numGBColumns, err := r.ReadInt()
	if err != nil {
		return
	}

	p, err := r.ReadString()
	if err != nil {
		return
	}

	var fromVarName string
	if p != nil {
		fromVarName = *p
	}

	isSelectStar, err := r.ReadBoolean()
	if err != nil {
		return
	}

	columnIters, err := deserializePlanIters(r)
	if err != nil {
		return
	}

	fromIter, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	offsetIter, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	limitIter, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	iter = &sfwIter{
		planIterDelegate: delegate,
		fromIter:         fromIter,
		fromVarName:      fromVarName,
		columnIters:      columnIters,
		columnNames:      columnNames,
		numGBColumns:     numGBColumns,
		isSelectStar:     isSelectStar,
		offsetIter:       offsetIter,
		limitIter:        limitIter,
	}
	return
}

func (iter *sfwIter) open(rcb *runtimeControlBlock) (err error) {
	rcb.setState(iter.statePos, newSfwIterState(iter))
	err = iter.fromIter.open(rcb)
	if err != nil {
		return
	}

	for _, colIter := range iter.columnIters {
		err = colIter.open(rcb)
		if err != nil {
			return
		}
	}

	return iter.computeOffsetLimit(rcb)
}

func (iter *sfwIter) reset(rcb *runtimeControlBlock) (err error) {
	if err = iter.fromIter.reset(rcb); err != nil {
		return
	}

	for _, colIter := range iter.columnIters {
		if err = colIter.reset(rcb); err != nil {
			return
		}
	}

	if iter.offsetIter != nil {
		if err = iter.offsetIter.reset(rcb); err != nil {
			return
		}
	}

	if iter.limitIter != nil {
		if err = iter.limitIter.reset(rcb); err != nil {
			return
		}
	}

	state := rcb.getState(iter.statePos)
	if state != nil {
		state.reset()
	}

	return iter.computeOffsetLimit(rcb)
}

func (iter *sfwIter) close(rcb *runtimeControlBlock) (err error) {
	state := rcb.getState(iter.statePos)
	if state == nil {
		return
	}

	if err = iter.fromIter.close(rcb); err != nil {
		return
	}

	for _, colIter := range iter.columnIters {
		if err = colIter.close(rcb); err != nil {
			return
		}
	}

	if iter.offsetIter != nil {
		if err = iter.offsetIter.close(rcb); err != nil {
			return
		}
	}

	if iter.limitIter != nil {
		if err = iter.limitIter.close(rcb); err != nil {
			return
		}
	}

	return state.close()
}

func (iter *sfwIter) next(rcb *runtimeControlBlock) (more bool, err error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*sfwIterState)
	if !ok {
		return false, fmt.Errorf("wrong iterator state type for sfwIter, expect *sfwIterState, got %T", st)
	}

	if state.isDone() {
		return false, nil
	}

	if state.numResults >= int64(state.limit) {
		state.done()
		return false, nil
	}

	// Skip the results until the specified offset reaches.
	for {
		more, err = iter.computeNextResult(rcb, state)
		if err != nil {
			return false, err
		}

		if !more {
			return false, nil
		}

		// Even though we have a result, the state may be DONE. This is the
		// case when the result is the last group tuple in a grouping SFW.
		// In this case, if we have not reached the offset yet, we should
		// ignore this result and return false.
		if state.isDone() && state.offset > 0 {
			return false, nil
		}

		if state.offset == 0 {
			state.numResults++
			break
		}

		state.offset--
	}

	return true, nil
}

func (iter *sfwIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *sfwIter) displayContent(sb *strings.Builder, f *planFormatter) {
	f.printIndent(sb)
	sb.WriteString("FROM:\n")
	iter.planIterDelegate.displayPlan(iter.fromIter, sb, f)
	sb.WriteString(" as ")
	sb.WriteString(iter.fromVarName)
	sb.WriteString("\n\n")

	if iter.numGBColumns >= 0 {
		f.printIndent(sb)
		sb.WriteString("GROUP BY:\n")
		f.printIndent(sb)
		if iter.numGBColumns == 0 {
			sb.WriteString("no grouping expressions")
		} else if iter.numGBColumns == 1 {
			sb.WriteString("grouping by the first expression in the SELECT list")
		} else {
			fmt.Fprintf(sb, "grouping by the first %d expressions in the SELECT list", iter.numGBColumns)
		}

		sb.WriteString("\n\n")
	}

	f.printIndent(sb)
	sb.WriteString("SELECT:\n")

	for i, colIter := range iter.columnIters {
		iter.planIterDelegate.displayPlan(colIter, sb, f)
		if i < len(iter.columnIters)-1 {
			sb.WriteString(",\n")
		}
	}

	if iter.offsetIter != nil {
		sb.WriteString("\n\n")
		f.printIndent(sb)
		sb.WriteString("OFFSET:\n")
		iter.planIterDelegate.displayPlan(iter.offsetIter, sb, f)
	}

	if iter.limitIter != nil {
		sb.WriteString("\n\n")
		f.printIndent(sb)
		sb.WriteString("LIMIT:\n")
		iter.planIterDelegate.displayPlan(iter.limitIter, sb, f)
	}
}

func (iter *sfwIter) computeOffsetLimit(rcb *runtimeControlBlock) (err error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*sfwIterState)
	if !ok {
		return fmt.Errorf("wrong iterator state type for sfwIter, expect *sfwIterState, got %T", st)
	}

	var res types.FieldValue
	var offset int64 = 0
	var limit int64 = -1

	if iter.offsetIter != nil {
		if err = iter.offsetIter.open(rcb); err != nil {
			return
		}

		if _, err = iter.offsetIter.next(rcb); err != nil {
			return
		}

		res = iter.offsetIter.getResult(rcb)
		offset, ok = res.(int64)
		if !ok {
			i, ok := res.(int)
			if !ok {
				return fmt.Errorf("offset must be an int64 or int value, got %d/%[1]T", res)
			}
			offset = int64(i)
		}

		if offset < 0 {
			return fmt.Errorf("offset cannot be a negative number, got %d", offset)
		}

		if offset > math.MaxInt32 {
			return fmt.Errorf("offset cannot be greater than maxInt32, got %d", offset)
		}

	}

	if iter.limitIter != nil {
		if err = iter.limitIter.open(rcb); err != nil {
			return
		}

		if _, err = iter.limitIter.next(rcb); err != nil {
			return
		}

		res = iter.limitIter.getResult(rcb)
		limit, ok = res.(int64)
		if !ok {
			i, ok := res.(int)
			if !ok {
				return fmt.Errorf("limit must be an int64 or int value, got %d/%[1]T", res)
			}
			limit = int64(i)
		}
		if limit < 0 {
			return fmt.Errorf("limit cannot be a negative number, got %d", limit)
		}

		if limit > math.MaxInt32 {
			return fmt.Errorf("limit cannot be greater than maxInt32, %d", limit)
		}

	}

	if limit < 0 {
		limit = math.MaxInt32
	}

	state.offset = offset
	state.limit = limit
	return nil
}

func (iter *sfwIter) computeNextResult(rcb *runtimeControlBlock, state *sfwIterState) (more bool, err error) {

	for {
		more, err = iter.fromIter.next(rcb)
		if err != nil {
			return false, err
		}

		if !more {
			if !rcb.reachedLimit {
				state.done()
			}

			if iter.numGBColumns >= 0 {
				return iter.produceLastGroup(rcb, state)
			}

			return false, nil
		}

		// Compute the expressions in the SELECT list.
		// If this is a grouping SFW, compute only the group-by columns.
		// Skip this computation if this is not a grouping SFW and it has an
		// offset that has not been reached yet.
		if iter.numGBColumns < 0 && state.offset > 0 {
			return true, nil
		}

		numCol := len(iter.columnIters)
		if iter.numGBColumns >= 0 {
			numCol = iter.numGBColumns
		}

		i := 0
		for ; i < numCol; i++ {
			colIter := iter.columnIters[i]
			more, err = colIter.next(rcb)
			if err != nil {
				return false, err
			}

			if !more {
				if iter.numGBColumns > 0 {
					err = colIter.reset(rcb)
					if err != nil {
						return false, err
					}

					break
				}

				colIter.setResult(rcb, types.NullValueInstance)

			} else {
				rcb.trace(3, "sfwIter.computeNextResult() : value for SFW column %d=%v", i, colIter.getResult(rcb))
			}

			err = colIter.reset(rcb)
			if err != nil {
				return false, err
			}
		}

		if i < numCol {
			continue
		}

		if iter.numGBColumns < 0 {
			if iter.isSelectStar {
				break
			}

			res := &types.MapValue{}
			rcb.setRegValue(iter.resultReg, res)

			for i, colIter := range iter.columnIters {
				val := colIter.getResult(rcb)
				res.Put(iter.columnNames[i], val)
			}
			break
		}

		ready, err := iter.groupInputTuple(rcb, state)
		if err != nil {
			return false, err
		}

		if ready {
			break
		}
	}

	return true, nil
}

func (iter *sfwIter) produceLastGroup(rcb *runtimeControlBlock, state *sfwIterState) (more bool, err error) {
	if rcb.reachedLimit {
		return false, nil
	}

	// If there is no group, return false
	if !state.hasGBTuple {
		return false, nil
	}

	res := &types.MapValue{}
	rcb.setRegValue(iter.resultReg, res)

	for i := 0; i < iter.numGBColumns; i++ {
		res.Put(iter.columnNames[i], state.gbTuples[i])
	}

	for i := iter.numGBColumns; i < len(iter.columnIters); i++ {
		colIter := iter.columnIters[i]
		if aggrIter, ok := colIter.(aggrPlanIter); ok {
			aggrVal, err := aggrIter.getAggrValue(rcb, true)
			if err != nil {
				return false, err
			}
			res.Put(iter.columnNames[i], aggrVal)
		}
	}

	rcb.trace(2, "sfwIter.produceLastGroup() : produced last group, result=%v", res)
	return true, nil
}

// groupInputTuple checks whether the current input tuple:
//
//   (a) starts the first group, i.e. it is the very 1st tuple in the input stream
//   (b) belongs to the current group
//   (c) starts a new group otherwise.
//
// The method returns true in case (c), indicating that an output tuple is
// ready to be returned to the consumer of this SFW, otherwise returns false.
func (iter *sfwIter) groupInputTuple(rcb *runtimeControlBlock, state *sfwIterState) (bool, error) {
	numCols := len(iter.columnIters)

	// If this is the very first input tuple, start the first group and go back
	// to compute next input tuple.
	if !state.hasGBTuple {
		for i := 0; i < iter.numGBColumns; i++ {
			state.gbTuples[i] = iter.columnIters[i].getResult(rcb)
		}

		for i := iter.numGBColumns; i < numCols; i++ {
			_, err := iter.columnIters[i].next(rcb)
			if err != nil {
				return false, err
			}

			err = iter.columnIters[i].reset(rcb)
			if err != nil {
				return false, err
			}
		}

		state.hasGBTuple = true

		rcb.trace(2, "sfwIter.groupInputTuple() : started first group")
		return false, nil
	}

	// Compare the current input tuple with the current group tuple.
	j := 0
	for ; j < iter.numGBColumns; j++ {
		newVal := iter.columnIters[j].getResult(rcb)
		currVal := state.gbTuples[j]
		if !reflect.DeepEqual(newVal, currVal) {
			break
		}
	}

	// If the input tuple is in current group, update the aggregate functions
	// and go back to compute the next input tuple.
	if j == iter.numGBColumns {
		rcb.trace(2, "sfwIter.groupInputTuple() : input tuple belongs to current group")

		for i := iter.numGBColumns; i < numCols; i++ {
			_, err := iter.columnIters[i].next(rcb)
			if err != nil {
				return false, err
			}

			err = iter.columnIters[i].reset(rcb)
			if err != nil {
				return false, err
			}
		}

		return false, nil
	}

	// Input tuple starts a new group. We must finish up the current group,
	// produce a result (output tuple) from it, and init the new group.

	// 1. Get the final aggregate values for the current group and store them in gbTuples.
	for i := iter.numGBColumns; i < numCols; i++ {
		colIter := iter.columnIters[i]
		if aggrIter, ok := colIter.(aggrPlanIter); ok {
			aggrVal, err := aggrIter.getAggrValue(rcb, true)
			if err != nil {
				return false, err
			}
			state.gbTuples[i] = aggrVal
		}
	}

	// 2. Create a result MapValue out of the GB tuple
	res := &types.MapValue{}
	rcb.setRegValue(iter.resultReg, res)

	for i := 0; i < numCols; i++ {
		res.Put(iter.columnNames[i], state.gbTuples[i])
	}

	rcb.trace(2, "sfwIter.groupInputTuple(): current group done, result=%v", res)

	// 3. Put the values of the grouping columns into the GB tuple
	for i := 0; i < iter.numGBColumns; i++ {
		state.gbTuples[i] = iter.columnIters[i].getResult(rcb)
	}

	// 4. Compute the values of the aggregate functions.
	for i := iter.numGBColumns; i < numCols; i++ {
		_, err := iter.columnIters[i].next(rcb)
		if err != nil {
			return false, err
		}

		err = iter.columnIters[i].reset(rcb)
		if err != nil {
			return false, err
		}
	}

	rcb.trace(2, "sfwIter.groupInputTuple(): Started new group.")

	return true, nil
}
