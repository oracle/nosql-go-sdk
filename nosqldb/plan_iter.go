//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// planIterKind represents the kind of plan iterators.
type planIterKind int

const (
	constRef   planIterKind = 0
	varRef     planIterKind = 1
	extVarRef  planIterKind = 2
	arithOp    planIterKind = 8
	fieldStep  planIterKind = 11
	sfw        planIterKind = 14
	recv       planIterKind = 17
	sumFunc    planIterKind = 39
	minMaxFunc planIterKind = 41
	sorting    planIterKind = 47
	group      planIterKind = 65
	sorting2   planIterKind = 66
)

func (itk planIterKind) String() string {
	switch itk {
	case constRef:
		return "CONST"
	case varRef:
		return "VAR_REF"
	case extVarRef:
		return "EXTERNAL_VAR_REF"
	case arithOp:
		return "ARITH_OP"
	case fieldStep:
		return "FIELD_STEP"
	case sfw:
		return "SFW"
	case recv:
		return "RECV"
	case sumFunc:
		return "FN_SUM"
	case minMaxFunc:
		return "FN_MIN_MAX"
	case sorting:
		return "SORT"
	case group:
		return "GROUP"
	case sorting2:
		return "SORT2"
	default:
		return "UNKNOWN"
	}
}

// funcCode represents a function code that used for the built-in function.
type funcCode int

const (
	// opAddSub represents the function code for addition/subtraction operations.
	opAddSub funcCode = 14

	// opMultDiv represents the function code for multiplication/division operations.
	opMultDiv funcCode = 15

	fnCountStar funcCode = 42

	fnCount funcCode = 43

	fnCountNumbers funcCode = 44

	fnSum funcCode = 45

	// fnMin represents the function code for the min() function.
	fnMin funcCode = 47

	// fnMax represents the function code for the max() function.
	fnMax funcCode = 48
)

func (fc funcCode) String() string {
	switch fc {
	case opAddSub:
		return "OP_ADD_SUB"
	case opMultDiv:
		return "OP_MULT_DIV"
	case fnMin:
		return "FN_MIN"
	case fnMax:
		return "FN_MAX"
	case fnCountStar:
		return "FN_COUNT_STAR"
	case fnCount:
		return "FN_COUNT"
	case fnCountNumbers:
		return "FN_COUNT_NUMBERS"
	case fnSum:
		return "FN_SUM"
	default:
		return "UNKNOWN"
	}
}

type planIter interface {
	open(rcb *runtimeControlBlock) error
	next(rcb *runtimeControlBlock) (bool, error)
	close(rcb *runtimeControlBlock) error
	reset(rcb *runtimeControlBlock) error
	getResult(rcb *runtimeControlBlock) types.FieldValue
	setResult(rcb *runtimeControlBlock, val types.FieldValue)
	getKind() planIterKind
	getState(rcb *runtimeControlBlock) planIterState
	getPlan() string
	displayContent(sb *strings.Builder, f *planFormatter)
}

type funcPlanIter interface {
	planIter
	getFuncCode() funcCode
}

type aggrPlanIter interface {
	planIter
	getAggrValue(rcb *runtimeControlBlock, reset bool) (v types.FieldValue, err error)
}

func readStringArray(r proto.Reader) ([]string, error) {
	size, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	if size < -1 {
		return nil, fmt.Errorf("invalid length: %d", size)
	}

	if size == -1 {
		return nil, nil
	}

	strs := make([]string, 0, size)
	for i := 0; i < size; i++ {
		p, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		if p != nil {
			strs = append(strs, *p)
		}
	}

	return strs, nil
}

func readSequenceLength(r proto.Reader) (n int, err error) {
	n, err = r.ReadPackedInt()
	if err != nil {
		return
	}

	if n < -1 {
		return n, fmt.Errorf("invalid sequence length: %d", n)
	}

	return
}

func readSortSpecs(r proto.Reader) ([]*sortSpec, error) {
	n, err := readSequenceLength(r)
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	specs := make([]*sortSpec, n)
	for i := 0; i < n; i++ {
		specs[i], err = newSortSpec(r)
		if err != nil {
			return nil, err
		}
	}

	return specs, nil
}

func deserializePlanIters(r proto.Reader) ([]planIter, error) {
	n, err := readSequenceLength(r)
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	iters := make([]planIter, n)
	for i := 0; i < n; i++ {
		iters[i], err = deserializePlanIter(r)
		if err != nil {
			return nil, err
		}
	}

	return iters, nil
}

func deserializePlanIter(r proto.Reader) (planIter, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	v := int8(b)
	if v == -1 {
		return nil, nil
	}

	kind := planIterKind(int(b))
	switch kind {
	case constRef:
		return newConstIter(r)
	case varRef:
		return newVarRefIter(r)
	case extVarRef:
		return newExtVarRefIter(r)
	case arithOp:
		return newArithOpIter(r)
	case fieldStep:
		return newFieldStepIter(r)
	case sumFunc:
		return newFuncSumIter(r)
	case minMaxFunc:
		return newFuncMinMaxIter(r)
	case sorting, sorting2:
		return newSortIter(r, kind)
	case sfw:
		return newSFWIter(r)
	case recv:
		return newReceiveIter(r)
	case group:
		return newGroupIter(r)
	default:
		return nil, fmt.Errorf("unknown query plan iterator of kind: %d", kind)
	}

}

type planIterDelegate struct {
	kind      planIterKind
	resultReg int
	statePos  int
	loc       location
}

func newPlanIterDelegate(r proto.Reader, k planIterKind) (d *planIterDelegate, err error) {
	resultReg, err := readIntAndCheck(r, -1)
	if err != nil {
		return
	}

	statePos, err := readIntAndCheck(r, 0)
	if err != nil {
		return
	}

	startLine, err := readIntAndCheck(r, 0)
	if err != nil {
		return
	}

	startColumn, err := readIntAndCheck(r, 0)
	if err != nil {
		return
	}

	endLine, err := readIntAndCheck(r, 0)
	if err != nil {
		return
	}

	endColumn, err := readIntAndCheck(r, 0)
	if err != nil {
		return
	}

	d = &planIterDelegate{
		kind:      k,
		resultReg: resultReg,
		statePos:  statePos,
		loc: location{
			startLine:   startLine,
			startColumn: startColumn,
			endLine:     endLine,
			endColumn:   endColumn,
		},
	}
	return d, nil
}

func readIntAndCheck(r proto.Reader, min int) (i int, err error) {
	i, err = r.ReadInt()
	if err != nil {
		return
	}

	if i >= min {
		return
	}

	return 0, nosqlerr.NewIllegalArgument("invalid integer value %d, "+
		"must be greater than or equal to %d", i, min)

}

// isDone returns whether the iterator is in the DONE or CLOSED state.
// CLOSED is included because, in the order of states, a CLOSED iterator is also DONE.
func (d *planIterDelegate) isDone(rcb runtimeControlBlock) bool {
	state := rcb.getState(d.statePos)
	if state == nil {
		return false
	}
	return state.isDone() || state.isClosed()
}

// getKind returns the kind of plan iterator.
func (d *planIterDelegate) getKind() planIterKind {
	return d.kind
}

func (d *planIterDelegate) getState(rcb *runtimeControlBlock) planIterState {
	return rcb.getState(d.statePos)
}

func (d *planIterDelegate) open(rcb *runtimeControlBlock) error {
	return rcb.openIter(d.statePos)
}

func (d *planIterDelegate) close(rcb *runtimeControlBlock) error {
	return rcb.closeIter(d.statePos)
}

func (d *planIterDelegate) reset(rcb *runtimeControlBlock) error {
	state := rcb.getState(d.statePos)
	return state.reset()
}

// next moves the plan iterator to the next iteration.
func (d *planIterDelegate) next(rcb *runtimeControlBlock) (bool, error) {
	state := rcb.getState(d.statePos)
	if state.isDone() {
		return false, nil
	}

	state.done()
	return true, nil
}

func (d *planIterDelegate) setResult(rcb *runtimeControlBlock, val types.FieldValue) {
	rcb.setRegValue(d.resultReg, val)
}

// getResult returns the result stored in register that managed by runtime control block.
func (d *planIterDelegate) getResult(rcb *runtimeControlBlock) types.FieldValue {
	return rcb.getRegValue(d.resultReg)
}

func (d *planIterDelegate) getExecPlan(iter planIter) string {
	sb := &strings.Builder{}
	f := newPlanFormatter()
	d.displayPlan(iter, sb, f)
	return sb.String()
}

func (d *planIterDelegate) displayPlan(iter planIter, sb *strings.Builder, f *planFormatter) {
	f.printIndent(sb)
	d.displayName(iter, sb)
	d.displayReg(sb)
	sb.WriteString("\n")

	f.printIndent(sb)
	sb.WriteString("[\n")

	f.incIndent()
	iter.displayContent(sb, f)
	f.decIndent()
	sb.WriteString("\n")

	f.printIndent(sb)
	sb.WriteString("]")
}

func (d *planIterDelegate) displayReg(sb *strings.Builder) {
	fmt.Fprintf(sb, "([%d])", d.resultReg)
}

func (d *planIterDelegate) displayName(it planIter, sb *strings.Builder) {
	if fnIt, ok := it.(funcPlanIter); ok {
		fmt.Fprintf(sb, "%v", fnIt.getFuncCode())
	} else {
		fmt.Fprintf(sb, "%v", it.getKind())
	}
}

func (d *planIterDelegate) displayContent(sb *strings.Builder, f *planFormatter) {
	// no-op
}

type location struct {
	startLine   int
	startColumn int
	endLine     int
	endColumn   int
}

func (loc location) String() string {
	return fmt.Sprintf("[%d:%d, %d:%d]", loc.startLine, loc.startColumn,
		loc.endLine, loc.endColumn)
}

type planFormatter struct {
	indentIncrement int
	indent          int
}

func newPlanFormatter() *planFormatter {
	return &planFormatter{
		indentIncrement: 2,
	}
}

func (f *planFormatter) printIndent(b *strings.Builder) {
	if f.indent <= 0 {
		return
	}

	format := "%" + strconv.Itoa(f.indent) + "s"
	fmt.Fprintf(b, format, " ")
}

func (f *planFormatter) incIndent() {
	f.indent += f.indentIncrement
}

func (f *planFormatter) decIndent() {
	f.indent -= f.indentIncrement
}

type planIterState interface {
	isOpen() bool
	isDone() bool
	isClosed() bool
	open() error
	done() error
	reset() error
	close() error
	setState(newState iterState) error
}

var _ planIterState = (*iterState)(nil)

// iterState represents dynamic state for an iterator.
type iterState int

const (
	open    iterState = iota // 0
	running                  // 1
	done                     // 2
	closed                   // 3
)

func newIterState() *iterState {
	state := open
	return &state
}

func (st iterState) String() string {
	switch st {
	case open:
		return "OPEN"
	case running:
		return "RUNNING"
	case done:
		return "DONE"
	case closed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

func (st iterState) isOpen() bool {
	return st == open
}

func (st iterState) isDone() bool {
	return st == done
}

func (st iterState) isClosed() bool {
	return st == closed
}

func (st *iterState) open() error {
	return st.setState(open)
}

func (st *iterState) reset() error {
	return st.setState(open)
}

func (st *iterState) done() error {
	return st.setState(done)
}

func (st *iterState) close() error {
	return st.setState(closed)
}

func (st *iterState) setState(newState iterState) error {
	switch *st {
	case open, running:
		// "open --> done" transition is allowed for iterators that are "done"
		// on the 1st next() call after an open() or reset() call. In this case,
		// rather than setting the state to "running" on entrance to the next()
		// call and then setting the state again to "done" before returning from
		// the same next() call, we allow a direct transition from "open" to "done".
		*st = newState
		return nil

	case done:
		if newState == open || newState == closed {
			*st = newState
			return nil
		}

	case closed:
		if newState == closed {
			return nil
		}
	}

	return nosqlerr.NewIllegalState("wrong state transition for plan iterator. "+
		"Current state: %v, New state: %v", *st, newState)
}
