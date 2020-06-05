//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"strings"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

var (
	_ planIter = (*varRefIter)(nil)
	_ planIter = (*extVarRefIter)(nil)
	_ planIter = (*constIter)(nil)
	_ planIter = (*fieldStepIter)(nil)
)

// varRefIter represents a reference to a non-external variable in the query.
//
// It simply returns the value that the variable is currently bound to.
// This value is computed by the variable's "domain iterator" (the iterator that
// evaluates the domain expression of the variable). The domain iterator stores
// the value in resultReg of this varRefIter.
//
// In the context of the driver, an implicit internal variable is used to
// represent the results arriving from the Oracle NoSQL servers.
// All other expressions that are computed at the driver operate on these
// results, so all such expressions reference this variable.
type varRefIter struct {
	*planIterDelegate

	// The variable name.
	name string
}

func newVarRefIter(r proto.Reader) (iter *varRefIter, err error) {
	delegate, err := newPlanIterDelegate(r, varRef)
	if err != nil {
		return
	}

	p, err := r.ReadString()
	if err != nil {
		return
	}

	var name string
	if p != nil {
		name = *p
	}

	iter = &varRefIter{
		planIterDelegate: delegate,
		name:             name,
	}
	return
}

func (iter *varRefIter) next(rcb *runtimeControlBlock) (bool, error) {
	state := rcb.getState(iter.statePos)
	if state.isDone() {
		rcb.trace(1, "there is no value for variable %q in register %d", iter.name, iter.resultReg)
		return false, nil
	}

	rcb.trace(1, "the value of variable %q in register %d is %v",
		iter.name, iter.resultReg, rcb.getRegValue(iter.resultReg))
	state.done()
	return true, nil
}

func (iter *varRefIter) getPlan() string {
	sb := &strings.Builder{}
	f := newPlanFormatter()
	f.printIndent(sb)
	iter.displayContent(sb, f)
	iter.planIterDelegate.displayReg(sb)
	return sb.String()
}

func (iter *varRefIter) displayContent(sb *strings.Builder, f *planFormatter) {
	fmt.Fprintf(sb, "VAR_REF(%s)", iter.name)
}

// extVarRefIter represents a reference to an external variable in the query.
//
// extVarRefIter simply returns the value that the variable is currently bound to.
// This value is set by the application via the methods of QueryRequest.
type extVarRefIter struct {
	*planIterDelegate

	// The external variable name.
	// This is used only when displaying the execution plan and in error messages.
	name string

	// The variable id.
	// This is used as an index into an array of FieldValues in the RCB that
	// stores the values of the external variables.
	id int
}

func newExtVarRefIter(r proto.Reader) (iter *extVarRefIter, err error) {
	delegate, err := newPlanIterDelegate(r, extVarRef)
	if err != nil {
		return
	}

	p, err := r.ReadString()
	if err != nil {
		return
	}

	var name string
	if p != nil {
		name = *p
	}

	id, err := readIntAndCheck(r, 0)
	if err != nil {
		return
	}

	iter = &extVarRefIter{
		planIterDelegate: delegate,
		name:             name,
		id:               id,
	}
	return
}

func (iter *extVarRefIter) next(rcb *runtimeControlBlock) (bool, error) {
	state := rcb.getState(iter.statePos)
	if state.isDone() {
		return false, nil
	}

	value := rcb.getExternalVar(iter.id)
	if value == nil {
		return false, nosqlerr.NewIllegalState("the external variable %q has not been set", iter.name)
	}

	rcb.setRegValue(iter.resultReg, value)
	state.done()
	return true, nil
}

func (iter *extVarRefIter) getPlan() string {
	sb := &strings.Builder{}
	f := newPlanFormatter()
	f.printIndent(sb)
	iter.displayContent(sb, f)
	iter.planIterDelegate.displayReg(sb)
	return sb.String()
}

func (iter *extVarRefIter) displayContent(sb *strings.Builder, f *planFormatter) {
	fmt.Fprintf(sb, "EXTERNAL_VAR_REF(%s, %d)", iter.name, iter.id)
}

// constIter represents a reference to a constant value in the query.
// Such a reference will need to be "executed" at the driver side when the
// constant appears in the OFFSET or LIMIT clause.
type constIter struct {
	*planIterDelegate
	value types.FieldValue
}

func newConstIter(r proto.Reader) (iter *constIter, err error) {
	delegate, err := newPlanIterDelegate(r, constRef)
	if err != nil {
		return
	}

	value, err := r.ReadFieldValue()
	if err != nil {
		return
	}

	iter = &constIter{
		planIterDelegate: delegate,
		value:            value,
	}
	return
}

func (iter *constIter) open(rcb *runtimeControlBlock) (err error) {
	if err = iter.planIterDelegate.open(rcb); err != nil {
		return
	}

	rcb.setRegValue(iter.resultReg, iter.value)
	return
}

func (iter *constIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *constIter) displayContent(sb *strings.Builder, f *planFormatter) {
	f.printIndent(sb)
	sb.WriteString(jsonutil.AsJSON(iter.value))
}

// fieldStepIter returns the value of a field in an input MapValue.
// It is used by the driver to implement column references in the SELECT list (see sfwIter).
type fieldStepIter struct {
	*planIterDelegate

	// The input plan iterator.
	input planIter

	// The field name.
	fieldName string
}

func newFieldStepIter(r proto.Reader) (iter *fieldStepIter, err error) {
	delegate, err := newPlanIterDelegate(r, fieldStep)
	if err != nil {
		return
	}

	input, err := deserializePlanIter(r)
	if err != nil {
		return
	}

	p, err := r.ReadString()
	if err != nil {
		return
	}

	var fieldName string
	if p != nil {
		fieldName = *p
	}

	iter = &fieldStepIter{
		planIterDelegate: delegate,
		input:            input,
		fieldName:        fieldName,
	}
	return
}

func (iter *fieldStepIter) open(rcb *runtimeControlBlock) (err error) {
	if err = iter.planIterDelegate.open(rcb); err != nil {
		return
	}

	return iter.input.open(rcb)
}

func (iter *fieldStepIter) close(rcb *runtimeControlBlock) (err error) {
	state := rcb.getState(iter.statePos)
	if state == nil {
		return nil
	}

	if err = iter.input.close(rcb); err != nil {
		return
	}

	return state.close()
}

func (iter *fieldStepIter) reset(rcb *runtimeControlBlock) (err error) {
	if err = iter.input.reset(rcb); err != nil {
		return err
	}

	state := rcb.getState(iter.statePos)
	return state.reset()
}

func (iter *fieldStepIter) next(rcb *runtimeControlBlock) (more bool, err error) {
	state := rcb.getState(iter.statePos)
	if state.isDone() {
		return false, nil
	}

	var value types.FieldValue

	for {
		more, err = iter.input.next(rcb)
		if err != nil {
			return false, err
		}
		value = iter.input.getResult(rcb)
		if !more || value == types.EmptyValueInstance {
			state.done()
			return false, nil
		}

		switch value.(type) {
		case *types.MapValue, []types.FieldValue, []interface{}:
			// no-op
		default:
			continue
		}

		if value == types.NullValueInstance {
			rcb.setRegValue(iter.resultReg, value)
			return true, nil
		}

		switch v := value.(type) {
		case *types.MapValue:
			res, ok := v.Get(iter.fieldName)
			if !ok || res == nil {
				continue
			}

			rcb.setRegValue(iter.resultReg, res)
			return true, nil

		default:
			return false, nosqlerr.NewIllegalState("the input value in field step has invalid type %T, "+
				"expect *types.MapValue", value)
		}
	}

}

func (iter *fieldStepIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *fieldStepIter) displayContent(sb *strings.Builder, f *planFormatter) {
	iter.planIterDelegate.displayPlan(iter.input, sb, f)

	sb.WriteString(",\n")
	f.printIndent(sb)
	sb.WriteString(iter.fieldName)
}
