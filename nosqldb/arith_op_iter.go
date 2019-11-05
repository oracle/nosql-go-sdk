//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
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
	"strings"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

var _ planIter = (*arithOpIter)(nil)

// arithOpIter represents a plan iterator that implements the arithmetic operations.
//
// An instance of this iterator implements either addition/substraction among
// two or more input values, or multiplication/division among two or more input
// values. For example, arg1 + arg2 - arg3 + arg4, or arg1 * arg2 * arg3 / arg4.
//
// The only arithmetic op that is strictly needed for the driver is the div
// (real division) op, to compute an AVG aggregate function as the division of
// a SUM by a COUNT. However, having all the arithmetic ops implemented allows
// for expressions in the SELECT list that do arithmetic among aggregate
// functions (for example, select a, sum(x) + sum(y) from foo group by a).
type arithOpIter struct {
	*planIterDelegate

	// fnCode is a function code that indicates whether this iterator performs
	// addition/substraction or multiplication/division.
	fnCode funcCode

	// argIters represents the plan iterators for input values.
	argIters []planIter

	// ops represents the operators used in arithmetic expressions in string format.
	//
	// For addition/substraction operations, ops is a string of "+" and/or "-"
	// signs, containing one such sign per input value.
	// For example, if the arithmetic expression is (arg1 + arg2 - arg3 + arg4),
	// ops is "++-+".
	//
	// For multiplication/division operations, ops is a string of "*", "/",
	// and/or "d" signs, containing one such sign per input value.
	// For example, if the arithmetic expression is (arg1 * arg2 * arg3 / arg4),
	// ops is "***/". The "d" sign is used for the div operator.
	ops string

	// hasRealDiv indicates whether div is any of the operations to be performed
	// by this arithOpIter.
	hasRealDiv bool
}

func newArithOpIter(r proto.Reader) (iter *arithOpIter, err error) {
	delegate, err := newPlanIterDelegate(r, arithOp)
	if err != nil {
		return
	}

	code, err := r.ReadInt16()
	if err != nil {
		return
	}

	argIters, err := deserializePlanIters(r)
	if err != nil {
		return
	}

	var ops string
	p, err := r.ReadString()
	if err != nil {
		return
	}
	if p != nil {
		ops = *p
	}

	if len(ops) != len(argIters) {
		return nil, fmt.Errorf("the number of operators %d and operands %d do not match",
			len(ops)-1, len(argIters))
	}

	iter = &arithOpIter{
		planIterDelegate: delegate,
		fnCode:           funcCode(int(code)),
		argIters:         argIters,
		ops:              ops,
		hasRealDiv:       strings.Contains(ops, "d"),
	}
	return
}

func (iter *arithOpIter) open(rcb *runtimeControlBlock) (err error) {
	if err = rcb.openIter(iter.statePos); err != nil {
		return
	}

	for _, argIter := range iter.argIters {
		if err = argIter.open(rcb); err != nil {
			return
		}
	}

	return
}

func (iter *arithOpIter) reset(rcb *runtimeControlBlock) (err error) {
	for _, argIter := range iter.argIters {
		if err = argIter.reset(rcb); err != nil {
			return
		}
	}

	state := rcb.getState(iter.statePos)
	if state == nil {
		return
	}

	return state.reset()
}

func (iter *arithOpIter) close(rcb *runtimeControlBlock) (err error) {
	state := rcb.getState(iter.statePos)
	if state == nil {
		return
	}

	for _, argIter := range iter.argIters {
		if err = argIter.close(rcb); err != nil {
			return
		}
	}

	return state.close()
}

func (iter *arithOpIter) next(rcb *runtimeControlBlock) (more bool, err error) {
	state := rcb.getState(iter.statePos)
	if state.isDone() {
		return false, nil
	}

	var opResult interface{}
	// Sets initial value for operation result for different cases.
	switch {
	case iter.hasRealDiv:
		opResult = float64(1)
	case iter.fnCode == opAddSub:
		opResult = int(0)
	default:
		opResult = int(1)
	}

	// Recover from panics such as "divide by zero".
	defer func() {
		if e := recover(); e != nil {
			rcb.trace(1, "arithOpIter.next(): %v", e)
			err = fmt.Errorf("arithmetic operation failed: %v", e)
		}
	}()

	for i, argIter := range iter.argIters {
		more, err = argIter.next(rcb)
		if err != nil {
			return false, err
		}

		if !more {
			state.done()
			return false, nil
		}

		argVal := argIter.getResult(rcb)
		if argVal == types.NullValueInstance {
			rcb.trace(1, "argVal: %v, ================= got nullValue", argVal)
			rcb.setRegValue(iter.resultReg, types.NullValueInstance)
			state.done()
			return true, nil
		}

		op := iter.ops[i]
		switch argVal := argVal.(type) {
		case int:
			opResult, err = calcInt(op, opResult, argVal)
		case int64:
			opResult, err = calcInt64(op, opResult, argVal)
		case float64:
			opResult, err = calcFloat64(op, opResult, argVal)
		case *big.Rat:
			opResult, err = calcBigRat(op, opResult, argVal, rcb.getRequest().getFPArithSpec())
		default:
			return false, nosqlerr.NewIllegalState("operand in "+
				"arithmetic operation has illegal type. "+
				"operand: %d, type: %T, at %v", i, argVal, iter.loc)
		}

		if err != nil {
			return false, err
		}
	}

	rcb.trace(1, "============ arithOp result: %v", opResult)
	rcb.setRegValue(iter.resultReg, opResult)
	state.done()
	return true, nil
}

// caclInt calculates the result of applying the specified operation to the operand of int type.
func calcInt(op byte, currRes interface{}, v int) (res interface{}, err error) {
	switch r := currRes.(type) {
	case int:
		switch op {
		case '+':
			res = r + v
		case '-':
			res = r - v
		case '*':
			res = r * v
		case '/', 'd':
			res = r / v
		}

	case int64:
		switch op {
		case '+':
			res = r + int64(v)
		case '-':
			res = r - int64(v)
		case '*':
			res = r * int64(v)
		case '/', 'd':
			res = r / int64(v)
		}

	case float64:
		switch op {
		case '+':
			res = r + float64(v)
		case '-':
			res = r - float64(v)
		case '*':
			res = r * float64(v)
		case '/', 'd':
			res = r / float64(v)
		}

	case *big.Rat:
		newVal := new(big.Rat)
		newVal.SetInt64(int64(v))

		switch op {
		case '+':
			res = r.Add(r, newVal)
		case '-':
			res = r.Sub(r, newVal)
		case '*':
			res = r.Mul(r, newVal)
		case '/', 'd':
			res = r.Quo(r, newVal)
		}

	default:
		return currRes, fmt.Errorf("unsupported result type for arithmetic operation: %T", currRes)
	}

	if res == nil {
		return currRes, fmt.Errorf("unsupported operation: %q", string(op))
	}

	return res, nil
}

// calcInt64 calculates the result of applying the specified operation to the operand of int64 type.
func calcInt64(op byte, currRes interface{}, v int64) (res interface{}, err error) {
	switch r := currRes.(type) {
	case int:
		switch op {
		case '+':
			res = int64(r) + v
		case '-':
			res = int64(r) - v
		case '*':
			res = int64(r) * v
		case '/', 'd':
			res = int64(r) / v
		}

	case int64:
		switch op {
		case '+':
			res = r + v
		case '-':
			res = r - v
		case '*':
			res = r * v
		case '/', 'd':
			res = r / v
		}

	case float64:
		switch op {
		case '+':
			res = r + float64(v)
		case '-':
			res = r - float64(v)
		case '*':
			res = r * float64(v)
		case '/', 'd':
			res = r / float64(v)
		}

	case *big.Rat:
		newVal := new(big.Rat)
		newVal.SetInt64(v)

		switch op {
		case '+':
			res = r.Add(r, newVal)
		case '-':
			res = r.Sub(r, newVal)
		case '*':
			res = r.Mul(r, newVal)
		case '/', 'd':
			res = r.Quo(r, newVal)
		}

	default:
		return currRes, fmt.Errorf("unsupported result type for arithmetic operation: %T", currRes)
	}

	if res == nil {
		return currRes, fmt.Errorf("unsupported operation: %q", string(op))
	}

	return res, nil
}

// calcFloat64 calculates the result of applying the specified operation to the operand of float64 type.
func calcFloat64(op byte, currRes interface{}, v float64) (res interface{}, err error) {
	switch r := currRes.(type) {
	case int:
		switch op {
		case '+':
			res = float64(r) + v
		case '-':
			res = float64(r) - v
		case '*':
			res = float64(r) * v
		case '/', 'd':
			res = float64(r) / v
		}

	case int64:
		switch op {
		case '+':
			res = float64(r) + v
		case '-':
			res = float64(r) - v
		case '*':
			res = float64(r) * v
		case '/', 'd':
			res = float64(r) / v
		}

	case float64:
		switch op {
		case '+':
			res = r + v
		case '-':
			res = r - v
		case '*':
			res = r * v
		case '/', 'd':
			res = r / v
		}

	case *big.Rat:
		newVal := new(big.Rat)
		newVal.SetFloat64(v)

		switch op {
		case '+':
			res = r.Add(r, newVal)
		case '-':
			res = r.Sub(r, newVal)
		case '*':
			res = r.Mul(r, newVal)
		case '/', 'd':
			res = r.Quo(r, newVal)
		}

	default:
		return currRes, fmt.Errorf("unsupported result type for arithmetic operation: %T", currRes)
	}

	if res == nil {
		return currRes, fmt.Errorf("unsupported operation: %q", string(op))
	}

	return res, nil
}

// calcBigRat calculates the result of applying the specified operation to the operand of big.Rat type.
func calcBigRat(op byte, currRes interface{}, v *big.Rat, fpSpec *FPArithSpec) (res interface{}, err error) {
	var rat *big.Rat
	switch r := currRes.(type) {
	case int:
		rat = new(big.Rat).SetInt64(int64(r))

	case int64:
		rat = new(big.Rat).SetInt64(r)

	case float64:
		rat = new(big.Rat).SetFloat64(r)

	case *big.Rat:
		rat = r

	default:
		return currRes, fmt.Errorf("unsupported result type for arithmetic operation: %T", currRes)
	}

	switch op {
	case '+':
		rat.Add(rat, v)
	case '-':
		rat.Sub(rat, v)
	case '*':
		rat.Mul(rat, v)
	case '/', 'd':
		rat.Quo(rat, v)
	default:
		return 0, fmt.Errorf("unsupported operation: %q", string(op))
	}

	// TODO: apply the specified FPArithSpec to the result

	res = rat
	return res, nil
}

func (iter *arithOpIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *arithOpIter) displayContent(sb *strings.Builder, f *planFormatter) {
	for i, argIter := range iter.argIters {
		f.printIndent(sb)
		if iter.fnCode == opAddSub {
			if iter.ops[i] == '+' {
				sb.WriteString("+")
			} else {
				sb.WriteString("-")
			}
		} else {
			if iter.ops[i] == '*' {
				sb.WriteString("*")
			} else {
				sb.WriteString("/")
			}
		}

		sb.WriteString(",\n")
		iter.planIterDelegate.displayPlan(argIter, sb, f)
		if i < len(iter.argIters)-1 {
			sb.WriteString(",\n")
		}
	}
}
