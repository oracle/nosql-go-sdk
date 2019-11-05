//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqlerr

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// NoSQLErrorsTestSuite contains tests for the NoSQL errors.
type NoSQLErrorsTestSuite struct {
	suite.Suite
}

func (suite *NoSQLErrorsTestSuite) TestNewErrors() {
	var e, cause *Error
	var msg, msgOfCause string

	e = NewIllegalArgument("illegal arguments: %v", "Arg1")
	suite.Equalf(IllegalArgument, e.Code, "unexpected error code")
	suite.Equalf("illegal arguments: Arg1", e.Message, "unexpected error message")
	suite.Falsef(e.Retryable(), "IllegalArgument error should not be retryable")

	e = NewIllegalState("illegal state: %v", "Unknown")
	suite.Equalf(IllegalState, e.Code, "unexpected error code")
	suite.Equalf("illegal state: Unknown", e.Message, "unexpected error message")
	suite.Falsef(e.Retryable(), "IllegalState error should not be retryable")

	timeout := 5 * time.Second
	e = NewRequestTimeout("request timed out after %v", timeout)
	suite.Equalf(RequestTimeout, e.Code, "unexpected error code")
	suite.Equalf("request timed out after 5s", e.Message, "unexpected error message")
	suite.Falsef(e.Retryable(), "RequestTimeout error should not be retryable")

	msg = "cannot get access token from authorization server"
	e = New(SecurityInfoUnavailable, msg)
	suite.Equalf(SecurityInfoUnavailable, e.Code, "unexpected error code")
	suite.Equalf(msg, e.Message, "unexpected error message")
	suite.Truef(e.Retryable(), "SecurityInfoUnavailable error should be retryable")

	msgOfCause = "table is busy"
	cause = New(TableBusy, msgOfCause)
	msg = "request timed out after 5s"
	e = NewWithCause(RequestTimeout, cause, msg)
	suite.Equalf(RequestTimeout, e.Code, "unexpected error code")
	suite.Containsf(e.Error(), msgOfCause, "unexpected error description")
	suite.Containsf(e.Error(), msg, "unexpected error description")
	suite.Falsef(e.Retryable(), "RequestTimeout error should not be retryable")
}

func (suite *NoSQLErrorsTestSuite) TestIsErrors() {
	e1 := NewIllegalArgument("illegal arguments: Arg1")
	e2 := New(TableNotFound, "table T1 does not exist")
	e3 := New(SecurityInfoUnavailable, "cannot get access token from authorization server")
	e4 := NewIllegalState("illegal state: Unknown")
	e5 := NewRequestTimeout("request timed out after 5s")

	expectCodes := []ErrorCode{
		IllegalArgument, TableNotFound, SecurityInfoUnavailable, RequestTimeout,
	}
	errs := [...]*Error{e1, e2, e3, e4, e5}
	var ok bool
	for _, e := range errs {
		ok = IsIllegalArgument(e)
		if e == e1 {
			suite.Truef(ok, "IsIllegalArgument(err=%v) should have returned true", e)
		} else {
			suite.Falsef(ok, "IsIllegalArgument(err=%v) should have returned false", e)
		}

		ok = IsTableNotFound(e)
		if e == e2 {
			suite.Truef(ok, "IsTableNotFound(err=%v) should have returned true", e)
		} else {
			suite.Falsef(ok, "IsTableNotFound(err=%v) should have returned false", e)
		}

		ok = IsSecurityInfoUnavailable(e)
		if e == e3 {
			suite.Truef(ok, "IsSecurityInfoUnavailable(err=%v) should have returned true", e)
		} else {
			suite.Falsef(ok, "IsSecurityInfoUnavailable(err=%v) should have returned false", e)
		}

		ok = Is(e, expectCodes...)
		if e != e4 {
			suite.Truef(ok, "Is(err=%v, expectCodes=%v) should have returned true", e, expectCodes)
		} else {
			suite.Falsef(ok, "Is(err=%v, expectCodes=%v) should have returned true", e, expectCodes)
		}

		ok = Is(e)
		suite.Truef(ok, "Is(err=%v) should have returned true", e)
	}

	otherErr := errors.New("this is not a NoSQL error")
	ok = Is(otherErr)
	suite.Falsef(ok, "Is(err=%v) should have returned false", otherErr)

}

func TestNoSQLErrors(t *testing.T) {
	suite.Run(t, new(NoSQLErrorsTestSuite))
}
