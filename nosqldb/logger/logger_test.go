//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
package logger

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

// LoggerTestSuite contains tests for the logger.
type LoggerTestSuite struct {
	suite.Suite
}

// TestNewLogger tests the New() function that used to create a logger.
func (suite *LoggerTestSuite) TestNewLogger() {
	var out bytes.Buffer

	tests := []struct {
		out          io.Writer
		level        LogLevel
		useLocalTime bool
		expectNil    bool // Whether to expect the New() function to return nil.
	}{
		{nil, Fine, true, true},
		{&out, Off, true, true},
		{&out, Fine - 1, true, true},
		{&out, Error + 1, true, true},
		{&out, Fine, true, false},
		{&out, Fine, false, false},
		{&out, Trace, true, false},
		{&out, Trace, false, false},
		{&out, Debug, true, false},
		{&out, Debug, false, false},
		{&out, Info, true, false},
		{&out, Info, false, false},
		{&out, Warn, true, false},
		{&out, Warn, false, false},
		{&out, Error, true, false},
		{&out, Error, false, false},
	}

	var lgr *Logger
	for _, r := range tests {
		lgr = New(r.out, r.level, r.useLocalTime)
		if r.expectNil {
			suite.Nilf(lgr, "New(out=%v, level=%v, useLocalTime=%v) should have failed",
				r.out, r.level, r.useLocalTime)

		} else {
			if suite.NotNilf(lgr, "New(out=%v, level=%v, useLocalTime=%v) should have succeeded",
				r.out, r.level, r.useLocalTime) {

				suite.Equalf(r.level, lgr.level, "unexpected logging level")
				var expectTZ string
				if !r.useLocalTime {
					expectTZ = "UTC "
				}
				suite.Equalf(expectTZ, lgr.timezone, "unexpected timezone string")
			}
		}
	}

}

// TestLogMessage tests the methods that used to log messages for a specific
// logging level.
func (suite *LoggerTestSuite) TestLogMessage() {
	var out bytes.Buffer
	msg := "this is a log entry for test"
	allLevels := []LogLevel{Fine, Trace, Debug, Info, Warn, Error, Off}
	for i, level := range allLevels {
		lgr := New(&out, level, false)
		for j, logEntryLevel := range allLevels {
			out.Reset()

			switch logEntryLevel {
			case Fine:
				lgr.Fine(msg)
			case Trace:
				lgr.Trace(msg)
			case Debug:
				lgr.Debug(msg)
			case Info:
				lgr.Info(msg)
			case Warn:
				lgr.Warn(msg)
			case Error:
				lgr.Error(msg)
			case Off:
				lgr.Log(Off, msg)
			}

			msgPrefix := fmt.Sprintf("Testcase %d-%d: (LoggerLevel=%s, LogEntryLevel=%s): ",
				i+1, j+1, level, logEntryLevel)
			logEntry := out.String()
			if level == Off || logEntryLevel == Off || logEntryLevel < level {
				suite.Emptyf(logEntry, msgPrefix+"the log message should have been empty")
			} else {
				suite.Containsf(logEntry, "UTC "+label(logEntryLevel), msgPrefix+"wrong log message")
				suite.Containsf(logEntry, msg, msgPrefix+"wrong log message")
			}
		}
	}
}

// TestLogMessage tests the LogWithFn method.
func (suite *LoggerTestSuite) TestLogWithFn() {
	var out bytes.Buffer
	msg := "this is a log entry for test item: "
	actualCnt := 0
	expectCnt := 0
	fn := func() string {
		actualCnt++
		return msg + strconv.Itoa(actualCnt)
	}

	allLevels := []LogLevel{Fine, Trace, Debug, Info, Warn, Error, Off}
	for i, level := range allLevels {
		lgr := New(&out, level, false)
		for j, logEntryLevel := range allLevels {
			out.Reset()
			lgr.LogWithFn(logEntryLevel, fn)

			msgPrefix := fmt.Sprintf("Testcase %d-%d: (LoggerLevel=%s, LogEntryLevel=%s): ",
				i+1, j+1, level, logEntryLevel)
			logEntry := out.String()
			if level == Off || logEntryLevel == Off || logEntryLevel < level {
				suite.Emptyf(logEntry, msgPrefix+"the log message should have been empty")
			} else {
				expectCnt++
				suite.Containsf(logEntry, "UTC "+label(logEntryLevel), msgPrefix+"wrong log message")
				suite.Containsf(logEntry, msg+strconv.Itoa(expectCnt), msgPrefix+"wrong log message")
			}
		}
	}
}

func TestLogger(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}
