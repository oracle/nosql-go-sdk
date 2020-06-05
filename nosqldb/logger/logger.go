//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package logger provides logging functionality.
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
)

// LogLevel defines a set of logging levels that used to control logging output.
//
// The logging levels are ordered. The available levels in ascending order are:
//
//   Fine
//   Debug
//   Info
//   Warn
//   Error
//
// Enabling logging at a given level also enables logging at all higher levels.
// For example, if desired logging level for the logger is set to Debug, the
// messages of Debug level, as well as Info, Warn and Error levels are all logged.
//
// In addition there is a level Off that can be used to turn off logging.
type LogLevel int

const (
	// Fine represents a level used to log tracing messages.
	Fine LogLevel = 10

	// Debug represents a level used to log debug messages.
	Debug LogLevel = 20

	// Info represents a level used to log informative messages.
	Info LogLevel = 30

	// Warn represents a level used to log warning messages.
	Warn LogLevel = 40

	// Error represents a level used to log error messages.
	Error LogLevel = 50

	// Off turns off logging.
	Off LogLevel = 99
)

// String returns a string representation for the log level.
//
// This implements the fmt.Stringer interface.
func (level LogLevel) String() string {
	switch level {
	case Fine:
		return "Fine"
	case Debug:
		return "Debug"
	case Info:
		return "Info"
	case Warn:
		return "Warn"
	case Error:
		return "Error"
	case Off:
		return "Off"
	default:
		return "N/A"
	}
}

// Logger represents a logging object that is a wrapper for log.Logger, adding
// capabilities to control the desired level of messges to log and whether the
// log entry time is displayed in local time zone or UTC.
type Logger struct {
	// logger represents a log.Logger.
	logger *log.Logger

	// level specifies the desired logging level.
	level LogLevel

	// timezone specifies the suffix that is displayed for log entry time.
	// This is an empty string if using local time zone, is "UTC" if using UTC time.
	timezone string
}

// New creates a logger that writes messages of the specified logging level to the specified io.Writer.
// If useLocalTime is set to false, the log entry displays UTC time.
//
// If specified level is set to Off or a not available value, returns nil that
// represents logging is disabled.
func New(out io.Writer, level LogLevel, useLocalTime bool) *Logger {
	if out == nil {
		return nil
	}

	switch level {
	case Fine, Debug, Info, Warn, Error:
	case Off:
		return nil
	default:
		return nil
	}

	var tz string
	flag := log.LstdFlags | log.Lmicroseconds
	if !useLocalTime {
		flag |= log.LUTC
		tz = "UTC "
	}

	return &Logger{
		level:    level,
		logger:   log.New(out, "", flag),
		timezone: tz,
	}
}

// Fine writes the specified message to the logger if the desired logging level is set to Fine.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) Fine(messageFormat string, messageArgs ...interface{}) {
	l.Log(Fine, messageFormat, messageArgs...)
}

// Debug writes the specified message to the logger if the desired logging level
// is set to Debug or a value lower than Debug such as Fine.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) Debug(messageFormat string, messageArgs ...interface{}) {
	l.Log(Debug, messageFormat, messageArgs...)
}

// Info writes the specified message to the logger if the desired logging level
// is set to Info or a value lower than Info such as Debug or Fine.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) Info(messageFormat string, messageArgs ...interface{}) {
	l.Log(Info, messageFormat, messageArgs...)
}

// Warn writes the specified message to the logger if the desired logging level
// is set to Warn or a value lower than Warn such as Info, Debug or Fine.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) Warn(messageFormat string, messageArgs ...interface{}) {
	l.Log(Warn, messageFormat, messageArgs...)
}

// Error writes the specified message to the logger if the desired logging level
// is set to Error or a value lower than Error such as Warn, Info, Debug or Fine.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) Error(messageFormat string, messageArgs ...interface{}) {
	l.Log(Error, messageFormat, messageArgs...)
}

// Log writes the specified message to logger if the specified logging level is
// the same as or higher than logger's desired level.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) Log(level LogLevel, messageFormat string, messageArgs ...interface{}) {
	if l == nil || level == Off || l.level > level {
		return
	}

	l.logger.Print(l.timezone+label(level), fmt.Sprintf(messageFormat, messageArgs...))
}

// LogWithFn calls the function fn if the specified logging level is the same as
// or higher than logger's desired level, writes the message returned from fn to
// the logger.
//
// The arguments for the logging message are handled in the manner of fmt.Printf.
func (l *Logger) LogWithFn(level LogLevel, fn func() string) {
	if l == nil || level == Off || l.level > level {
		return
	}

	l.logger.Print(l.timezone+label(level), fn())
}

// label returns a label for the specified logging level used to display in log entry.
func label(level LogLevel) string {
	switch level {
	case Fine:
		return "[FINE]  "
	case Debug:
		return "[DEBUG] "
	case Info:
		return "[INFO]  "
	case Warn:
		return "[WARN]  "
	case Error:
		return "[ERROR] "
	default:
		return ""
	}
}

// DefaultLogger represents a default logger that writes warning and higher priority events to stderr.
var DefaultLogger *Logger = New(os.Stderr, Warn, false)
