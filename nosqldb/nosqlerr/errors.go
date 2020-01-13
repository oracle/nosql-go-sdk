//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

//go:generate stringer -type=ErrorCode -output errorcode_string.go

// Package nosqlerr defines types and error code constants that represent errors
// which may return by the NoSQL client.
package nosqlerr

import (
	"fmt"
)

// Error represents an error that wraps the error code, error message and an
// optional cause of the error.
//
// This implements the error interface.
type Error struct {
	// Code specifies the error code.
	Code ErrorCode `json:"code"`

	// Message specifies the description of error.
	Message string `json:"message"`

	// Cause optionally specifies the cause of error.
	Cause error `json:"cause,omitempty"`
}

// New creates an error with the specified error code and message.
func New(code ErrorCode, msgFmt string, msgArgs ...interface{}) *Error {
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(msgFmt, msgArgs...),
	}
}

// NewWithCause creates an error with the specified error code, message and the cause of error.
func NewWithCause(code ErrorCode, cause error, msgFmt string, msgArgs ...interface{}) *Error {
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(msgFmt, msgArgs...),
		Cause:   cause,
	}
}

// Error returns a descriptive message for the error.
func (e *Error) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("[%s]: %s", e.Code.String(), e.Message)
	}

	return fmt.Sprintf("[%s]: %s. Caused by:\n\t%s", e.Code.String(), e.Message, e.Cause.Error())
}

// Retryable returns whether the error is retryable.
func (e *Error) Retryable() bool {
	return retryableErrors[e.Code]
}

// retryableErrors represents a map whose keys are the error codes of pre-defined
// errors that are retryable. This is used as a fast lookup table to check if
// an error is retryable.
var retryableErrors map[ErrorCode]bool = map[ErrorCode]bool{
	SecurityInfoUnavailable: true,
	RetryAuthentication:     true,
	ServerError:             true,
	TableBusy:               true,
	OperationLimitExceeded:  true,
	ReadLimitExceeded:       true,
	WriteLimitExceeded:      true,
	SizeLimitExceeded:       true,
}

// NewIllegalArgument creates an IllegalArgument error with the specified message.
func NewIllegalArgument(msgFmt string, msgArgs ...interface{}) *Error {
	return New(IllegalArgument, msgFmt, msgArgs...)
}

// NewIllegalState creates an IllegalState error with the specified message.
func NewIllegalState(msgFmt string, msgArgs ...interface{}) *Error {
	return New(IllegalState, msgFmt, msgArgs...)
}

// NewRequestTimeout creates a RequestTimeout error with the specified message.
func NewRequestTimeout(msgFmt string, msgArgs ...interface{}) *Error {
	return New(RequestTimeout, msgFmt, msgArgs...)
}

// Is checks if the specified error is an Error value and the error code
// matches any of the expected error codes if specified.
func Is(err error, expectedCodes ...ErrorCode) bool {
	e, ok := err.(*Error)
	if !ok {
		return false
	}

	if len(expectedCodes) == 0 {
		return true
	}

	for _, code := range expectedCodes {
		if e.Code == code {
			return true
		}
	}

	return false
}

// IsTableNotFound returns true if the specified error is a TableNotFound error,
// otherwise returns false.
func IsTableNotFound(err error) bool {
	return Is(err, TableNotFound)
}

// IsIllegalArgument returns true if the specified error is an IllegalArgument error,
// otherwise returns false.
func IsIllegalArgument(err error) bool {
	return Is(err, IllegalArgument)
}

// IsSecurityInfoUnavailable returns true if the specified error is a SecurityInfoUnavailable error,
// otherwise returns false.
func IsSecurityInfoUnavailable(err error) bool {
	return Is(err, SecurityInfoUnavailable)
}

// ErrorCode represents the error code.
// Error codes are divided into categories as follows:
//
// 1. Error codes for user-generated errors, range from 1 to 50(exclusive).
// These include illegal arguments, exceeding size limits for some objects,
// resource not found, etc.
//
// 2. Error codes for user throttling, range from 50 to 100(exclusive).
//
// 3. Error codes for server issues, range from 100 to 150(exclusive).
//
// 3.1 Retryable server issues, range from 100 to 125(exclusive), that represent
// internal problems, presumably temporary, and need to be sent back to the
// application for retry.
//
// 3.2 Other server issues, begin from 125.
// These include server illegal state, unknown server error, etc.
// They might be retryable, or not.
//
type ErrorCode int

const (
	// NoError represents there is no error.
	NoError ErrorCode = iota // 0

	// UnknownOperation error represents the operation attempted is unknown.
	UnknownOperation // 1

	// TableNotFound error represents the operation attempted to access a table
	// that does not exist or is not in a visible state.
	TableNotFound // 2

	// IndexNotFound error represents the operation attempted to access an index
	// that does not exist or is not in a visible state.
	IndexNotFound // 3

	// IllegalArgument error represents the application provided an illegal
	// argument for the operation.
	IllegalArgument // 4

	// RowSizeLimitExceeded error represents an attempt has been made to create
	// a row with a size that exceeds the system defined limit.
	//
	// This is used for cloud service only.
	RowSizeLimitExceeded // 5

	// KeySizeLimitExceeded error represents an attempt has been made to create
	// a row with a primary key or index key size that exceeds the system defined limit.
	//
	// This is used for cloud service only.
	KeySizeLimitExceeded // 6

	// BatchOpNumberLimitExceeded error represents that the number of operations
	// included in Client.WriteMultiple operation exceeds the system defined limit.
	//
	// This is used for cloud service only.
	BatchOpNumberLimitExceeded // 7

	// RequestSizeLimitExceeded error represents that the size of a request
	// exceeds the system defined limit.
	//
	// This is used for cloud service only.
	RequestSizeLimitExceeded // 8

	// TableExists error represents the operation attempted to create a table
	// but the named table already exists.
	TableExists // 9

	// IndexExists error represents the operation attempted to create an index
	// for a table but the named index already exists.
	IndexExists // 10

	// InvalidAuthorization error represents the client provides an invalid
	// authorization string in the request header.
	InvalidAuthorization // 11

	// InsufficientPermission error represents an application does not have
	// sufficient permission to perform a request.
	InsufficientPermission // 12

	// ResourceExists error represents the operation attempted to create a
	// resource but it already exists.
	ResourceExists // 13

	// ResourceNotFound error represents the operation attempted to access a
	// resource that does not exist or is not in a visible state.
	ResourceNotFound // 14

	// TableLimitExceeded error represents an attempt has been made to create a
	// number of tables that exceeds the system defined limit.
	//
	// This is used for cloud service only.
	TableLimitExceeded // 15

	// IndexLimitExceeded error represents an attempt has been made to create
	// more indexes on a table than the system defined limit.
	//
	// This is used for cloud service only.
	IndexLimitExceeded // 16

	// BadProtocolMessage error represents there is an error in the protocol
	// used by client and server to exchange informations.
	// This error is not visible to applications. It is wrapped as an IllegalArgument
	// error and returned to applications.
	BadProtocolMessage // 17

	// EvolutionLimitExceeded error represents an attempt has been made to evolve
	// the schema of a table more times than allowed by the system defined limit.
	//
	// This is used for cloud service only.
	EvolutionLimitExceeded // 18

	// TableDeploymentLimitExceeded error represents an attempt has been made to
	// create or modify a table using limits that exceed the maximum allowed for
	// a single table.
	//
	// This is system-defined limit, used for cloud service only.
	TableDeploymentLimitExceeded // 19

	// TenantDeploymentLimitExceeded error represents an attempt has been made to
	// create or modify a table using limits that cause the tenant's aggregate
	// resources to exceed the maximum allowed for a tenant.
	//
	// This is system-defined limit, used for cloud service only.
	TenantDeploymentLimitExceeded // 20

	// OperationNotSupported error represents the operation attempted is not supported.
	// This may be related to on-premise vs cloud service configurations.
	OperationNotSupported // 21
)

const (
	// ReadLimitExceeded error represents that the provisioned read throughput
	// has been exceeded.
	//
	// Operations resulting in this error can be retried but it is recommended
	// that callers use a delay before retrying in order to minimize the chance
	// that a retry will also be throttled. Applications should attempt to avoid
	// throttling errors by rate limiting themselves to the degree possible.
	//
	// Retries and behavior related to throttling can be managed by configuring
	// the DefaultRetryHandler for client or by providing a custom implementation
	// of the RetryHandler interface for client.
	//
	// This is used for cloud service only.
	ReadLimitExceeded ErrorCode = iota + 50 // 50

	// WriteLimitExceeded error represents that the provisioned write throughput
	// has been exceeded.
	//
	// Operations resulting in this error can be retried but it is recommended
	// that callers use a delay before retrying in order to minimize the chance
	// that a retry will also be throttled. Applications should attempt to avoid
	// throttling errors by rate limiting themselves to the degree possible.
	//
	// Retries and behavior related to throttling can be managed by configuring
	// the DefaultRetryHandler for client or by providing a custom implementation
	// of the RetryHandler interface for client.
	//
	// This is used for cloud service only.
	WriteLimitExceeded // 51

	// SizeLimitExceeded error represents a table size limit has been exceeded
	// by writing more data than the table can support.
	// This error is not retryable because the conditions that lead to it being
	// retuned, while potentially transient, typically require user intervention.
	SizeLimitExceeded // 52

	// OperationLimitExceeded error represents the operation attempted has exceeded
	// the allowed limits for non-data operations defined by the system.
	//
	// This error is returned when a non-data operation is throttled.
	// This can happen if an application attempts too many control operations
	// such as table creation, deletion, or similar methods. Such operations
	// do not use throughput or capacity provisioned for a given table but they
	// consume system resources and their use is limited.
	//
	// Operations resulting in this error can be retried but it is recommended
	// that callers use a relatively large delay before retrying in order to
	// minimize the chance that a retry will also be throttled.
	//
	// This is used for cloud service only.
	OperationLimitExceeded // 53
)

const (
	// RequestTimeout error represents the request cannot be processed or does
	// not complete when the specified timeout duration elapses.
	//
	// If a retry handler is configured for the client it is possible that the
	// request has been retried a number of times before the timeout occurs.
	RequestTimeout ErrorCode = iota + 100 // 100

	// ServerError represents there is an internal system problem.
	// Most system problems are temporary.
	// The operation that leads to this error may need to retry.
	ServerError // 101

	// ServiceUnavailable error represents the requested service is currently unavailable.
	// This is usually a temporary error.
	// The operation that leads to this error may need to retry.
	ServiceUnavailable // 102

	// TableBusy error represents the table is in use or busy.
	// This error may be returned when a table operation fails.
	// Note that only one modification operation at a time is allowed on a table.
	TableBusy // 103

	// SecurityInfoUnavailable error represents the security information is not
	// ready in the system.
	// This error will occur as the system acquires security information and
	// must be retried in order for authorization to work properly.
	//
	// This is used for cloud service only.
	SecurityInfoUnavailable // 104

	// RetryAuthentication error represents the authentication failed and may need to retry.
	// This may be returned by kvstore.AccessTokenProvider in the following cases:
	//
	// 1. Authentication information was not provided in the request header.
	// 2. The user session has expired. By default kvstore.AccessTokenProvider
	// will automatically retry authentication with user credentials provided.
	//
	RetryAuthentication // 105
)

const (
	// UnknownError represents an unknown error has occurred on the server.
	UnknownError ErrorCode = iota + 125 // 125

	// IllegalState error represents an illegal state.
	IllegalState // 126
)
