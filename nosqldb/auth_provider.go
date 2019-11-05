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
	"net/http"
	"strings"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/idcs"
)

// AuthorizationProvider is an interface that provides request authorization for clients.
//
// A Client should call the AuthorizationString() method to obtain an authorization string
// when it is required for the request.
//
// The default implementations for this interface:
//
//   iam.SignatureProvider        cloud IAM
//   idcs.AccessTokenProvider     cloud IDCS
//   kvstore.AccessTokenProvider  on-prem
//
// Applications do not need to provide an implementation
// for this interface unless there is a special requirement.
//
// Implementations of this interface must be safe for concurrent use by multiple goroutines.
type AuthorizationProvider interface {
	// AuthorizationScheme returns a string that represents the supported
	// authorization scheme for this provider.
	//
	// The BearerToken scheme "Bearer" is supported for NoSQL cloud service that
	// authorizes requests by IDCS, and on-premise NoSQL server that authorizes
	// requests by itself.
	AuthorizationScheme() string

	// AuthorizationString returns an authorization string for the specified request.
	// The string will be sent to the server in the request for authorization.
	// Authorization information can be request-dependent.
	AuthorizationString(req auth.Request) (string, error)

	// Some authorization providers need the entire http request to sign it
	// note this method may add or change header fields (but should never modify the
	// http data payload)
	SignHttpRequest(httpReq *http.Request) error

	// Close releases resources allocated by the provider.
	Close() error
}

// bearerTokenRequest represents a request for access token from authorization server.
//
// This is used for NoSQL cloud service that authorizes requests by IDCS, and
// on-premise NoSQL server that authorizes requests by itself.
//
// This implements the auth.Request interface.
type bearerTokenRequest struct {
	// opReq specifies a NoSQL operation request that needs to authorize.
	opReq Request
}

// Scheme returns the authorization scheme for the bearerTokenRequest that is "Bearer".
func (r bearerTokenRequest) Scheme() string {
	return auth.BearerToken
}

// Value returns a string that represents the desired token kind depending on
// the specified operation request opReq.
//
// The returned string is either "ACCOUNT" or "SERVICE" which represent
// account access token and service access token respectively.
//
// This is only used for cloud service, is ignored for on-premise.
func (r bearerTokenRequest) Value() string {
	if needAccountAccessToken(r.opReq) {
		return string(idcs.AccountToken)
	}

	return string(idcs.ServiceToken)
}

// needAccountAccessToken returns true if the specified request needs an account
// access token, otherwise returns false.
//
// The TableRequest and ListTablesRequest for the following operations need an
// account access token:
//
//   Create tables
//   Drop tables
//   Change table limits
//   List tables
//
func needAccountAccessToken(req Request) bool {
	switch req := req.(type) {
	case *ListTablesRequest:
		return true

	case *TableRequest:
		// A non-nil TableLimits indicates the request is intended to create a
		// table or modify limits for an existing table.
		if req.TableLimits != nil {
			return true
		}

		return isDropTable(req)

	default:
		return false
	}
}

// isDropTable checks if the specified TableRequest is a drop table operation.
func isDropTable(req *TableRequest) bool {
	s := strings.Fields(req.Statement)
	if len(s) < 2 {
		return false
	}

	return strings.EqualFold(s[0], "DROP") && strings.EqualFold(s[1], "TABLE")
}
