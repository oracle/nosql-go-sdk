//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"net/http"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
)

// AuthorizationProvider is an interface used to provide authorization information
// for client to access authorized resources on the server.
//
// A Client should call the AuthorizationString() method to obtain an authorization
// string when it is required for the request, or call the SignHTTPRequest()
// method depending on the authorization scheme.
//
// The default implementations for this interface:
//
//   iam.SignatureProvider        cloud IAM
//   kvstore.AccessTokenProvider  on-premise
//
// Applications do not need to provide an implementation
// for this interface unless there is a special requirement.
//
// Implementations of this interface must be safe for concurrent use by multiple goroutines.
type AuthorizationProvider interface {
	// AuthorizationScheme returns a string that represents the supported
	// authorization scheme for this provider.
	//
	// The available authorization schemes are:
	//
	//   Bearer    : supported for the on-premise Oracle NoSQL server and the Oracle NoSQL cloud simulator
	//   Signature : supported for the Oracle NoSQL cloud service that uses OCI IAM for request authorization
	//
	AuthorizationScheme() string

	// AuthorizationString returns an authorization string for the specified request.
	// The returned string will be sent to the server in the request for authorization.
	// Authorization information can be request-dependent.
	AuthorizationString(req auth.Request) (string, error)

	// Some authorization providers need the entire http request to sign it
	// note this method may add or change header fields (but should never modify the
	// http data payload)
	SignHTTPRequest(httpReq *http.Request) error

	// Close releases resources allocated by the provider.
	Close() error
}

// accessTokenRequest represents a request for access token from authorization server.
//
// This implements the auth.Request interface.
type accessTokenRequest struct {
	// opReq specifies a NoSQL operation request that needs to authorize.
	opReq Request
}

// Value returns a request dependant value that represents what kind of access
// token is desired for the request.
//
// This method is reserved for future use. It returns an empty string for now.
func (r accessTokenRequest) Value() string {
	return ""
}
