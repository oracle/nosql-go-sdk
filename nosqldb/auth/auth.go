//
// Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package auth provides functionality and types used for authorization providers.
package auth

import (
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
)

const (
	// BearerToken represents the bearer token authorization scheme that the
	// bearer who holds the access token can access authorized resources.
	//
	// This is used for the on-premise Oracle NoSQL server that authorizes
	// requests by itself, and the Oracle NoSQL cloud simulator.
	BearerToken string = "Bearer"

	// Signature authorization scheme.
	// This is used for NoSQL cloud service that uses OCI IAM for request authorization.
	Signature string = "Signature"
)

// Token represents the credentials used to authorize the requests to access protected resources.
type Token struct {
	// The access token issued by the authorization server.
	AccessToken string `json:"access_token"`

	// Token type.
	// If not set, this is "Bearer" by default.
	Type string `json:"token_type,omitempty"`

	// The duration of time the access token is granted for.
	// A zero value of ExpiresIn means the access token does not expire.
	ExpiresIn time.Duration `json:"expires_in,omitempty"`

	// The time when the access token expires.
	// A zero value of Expiry means the access token does not expire.
	Expiry time.Time `json:"expiry,omitempty"`
}

// NewToken creates a token with the specified access token, token type and expiresIn duration.
func NewToken(accessToken, tokenType string, expiresIn time.Duration) *Token {
	if tokenType == "" {
		tokenType = BearerToken
	}

	t := &Token{
		AccessToken: accessToken,
		Type:        tokenType,
		ExpiresIn:   expiresIn,
	}

	if expiresIn > 0 {
		t.Expiry = time.Now().Add(expiresIn)
	}

	return t
}

// NewTokenWithExpiry creates a token with the specified access token, token type and expiry.
func NewTokenWithExpiry(accessToken, tokenType string, expiry time.Time) *Token {
	if tokenType == "" {
		tokenType = BearerToken
	}

	t := &Token{
		AccessToken: accessToken,
		Type:        tokenType,
		Expiry:      expiry,
	}

	if expiry.After(time.Now()) {
		t.ExpiresIn = time.Until(expiry)
	}

	return t
}

// Expired checks whether the access token has expired.
func (t Token) Expired() bool {
	// A zero expiry time means the access token does not expire.
	if t.Expiry.IsZero() {
		return false
	}

	return t.Expiry.Before(time.Now())
}

// NeedRefresh checks whether the access token needs to refresh.
//
// An access token needs to refresh if it is about to expire in a duration of
// time that is within the specified expiry window.
func (t Token) NeedRefresh(expiryWindow time.Duration) bool {
	if t.Expiry.IsZero() || expiryWindow <= 0 || expiryWindow > t.ExpiresIn {
		return false
	}

	return time.Until(t.Expiry) <= expiryWindow
}

// AuthString returns a string that will be set in the HTTP "Authorization" header.
func (t Token) AuthString() string {
	if t.Type == "" {
		return BearerToken + " " + t.AccessToken
	}

	return t.Type + " " + t.AccessToken
}

// Request is an interface that wraps the request dependent value for an authorization request.
type Request interface {
	// Request-dependent value.
	// The authorization provider is supposed to be able to interpret the value.
	Value() string
}

// ProviderOptions represents options for an authorization provider.
type ProviderOptions struct {
	// Timeout specifies the timeout for requests.
	// If not set, or set to a value that is less than 1 millisecond,
	// use the default timeout that depends on the concrete implementation of
	// authorization provider.
	Timeout time.Duration

	// ExpiryWindow specifies a duration of time that determines how far ahead
	// of access token expiry the provider is allowed to renew the access token.
	// If not set, or set to a value that is less than 1 millisecond,
	// use the default expiry window that depends on the concrete implementation
	// of authorization provider.
	// If set to a duration that is greater than the access token's lifetime,
	// the provider does not renew cached tokens.
	ExpiryWindow time.Duration

	// Logger specifies a logger for the provider.
	// If not set, use logger.DefaultLogger by default.
	Logger *logger.Logger

	// HTTPClient specifies an HTTP client for the provider.
	// If not set, use httputil.DefaultHTTPClient by default.
	HTTPClient *httputil.HTTPClient
}
