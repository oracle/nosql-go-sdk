//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package cloudsim provides authorization provider implementations for clients
// that connect to cloud simulator.
package cloudsim

import (
	"net/http"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
)

// AccessTokenProvider implements the nosqldb.AuthorizationProvider interface.
// It provides a dummy access token for the examples that run against the Oracle
// NoSQL Cloud Simulator.
//
// This should only be used when running with the Cloud Simulator, it is not
// recommended for use in production.
type AccessTokenProvider struct {
	TenantID string
}

// AuthorizationScheme returns a string representation of the supported authorization scheme.
func (p *AccessTokenProvider) AuthorizationScheme() string {
	return auth.BearerToken
}

// AuthorizationString returns an authorization string for the specified request.
func (p *AccessTokenProvider) AuthorizationString(req auth.Request) (string, error) {
	return auth.BearerToken + " " + p.TenantID, nil
}

// Close releases resources allocated by the provider.
// It is no-op for this provider.
func (p *AccessTokenProvider) Close() error {
	return nil
}

// SignHTTPRequest signs the specified HTTP request.
// It is no-op for this provider.
func (p *AccessTokenProvider) SignHTTPRequest(req *http.Request) error {
	return nil
}

// GetLogger returns a logger to use.
func (p *AccessTokenProvider) GetLogger() *logger.Logger {
	return nil
}
