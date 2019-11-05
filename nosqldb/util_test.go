//
// Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"net/http"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
)

func equalError(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Error() == b.Error()
}

// DummyAccessTokenProvider represents a dummy access token provider, which is used by tests.
// It implements the AccessTokenProvider interface.
type DummyAccessTokenProvider struct {
	TenantID string
}

func (p *DummyAccessTokenProvider) AuthorizationScheme() string {
	return auth.BearerToken
}

func (p *DummyAccessTokenProvider) AuthorizationString(req auth.Request) (string, error) {
	return auth.BearerToken + " " + p.TenantID, nil
}

func (p *DummyAccessTokenProvider) Close() error {
	return nil
}

func (p DummyAccessTokenProvider) SignHttpRequest(req *http.Request) error {
	return nil
}
