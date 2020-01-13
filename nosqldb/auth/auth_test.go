//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewToken(t *testing.T) {
	tests := []struct {
		accessToken string
		tokenType   string
		expiresIn   time.Duration
		doNotExpire bool // Whether the token does not expire.
	}{
		{"", BearerToken, 1 * time.Minute, false},
		{"abcd", "", 0, true},
		{"efgh", "JWT", -1, true},
	}

	tokens := make([]*Token, 3)
	for _, r := range tests {
		var expireTime time.Time
		if r.expiresIn > 0 {
			expireTime = time.Now().Add(r.expiresIn)
		}

		tokens[0] = NewToken(r.accessToken, r.tokenType, r.expiresIn)
		tokens[1] = NewTokenWithExpiry(r.accessToken, r.tokenType, expireTime)
		tokens[2] = &Token{
			AccessToken: r.accessToken,
			Type:        r.tokenType,
			ExpiresIn:   r.expiresIn,
		}
		expectType := r.tokenType
		if r.tokenType == "" {
			expectType = BearerToken
		}

		for i, tok := range tokens {
			assert.Equalf(t, expectType+" "+r.accessToken, tok.AuthString(),
				"token-%d [tokenType=%s] AuthString() got unexpected result", i, r.tokenType)
			if r.doNotExpire {
				assert.Falsef(t, tok.Expired(), "token-%d [expireIn=%v] Expired() got unexpected result", i, r.expiresIn)
			}
		}
	}

	// token3 expires in 3 seconds
	expiryTime := time.Now().Add(3 * time.Second)
	token3 := NewTokenWithExpiry("ijkl", BearerToken, expiryTime)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	expireWindows := []time.Duration{0, 500 * time.Millisecond, time.Second}
	for {
		select {
		case currTime := <-ticker.C:
			assert.Equalf(t, currTime.After(expiryTime), token3.Expired(), "token3.Expired() got unexpected result")

			for _, window := range expireWindows {
				needRefresh := expiryTime.Sub(currTime) <= window
				if window <= 0 {
					needRefresh = false
				}
				assert.Equalf(t, needRefresh, token3.NeedRefresh(window),
					"token3.NeedRefresh(expiryWindow=%v) got unexpected result", window)
			}

			if currTime.After(expiryTime) {
				return
			}
		}
	}

}
