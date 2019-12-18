//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package kvstore

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/stretchr/testify/suite"
)

type AuthTestSuite struct {
	suite.Suite
}

func TestAuth(t *testing.T) {
	suite.Run(t, &AuthTestSuite{})
}

var (
	testLogger        = logger.New(os.Stderr, logger.Error, true)
	testHTTPClient, _ = httputil.NewHTTPClient(httputil.HTTPConfig{
		UseHTTPS:           true,
		InsecureSkipVerify: true,
	})
)

var testProviderOptions = []struct {
	// A short description of the test case.
	desc  string
	input auth.ProviderOptions
	want  auth.ProviderOptions
}{
	{
		desc: "invalid Timeout and ExpiryWindow",
		input: auth.ProviderOptions{
			Timeout:      500 * time.Microsecond,
			ExpiryWindow: 500 * time.Microsecond,
		},
		want: defaultOptions,
	},
	{
		desc: "valid Timeout and ExpiryWindow",
		input: auth.ProviderOptions{
			Timeout:      5 * time.Second,
			ExpiryWindow: 2 * time.Second,
		},
		want: auth.ProviderOptions{
			Timeout:      5 * time.Second,
			ExpiryWindow: 2 * time.Second,
			Logger:       defaultOptions.Logger,
			HTTPClient:   defaultOptions.HTTPClient,
		},
	},
	{
		desc: "specified Logger and HTTPClient",
		input: auth.ProviderOptions{
			Logger:     testLogger,
			HTTPClient: testHTTPClient,
		},
		want: auth.ProviderOptions{
			Timeout:      defaultOptions.Timeout,
			ExpiryWindow: defaultOptions.ExpiryWindow,
			Logger:       testLogger,
			HTTPClient:   testHTTPClient,
		},
	},
}

func (suite *AuthTestSuite) TestNewAccessTokenProvider() {
	tests := []struct {
		desc     string
		username string
		password []byte
		wantErr  bool
	}{
		{
			desc:     "empty username",
			username: "",
			password: []byte("NoSql00__123456"),
			wantErr:  true,
		},
		{
			desc:     "nil password",
			username: "TestUser01",
			password: nil,
			wantErr:  true,
		},
		{
			desc:     "empty password",
			username: "TestUser01",
			password: []byte{},
			wantErr:  true,
		},
		{
			desc:     "good username and password",
			username: "TestUser01",
			password: []byte("NoSql00__123456"),
			wantErr:  false,
		},
	}

	var msg string
	var err error
	var p *AccessTokenProvider
	for i, r := range tests {
		msg = fmt.Sprintf("Test-%d (%s): NewAccessTokenProvider() ", i+1, r.desc)
		_, err = NewAccessTokenProvider(r.username, r.password)
		if r.wantErr {
			suite.Errorf(err, msg+"should have failed")
			continue
		}

		suite.NoErrorf(err, msg+"got error %v", err)
		// Test the cases where provider options are specified.
		for j, k := range testProviderOptions {
			msg = fmt.Sprintf("Test-%d-%d (%s): NewAccessTokenProvider() ", i+1, j+1, k.desc)
			p, err = NewAccessTokenProvider(r.username, r.password, k.input)
			if suite.NoErrorf(err, msg+"got error %v", err) {
				suite.Equalf(k.want.Timeout, p.timeout, msg+"got unexpected Timeout")
				suite.Equalf(k.want.ExpiryWindow, p.expiryWindow, msg+"got unexpected ExpiryWindow")
				suite.Equalf(k.want.Logger, p.logger, msg+"got unexpected Logger")
				suite.Equalf(k.want.HTTPClient, p.httpClient, msg+"got unexpected HTTPClient")
			}
		}
	}
}

func (suite *AuthTestSuite) TestNewAccessTokenProviderWithFile() {
	tests := []struct {
		desc     string
		username *string
		password []byte
		wantErr  bool
	}{
		{
			desc:     "username not specified in config file",
			username: nil,
			password: []byte("NoSql00__123456"),
			wantErr:  true,
		},
		{
			desc:     "password not specified in config file",
			username: stringPtr("TestUser01"),
			password: nil,
			wantErr:  true,
		},
		{
			desc:     "empty username",
			username: stringPtr(""),
			password: []byte("NoSql00__123456"),
			wantErr:  true,
		},
		{
			desc:     "empty password",
			username: stringPtr("TestUser01"),
			password: []byte{},
			wantErr:  true,
		},
		{
			desc:     "good username and password",
			username: stringPtr("TestUser01"),
			password: []byte("NoSql00__123456"),
			wantErr:  false,
		},
	}

	var err error
	var configFile string
	var msg string
	var p *AccessTokenProvider

	// Specify a non-exist configuration file.
	configFile = "config_file_not_exist__"
	_, err = NewAccessTokenProviderWithFile(configFile)
	suite.Errorf(err, "NewAccessTokenProviderWithFile(file=%s) should have failed")

	for i, r := range tests {
		msg = fmt.Sprintf("Test-%d (%s): NewAccessTokenProviderWithFile() ", i+1, r.desc)
		configFile, err = createConfigFile(r.username, r.password)
		if len(configFile) > 0 {
			defer os.Remove(configFile)
		}

		if !suite.NoErrorf(err, "failed to create a config file") {
			continue
		}

		_, err = NewAccessTokenProviderWithFile(configFile)
		if r.wantErr {
			suite.Errorf(err, msg+"should have failed")
			continue
		}

		if !suite.NoErrorf(err, msg+"got error %v", err) {
			continue
		}

		// Test the cases where provider options are specified.
		for j, k := range testProviderOptions {
			msg = fmt.Sprintf("Test-%d-%d (%s): NewAccessTokenProviderWithFile() ", i+1, j+1, k.desc)
			p, err = NewAccessTokenProviderWithFile(configFile, k.input)
			if suite.NoErrorf(err, msg+"got error %v", err) {
				suite.Equalf(k.want.Timeout, p.timeout, msg+"got unexpected Timeout")
				suite.Equalf(k.want.ExpiryWindow, p.expiryWindow, msg+"got unexpected ExpiryWindow")
				suite.Equalf(k.want.Logger, p.logger, msg+"got unexpected Logger")
				suite.Equalf(k.want.HTTPClient, p.httpClient, msg+"got unexpected HTTPClient")
			}
		}
	}

}

func (suite *AuthTestSuite) TestAuthentication() {
	username := "TestUser01"
	password := []byte("NoSql00__123456")

	// Create a mock authorization server.
	mockServer := &mockAuthServer{
		tokenLifetime: 30 * time.Minute,
		username:      username,
		password:      password,
	}
	mockServer.Server = httptest.NewTLSServer(mockServer)
	defer mockServer.Server.Close()

	option := auth.ProviderOptions{
		HTTPClient:   testHTTPClient,
		Logger:       testLogger,
		ExpiryWindow: 500 * time.Millisecond,
	}

	tests := []struct {
		desc        string
		user        string
		pwd         []byte
		wantAuthErr bool
	}{
		{"wrong username", "user_not_exist", password, true},
		{"wrong password", username, []byte("wrong_password"), true},
		{"good username and password", username, password, false},
	}

	var msg string
	for i, r := range tests {
		msg = fmt.Sprintf("Test-%d (%s): ", i+1, r.desc)
		p, err := NewAccessTokenProvider(r.user, r.pwd, option)
		if !suite.NoErrorf(err, msg+"NewAccessTokenProvider() got error %v", err) {
			continue
		}

		p.SetEndpoint(mockServer.Server.URL)
		_, err = p.login()
		_, ok, _ := p.getCachedToken()
		if r.wantAuthErr {
			suite.Errorf(err, msg+"login() should have failed")
			suite.Falsef(ok, msg+"getCachedToken() should not find cached token")
			continue
		}

		// login should succeed.
		if !suite.NoErrorf(err, msg+"login() got error %v", err) {
			continue
		}
		suite.Truef(ok, msg+"getCachedToken() should find cached token")

		goodToken := p.cachedToken
		// Invalidate the cache.
		p.cachedToken = nil
		// renewToken() does not send a renew request if the cached token is nil.
		err = p.renewToken()
		suite.NoErrorf(err, msg+"renewToken() got error %v", err)

		// Inject a wrong token in the cache.
		p.cachedToken = auth.NewToken("bad-token", "", 0)
		err = p.renewToken()
		suite.Errorf(err, msg+"renewToken() with a bad token should have failed")

		err = p.logout()
		suite.Errorf(err, msg+"logout() with a bad token should have failed.")

		// Recover good token in the cache.
		p.cachedToken = goodToken
		err = p.renewToken()
		suite.NoErrorf(err, msg+"renewToken() got error %v", err)

		err = p.logout()
		suite.NoErrorf(err, msg+"logout() got error %v", err)

		p.Close()
	}
}

func (suite *AuthTestSuite) TestAuthorizationString() {
	// Test the case where client connects to the NoSQL database server that has
	// security configuration disabled.
	p := &AccessTokenProvider{}
	authStr, err := p.AuthorizationString(nil)
	if suite.NoErrorf(err, "AuthorizationString() got error %v", err) {
		suite.Emptyf(authStr, "AuthorizationString() got unexpected result")
		err = p.Close()
		suite.NoErrorf(err, "Close() got error %v", err)
	}

	// Test the case where client connects to the NoSQL database server that has
	// security configuration enabled.
	tokenLifetime := 2 * time.Second
	username := "TestUser01"
	password := []byte("NoSql00__123456")

	// Create a mock authorization server.
	mockServer := &mockAuthServer{
		tokenLifetime: tokenLifetime,
		username:      username,
		password:      password,
	}
	mockServer.Server = httptest.NewTLSServer(mockServer)
	defer mockServer.Server.Close()

	option := auth.ProviderOptions{
		HTTPClient:   testHTTPClient,
		Logger:       testLogger,
		ExpiryWindow: 300 * time.Millisecond,
	}

	// Use invalid username and password.
	p, err = NewAccessTokenProvider("NotExistsUser", password, option)
	if suite.NoErrorf(err, "NewAccessTokenProvider() got error %v") {
		p.SetEndpoint(mockServer.Server.URL)
		_, err = p.AuthorizationString(nil)
		suite.Errorf(err, "AuthorizationString() should have failed", err)

		err = p.Close()
		suite.NoErrorf(err, "Close() got error %v", err)
		// Subsequent call of Close() should be no-op.
		err = p.Close()
		suite.NoErrorf(err, "Close() got error %v", err)
	}

	// Use valid username and password.
	p, err = NewAccessTokenProvider(username, password, option)
	if !suite.NoErrorf(err, "NewAccessTokenProvider() got error %v") {
		return
	}

	p.SetEndpoint(mockServer.Server.URL)
	defer p.Close()

	suite.getAuthStringTest("single goroutine", mockServer, p)

	// Invalidate the cache to force a login.
	p.cachedToken = nil
	var wg sync.WaitGroup
	cnt := 20
	for i := 0; i < cnt; i++ {
		name := fmt.Sprintf("concurrency (goroutine-%d)", i+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.getAuthStringTest(name, mockServer, p)
		}()
	}

	wg.Wait()
}

func (suite *AuthTestSuite) getAuthStringTest(testName string,
	server *mockAuthServer, p *AccessTokenProvider) {

	// Run the test for a duration that is twice the lifetime of the access token.
	// This makes sure the access token renew process is tested.
	server.tokenLifetime = 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 2*server.tokenLifetime)
	defer cancel()

	// Create a ticker that delivers ticks at an interval between [100, 300) milliseconds.
	d := time.Duration(int64(100)+rand.Int63n(200)) * time.Millisecond
	ticker := time.NewTicker(d)

	msg := fmt.Sprintf("%s: AuthorizationString() ", testName)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return

		case <-ticker.C:
			gotAuthStr, err := p.AuthorizationString(nil)

			if !suite.NoErrorf(err, msg+"got error %v", err) {
				continue
			}

			wantAuthStr := auth.BearerToken + " " + server.getIssuedToken()
			suite.Equalf(wantAuthStr, gotAuthStr, msg+"got unexpected auth string")
		}
	}
}

func (suite *AuthTestSuite) TestParseTokenFromJSON() {
	tests := []struct {
		name           string
		jsonStr        string
		wantErr        bool
		wantToken      string
		wantExpireTime time.Time
	}{
		{
			name:    "invalid JSON",
			jsonStr: `{"token": "ABCD}`,
			wantErr: true,
		},
		{
			name:    `missing the required "token" field`,
			jsonStr: `{"expireAt": 1576108800000}`,
			wantErr: true,
		},
		{
			name:      `A valid JSON containing the "token" field`,
			jsonStr:   `{"token": "abcd"}`,
			wantErr:   false,
			wantToken: "abcd",
		},
		{
			name:           `A valid JSON containing the "token" and "expireAt" fields`,
			jsonStr:        `{"token": "efgh", "expireAt": 1576108800000}`,
			wantErr:        false,
			wantToken:      "efgh",
			wantExpireTime: time.Date(2019, time.December, 12, 0, 0, 0, 0, time.UTC),
		},
	}

	var msgPrefix string
	for i, r := range tests {
		msgPrefix = fmt.Sprintf("Test-%d (%s): parseTokenFromJSON ", i+1, r.name)
		token, err := parseTokenFromJSON([]byte(r.jsonStr))
		if r.wantErr {
			suite.Errorf(err, msgPrefix+"should have failed")
			continue
		}

		suite.Equalf(r.wantToken, token.AccessToken, msgPrefix+"got unexpected access token")
		suite.Truef(r.wantExpireTime.Equal(token.Expiry), msgPrefix+"got unexpected expire time")
	}

}

// A mock authorization server.
type mockAuthServer struct {
	*httptest.Server
	username string
	password []byte
	// The duration of time the access token is granted for.
	tokenLifetime time.Duration
	// A mutex used to guard the read/write of issuedToken.
	mutex       sync.RWMutex
	issuedToken string
}

// ServeHTTP handles login, logout and renew requests for clients.
func (m *mockAuthServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, loginService):
		usr, pwd, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintln(w, "HTTP basic authentication is required.")
			return
		}

		if usr != m.username || pwd != string(m.password) {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintln(w, "Invalid username or password.")
			return
		}

		// The expiration time for the access token is represented as the number
		// of milliseconds elapsed since January 1, 1970 UTC.
		expireAt := time.Now().Add(m.tokenLifetime).Unix() * 1000
		token := m.generateAccessToken()
		fmt.Fprintf(w, `{"token": "%s", "expireAt": %d}`, token, expireAt)
		m.setIssuedToken(token)

	case strings.HasSuffix(r.URL.Path, renewService), strings.HasSuffix(r.URL.Path, logoutService):
		authStr := r.Header.Get("Authorization")
		if authStr == "" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintln(w, "HTTP Authorization header is required.")
			return
		}

		subStrs := strings.Fields(authStr)
		if len(subStrs) != 2 {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintln(w, "Invalid HTTP Authorization header.")
			return
		}

		tokenType := subStrs[0]
		gotToken := subStrs[1]
		wantToken := m.getIssuedToken()
		if tokenType != auth.BearerToken || gotToken != wantToken {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintf(w, "Invalid access token %s, want %s", authStr, wantToken)
			return
		}

		// Issue a new access token for the client.
		if strings.HasSuffix(r.URL.Path, renewService) {
			expireAt := time.Now().Add(m.tokenLifetime).Unix() * 1000
			token := m.generateAccessToken()
			fmt.Fprintf(w, `{"token": "%s", "expireAt": %d}`, token, expireAt)
			m.setIssuedToken(token)
			return
		}

		// Logout the user.
		m.setIssuedToken("")
		fmt.Fprintln(w, `{"result": "logout was successful"}`)
	}
}

func (m *mockAuthServer) getIssuedToken() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.issuedToken
}

func (m *mockAuthServer) setIssuedToken(token string) {
	m.mutex.Lock()
	m.issuedToken = token
	m.mutex.Unlock()
}

func (m *mockAuthServer) generateAccessToken() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bs := make([]byte, 16)
	for i := range bs {
		bs[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return string(bs)
}

// createConfigFile creates a temporary configuration file with the specified
// username and password.
// The file could be used for the NewAccessTokenProviderWithFile function.
func createConfigFile(username *string, password []byte) (string, error) {
	var buf bytes.Buffer
	if username != nil {
		buf.WriteString("username=" + *username + "\n")
	}

	if password != nil {
		buf.WriteString("password=" + string(password) + "\n")
	}

	f, err := ioutil.TempFile("", "kvstore-auth-config.*~")
	if err != nil {
		return "", err
	}
	defer f.Close()

	err = ioutil.WriteFile(f.Name(), buf.Bytes(), os.FileMode(0600))
	return f.Name(), err
}

func stringPtr(s string) *string {
	return &s
}
