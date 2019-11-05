//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package idcs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/stretchr/testify/suite"
)

const (
	okUser         = "test_user"
	okPwd          = "test_pwd!"
	okClientSecret = "test_secret"
	okClientId     = "test_client_id"

	serviceAT = "service-at"
	accountAT = "account-at"
	clientAT  = "client-at"

	mockEntitlementId = "abcd-efgh-ijkl-mnop"
	mockPsmAudience   = "http://psm"
	mockPsmFQS        = mockPsmAudience + psmScope
	mockAndcAudience  = "urn:opc:andc:entitlementid=" + mockEntitlementId
	mockAndcFQS       = mockAndcAudience + andcScope
	mockPsmAppId      = "PSMApp-cacct12345"
	mockPsmSecret     = "adfjeelkc"

	tokenResult      = `{"access_token": "%s", "expires_in": 100, "token_type": "Bearer"}`
	clientInfoResult = `{` +
		`"schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],` +
		`"totalResults": 1,` +
		`"Resources": [{"name": "NoSQLClient", "allowedScopes": [` +
		`{"fqs": "` + mockPsmFQS + `",` +
		`"idOfDefiningApp": "deabd3635565402ebd4848286ae5a3a4"},` +
		`{"fqs": "` + mockAndcFQS + `",` +
		`"idOfDefiningApp": "897fe6f66712491497c20a9fa9cddaf0"}` +
		`]}]` +
		`}`
	oldPSMInfoResult = `{` +
		`"schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],` +
		`"totalResults": 1,` +
		`"Resources": [{"audience": "` + mockPsmAudience + `", ` +
		`"name": "` + mockPsmAppId + `", "clientSecret": "` + mockPsmSecret + `"}]` +
		`}`
	newPSMInfoResult = `{` +
		`"schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],` +
		`"totalResults": 1,` +
		`"Resources": [{"audience": "` + mockPsmAudience + `", "name": "` + mockPsmAppId + `"}]` +
		`}`
	emptyResult = `{"schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],` +
		`"totalResults": 0, "Resources": []}`
	errorJsonResp = `{"error": %q}`
)

var goodCredsProvider = &mockCredsProvider{
	username:     okUser,
	password:     okPwd,
	clientId:     okClientId,
	clientSecret: okClientSecret,
}

type mockIDCSError int

const (
	mockServerErr mockIDCSError = iota + 1
	mockBadReqErr
	mockEmptyResourceErr
	mockNoClientInfoErr
	mockNoClientSecretErr
)

type idcsAuthTestSuite struct {
	suite.Suite
	server      *httptest.Server
	reqHandler  *mockRequestHandler
	newProvider *AccessTokenProvider
	oldProvider *AccessTokenProvider
}

func (suite *idcsAuthTestSuite) SetupSuite() {
	var err error
	credsProvider := goodCredsProvider
	suite.reqHandler = &mockRequestHandler{credsProvider: credsProvider}
	suite.server = httptest.NewServer(suite.reqHandler)

	options := auth.ProviderOptions{
		Timeout: 5 * time.Second,
	}
	suite.newProvider, err = NewAccessTokenProviderWithURL(suite.server.URL, options)
	suite.Require().NoErrorf(err, "failed to create AccessTokenProvider, got error %v", err)
	suite.newProvider.SetCredentialsProvider(credsProvider)

	suite.oldProvider, err = NewAccessTokenProviderWithEntitlementId(suite.server.URL,
		mockEntitlementId, // specify an entitlement id
		options)
	suite.Require().NoErrorf(err, "failed to create AccessTokenProvider, got error %v", err)
	suite.oldProvider.SetCredentialsProvider(credsProvider)
}

func (suite *idcsAuthTestSuite) TearDownSuite() {
	if suite.server != nil {
		suite.server.Close()
	}
}

func (suite *idcsAuthTestSuite) SetupTest() {
	suite.reqHandler.mockErr = 0
	resetFQS(suite.newProvider, false)
	resetFQS(suite.oldProvider, true)
}

// mockRequestHandler handles incoming requests for test server.
//
// It is an implementation of http.Handler interface.
type mockRequestHandler struct {
	credsProvider *mockCredsProvider
	mockErr       mockIDCSError
}

func (m *mockRequestHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch p := req.URL.Path; p {
	case tokenEndpoint:
		m.handleTokenRequest(w, req)
	case appEndpoint:
		m.handleAppsRequest(w, req)
	default:
		writeErrorResponse(w, http.StatusNotFound, "cannot handle request '%s'", p)
	}
}

func (m *mockRequestHandler) handleTokenRequest(w http.ResponseWriter, req *http.Request) {
	switch m.mockErr {
	case mockServerErr:
		writeResponse(w, http.StatusInternalServerError, "server error")
		return
	case mockBadReqErr:
		writeBadRequestResp(w, "bad request")
		return
	default:
	}

	clientId, secret, ok := req.BasicAuth()
	if !ok {
		writeBadRequestResp(w, "cannot find \"Authorization\" header")
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeBadRequestResp(w, err.Error())
		return
	}

	payload, err := m.parseGrantPayload(body)
	if err != nil {
		writeBadRequestResp(w, err.Error())
		return
	}

	scope := payload.scope
	isPsmScope := strings.Contains(scope, mockPsmAudience)
	creds := &IDCSCredentials{
		Alias:  clientId,
		Secret: []byte(secret),
	}
	if err = m.verifyAuth(creds, isPsmScope); err != nil {
		writeErrorResponse(w, http.StatusUnauthorized, err.Error())
		return
	}

	var token string
	switch payload.grantType {
	case clientCredentialsGrant:
		if scope != idcsScope {
			writeErrorResponse(w, http.StatusUnauthorized,
				"wrong scope specified for IDCS access token request: %s", scope)
			return
		}
		token = clientAT

	case passwordGrant:
		if payload.userCreds.Alias != okUser ||
			!bytes.Equal(payload.userCreds.Secret, []byte(okPwd)) {
			writeBadRequestResp(w, "invalid username or password")
			return
		}
		switch scope {
		case idcsScope:
			token = clientAT
		case mockPsmFQS:
			token = accountAT
		case mockAndcFQS:
			token = serviceAT
		default:
			writeBadRequestResp(w, "wrong scope %s", scope)
			return
		}

	default:
		writeBadRequestResp(w, "unsupported grant type %v", payload.grantType)
		return
	}

	writeResponse(w, http.StatusOK, tokenResult, token)
}

func (m *mockRequestHandler) handleAppsRequest(w http.ResponseWriter, req *http.Request) {
	authString := req.Header.Get("Authorization")
	// Bearer client-at
	if authString != auth.BearerToken+" "+clientAT {
		writeErrorResponse(w, http.StatusUnauthorized, "invalid authorization header: %s", authString)
		return
	}

	var res string
	reqURI := req.RequestURI

	switch m.mockErr {
	case mockEmptyResourceErr:
		res = emptyResult

	case mockNoClientInfoErr:
		res = oldPSMInfoResult

	case mockNoClientSecretErr:
		res = newPSMInfoResult

	default:
		switch {
		case strings.Contains(reqURI, "PSM"):
			res = newPSMInfoResult

		case strings.Contains(reqURI, okClientId):
			res = clientInfoResult
		}
	}

	writeResponse(w, http.StatusOK, res)
}

func (m *mockRequestHandler) verifyAuth(creds *IDCSCredentials, isPsm bool) error {
	if creds.Alias == okClientId && bytes.Equal(creds.Secret, []byte(okClientSecret)) {
		return nil
	}

	if isPsm {
		if creds.Alias == mockPsmAppId && bytes.Equal(creds.Secret, []byte(mockPsmSecret)) {
			return nil
		}
		return errors.New("wrong PSM credentials")

	} else {
		return errors.New("wrong client credentials")
	}
}

func (m *mockRequestHandler) parseGrantPayload(body []byte) (req *accessTokenRequest, err error) {
	s := string(body)
	values, err := url.ParseQuery(s)
	if err != nil {
		return nil, err
	}

	req = &accessTokenRequest{
		scope:     values.Get("scope"),
		grantType: grantType(values.Get("grant_type")),
		userCreds: IDCSCredentials{
			Alias:  values.Get("username"),
			Secret: []byte(values.Get("password")),
		},
	}

	return req, nil
}

func writeResponse(w http.ResponseWriter, statusCode int, msgFmt string, msgArgs ...interface{}) {
	msg := fmt.Sprintf(msgFmt, msgArgs...)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	io.WriteString(w, msg)
}

func writeErrorResponse(w http.ResponseWriter, statusCode int, errMsgFmt string, errMsgArgs ...interface{}) {
	errMsg := fmt.Sprintf(errMsgFmt, errMsgArgs...)
	msg := fmt.Sprintf(errorJsonResp, errMsg)
	writeResponse(w, statusCode, msg)
}

// write an HTTP response with status code set to http.StatusBadRequest(400)
func writeBadRequestResp(w http.ResponseWriter, errMsgFmt string, errMsgArgs ...interface{}) {
	writeErrorResponse(w, http.StatusBadRequest, errMsgFmt, errMsgArgs...)
}

func resetFQS(p *AccessTokenProvider, psmOnly bool) {
	p.psmFQS = ""
	if !psmOnly {
		p.andcFQS = ""
	}
}

type testErrSpec struct {
	code nosqlerr.ErrorCode
	msg  string
}

// Positive tests for acquiring access tokens using AccessTokenProvider
// with and without an entitlement id specified.
func (suite *idcsAuthTestSuite) TestOKRequest() {
	var p *AccessTokenProvider

	// Acquire access token with the new provider, which does not specify an entitlement id.
	p = suite.newProvider
	p.credsProvider = goodCredsProvider
	suite.acquireAccessTokenTest(p, true, nil, "")
	suite.acquireAccessTokenTest(p, false, nil, "")

	// Acquire access token with the old provider, which specifies an entitlement id.
	p = suite.oldProvider
	p.credsProvider = goodCredsProvider
	suite.acquireAccessTokenTest(p, true, nil, "")
	suite.acquireAccessTokenTest(p, false, nil, "")
}

// Negative tests for acquiring access tokens with wrong credentials.
func (suite *idcsAuthTestSuite) TestBadCredentials() {

	testCases := []struct {
		credsProvider      CredentialsProvider
		expectServiceATErr *testErrSpec
		expectAccountATErr *testErrSpec
	}{
		// Testcase 1: Invalid username.
		{
			credsProvider: &mockCredsProvider{
				username:     "noSuchUser",
				password:     okPwd,
				clientId:     okClientId,
				clientSecret: okClientSecret,
			},
			expectServiceATErr: &testErrSpec{
				code: nosqlerr.InvalidAuthorization,
				// error message returned by test server
				msg: "invalid username or password",
			},
			expectAccountATErr: &testErrSpec{
				code: nosqlerr.InvalidAuthorization,
				msg:  "invalid username or password",
			},
		},
		// Testcase 2: Invalid password.
		{
			credsProvider: &mockCredsProvider{
				username:     okUser,
				password:     "invalid-password",
				clientId:     okClientId,
				clientSecret: okClientSecret,
			},
			expectServiceATErr: &testErrSpec{
				code: nosqlerr.InvalidAuthorization,
				msg:  "invalid username or password",
			},
			expectAccountATErr: &testErrSpec{
				code: nosqlerr.InvalidAuthorization,
				msg:  "invalid username or password",
			},
		},
		// Testcase 3: Invalid client id.
		{
			credsProvider: &mockCredsProvider{
				username:     okUser,
				password:     okPwd,
				clientId:     "noSuchClientId",
				clientSecret: okClientSecret,
			},
			expectServiceATErr: &testErrSpec{
				code: nosqlerr.IllegalArgument,
				msg:  "unable to find service scope from OAuth client",
			},
			expectAccountATErr: &testErrSpec{
				code: nosqlerr.InvalidAuthorization,
				// error message returned by test server
				msg: "wrong client credentials",
			},
		},
		// Testcase 4: Invalid client secret.
		{
			credsProvider: &mockCredsProvider{
				username:     okUser,
				password:     okPwd,
				clientId:     okClientId,
				clientSecret: "invalid-clientSecret",
			},
			expectServiceATErr: &testErrSpec{
				code: nosqlerr.IllegalArgument,
				msg:  "unable to find service scope from OAuth client",
			},
			expectAccountATErr: &testErrSpec{
				code: nosqlerr.InvalidAuthorization,
				msg:  "wrong client credentials",
			},
		},
	}

	var msgPrefix string
	for i, r := range testCases {
		msgPrefix = fmt.Sprintf("Testcase %d: ", i+1)
		p := suite.newProvider
		// Reset FQS to eliminate side effect caused by previous test runs.
		resetFQS(p, false)
		p.credsProvider = r.credsProvider
		suite.acquireAccessTokenTest(p, true, r.expectServiceATErr, msgPrefix)
		suite.acquireAccessTokenTest(p, false, r.expectAccountATErr, msgPrefix)
	}
}

// Test cases where IDCS server returns bad resource responses.
func (suite *idcsAuthTestSuite) TestBadResourceResponse() {
	testCases := []struct {
		mockErr         mockIDCSError
		wantServiceAT   bool // acquire service access token
		wantOldProvider bool // use old provider to acquire access token
		expectErr       *testErrSpec
	}{
		// Testcase 1: IDCS response does not contain client info.
		{
			mockErr:         mockNoClientInfoErr,
			wantServiceAT:   true,
			wantOldProvider: false,
			expectErr: &testErrSpec{
				code: nosqlerr.IllegalArgument,
				msg:  "unable to find service scope from OAuth client",
			},
		},
		// Testcase 2: IDCS response does not contain client secret.
		{
			mockErr:         mockNoClientSecretErr,
			wantServiceAT:   false,
			wantOldProvider: false,
			expectErr: &testErrSpec{
				code: nosqlerr.IllegalState,
				msg:  "account metadata does not have a secret",
			},
		},
		// Testcase 3: IDCS response does not contain any resources.
		{
			mockErr:         mockEmptyResourceErr,
			wantServiceAT:   false,
			wantOldProvider: true,
			expectErr: &testErrSpec{
				code: nosqlerr.InsufficientPermission,
				msg:  "empty resource list",
			},
		},
	}

	var msgPrefix string
	var p *AccessTokenProvider

	for i, r := range testCases {
		msgPrefix = fmt.Sprintf("Testcase %d: ", i+1)

		if r.wantOldProvider {
			p = suite.oldProvider
			// Reset FQS to eliminate side effect caused by previous test runs.
			resetFQS(p, true)
		} else {
			p = suite.newProvider
			resetFQS(p, false)
		}

		p.credsProvider = goodCredsProvider
		suite.reqHandler.mockErr = r.mockErr
		suite.acquireAccessTokenTest(p, r.wantServiceAT, r.expectErr, msgPrefix)
	}
}

func (suite *idcsAuthTestSuite) TestServerError() {
	var msgPrefix string
	var expectErr *testErrSpec
	var n int

	// Testcase 1:
	// Inject a server error when client tries to acquire a service access token.
	// Client would retry upon receiving server errors and return a RequestTimeout
	// error when the specified timeout elapses.
	n++
	msgPrefix = fmt.Sprintf("Testcase %d: ", n)
	expectErr = &testErrSpec{
		code: nosqlerr.IllegalState,
		msg:  fmt.Sprintf("request timed out after %v", suite.oldProvider.timeout),
	}
	suite.reqHandler.mockErr = mockServerErr
	suite.acquireAccessTokenTest(suite.oldProvider, true, expectErr, msgPrefix)

	// Testcase 2:
	// Inject a bad request error when client tries to acquire a service access token.
	n++
	msgPrefix = fmt.Sprintf("Testcase %d: ", n)
	expectErr = &testErrSpec{
		code: nosqlerr.InvalidAuthorization,
		msg:  "bad request",
	}
	suite.reqHandler.mockErr = mockBadReqErr
	suite.acquireAccessTokenTest(suite.oldProvider, true, expectErr, msgPrefix)

	// Testcase 3 and 4:
	// Connect to an invalid IDCS host or port.

	origServerURL := suite.newProvider.idcsURL
	origTimeout := suite.newProvider.timeout
	defer func() {
		suite.newProvider.idcsURL = origServerURL
		suite.newProvider.timeout = origTimeout
	}()

	suite.newProvider.credsProvider = goodCredsProvider

	// Testcase 3: Connect to an invalid IDCS host.
	n++
	suite.newProvider.idcsURL = "http://mockIDCSHost.com:5432"
	suite.newProvider.timeout = 3 * time.Second
	expectErr = &testErrSpec{
		code: nosqlerr.IllegalState,
		msg:  "Error acquiring access token",
	}
	msgPrefix = fmt.Sprintf("Testcase %d: invalid IDCS host: ", n)
	suite.acquireAccessTokenTest(suite.newProvider, true, expectErr, msgPrefix)

	// Testcase 4: Connect to an invalid IDCS port.
	n++
	u, err := url.Parse(origServerURL)
	if !suite.NoErrorf(err, "failed to parse IDCS URL: %s", origServerURL) {
		return
	}

	var portNum int
	port := u.Port()
	if len(port) == 0 {
		portNum = 5432
	} else {
		portNum, err = strconv.Atoi(port)
		if !suite.NoErrorf(err, "invalid IDCS port: %d", port) {
			return
		}
		portNum++
	}

	suite.newProvider.idcsURL = fmt.Sprintf("%s://%s:%d", u.Scheme, u.Hostname(), portNum)
	suite.newProvider.timeout = 3 * time.Second
	expectErr = &testErrSpec{
		code: nosqlerr.IllegalState,
		msg:  "Error acquiring access token",
	}
	msgPrefix = fmt.Sprintf("Testcase %d: invalid IDCS port: ", n)
	suite.acquireAccessTokenTest(suite.newProvider, true, expectErr, msgPrefix)
}

func (suite *idcsAuthTestSuite) acquireAccessTokenTest(provider *AccessTokenProvider,
	wantServiceAT bool, expectErr *testErrSpec, msgPrefix string) {

	var err error
	var token *auth.Token

	if wantServiceAT {
		msgPrefix = msgPrefix + "serviceAccessToken(): "
		token, err = provider.serviceAccessToken()
	} else {
		msgPrefix = msgPrefix + "accountAccessToken(): "
		token, err = provider.accountAccessToken()
	}

	if expectErr == nil || expectErr.code == nosqlerr.NoError {
		if !suite.NoErrorf(err, msgPrefix+"got error %v", err) {
			return
		}

		if wantServiceAT {
			suite.Equalf(serviceAT, token.AccessToken, msgPrefix+"got unexpected service access token")
		} else {
			suite.Equalf(accountAT, token.AccessToken, msgPrefix+"got unexpected account access token")
		}

		return
	}

	// Check if returned error is as expected.
	if suite.Truef(nosqlerr.Is(err, expectErr.code),
		msgPrefix+"expect %v error, got error %v", expectErr.code, err) {

		// Check error message.
		errMsg := expectErr.msg
		suite.Truef(len(errMsg) == 0 || strings.Contains(err.Error(), errMsg),
			msgPrefix+"expect error message %q, got %q", errMsg, err.Error())
	}

}

type mockCredsProvider struct {
	username     string
	password     string
	clientId     string
	clientSecret string
}

func (p *mockCredsProvider) OAuthClientCredentials() (IDCSCredentials, error) {
	return IDCSCredentials{
		Alias:  p.clientId,
		Secret: []byte(p.clientSecret),
	}, nil
}

func (p *mockCredsProvider) UserCredentials() (IDCSCredentials, error) {
	return IDCSCredentials{
		Alias:  p.username,
		Secret: []byte(p.password),
	}, nil
}

func TestIDCSAuth(t *testing.T) {
	suite.Run(t, new(idcsAuthTestSuite))
}
