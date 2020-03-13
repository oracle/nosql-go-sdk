//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// Package kvstore provides authorization provider implementations for clients
// that connect to on-premise NoSQL servers.
package kvstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

const (
	// Login service endpoint.
	loginService = "/login"
	// Logout service endpoint.
	logoutService = "/logout"
	// Token renew service endpoint.
	renewService = "/renew"
)

// Default options for the provider.
var defaultOptions = auth.ProviderOptions{
	Timeout:      10 * time.Second,
	ExpiryWindow: 2 * time.Minute,
	Logger:       logger.DefaultLogger,
	HTTPClient:   httputil.DefaultHTTPClient,
}

// AccessTokenProvider is an access token provider used for on-premise NoSQL server.
//
// This implements the nosqldb.AuthorizationProvider interface.
type AccessTokenProvider struct {
	// Endpoint for authorization server.
	endpoint string

	// Username and password are used for the NoSQL server that is enabled
	// with security configurations.
	//
	// Username used to authenticate with the NoSQL server.
	username string

	// Password used to authenticate with the NoSQL server.
	password []byte

	// Logger.
	logger *logger.Logger

	// HTTP client.
	httpClient *httputil.HTTPClient

	// HTTP request headers.
	reqHeaders map[string]string

	// Request timeout.
	timeout time.Duration

	// HTTP basic authentication string.
	basicAuth string

	// isSecure represents if the NoSQL server is enabled with security or not.
	isSecure bool

	// isClosed represents if the provider is closed or not.
	isClosed bool

	// Cached token that can be reused when it is valid.
	cachedToken *auth.Token

	// A duration of time that determines how far ahead of access token expiry
	// the provider is allowed to renew the token.
	expiryWindow time.Duration

	mutex sync.RWMutex
	wg    sync.WaitGroup
}

// NewAccessTokenProviderFromFile creates an access token provider using the
// specified configuration file and options. The configuration file must specify
// the username and password that used to authenticate with the Oracle NoSQL
// Server in the form of:
//
//   username=user1
//   password=NoSql00__123456
//
// This is a variadic function that may be invoked with zero or more arguments
// for the options parameter, but only the first argument for the options
// parameter, if specified, is used, others are ignored.
//
// This is used for the NoSQL server that is enabled with security configurations.
func NewAccessTokenProviderFromFile(configFile string, options ...auth.ProviderOptions) (*AccessTokenProvider, error) {
	prop, err := sdkutil.NewProperties(configFile)
	if err != nil {
		return nil, err
	}

	prop.Load()

	username, err := prop.Get("username")
	if err != nil {
		return nil, err
	}

	password, err := prop.Get("password")
	if err != nil {
		return nil, err
	}

	return NewAccessTokenProvider(username, []byte(password), options...)
}

// NewAccessTokenProvider creates an access token provider with the specified
// username, password and options.
//
// This is a variadic function that may be invoked with zero or more arguments
// for the options parameter, but only the first argument for the options
// parameter, if specified, is used, others are ignored.
//
// This is used for the NoSQL server that is enabled with security configurations.
func NewAccessTokenProvider(username string, password []byte, options ...auth.ProviderOptions) (*AccessTokenProvider, error) {
	if username == "" {
		return nil, nosqlerr.NewIllegalArgument("username must be non-empty")
	}

	if len(password) == 0 {
		return nil, nosqlerr.NewIllegalArgument("password must be non-nil and non-empty")
	}

	// Initialize with default options.
	opt := defaultOptions
	// Overwrite with supplied values if they are valid.
	if len(options) > 0 {
		v := &options[0]
		if v.Timeout >= time.Millisecond {
			opt.Timeout = v.Timeout
		}

		if v.ExpiryWindow >= time.Millisecond {
			opt.ExpiryWindow = v.ExpiryWindow
		}

		if v.Logger != nil {
			opt.Logger = v.Logger
		}

		if v.HTTPClient != nil {
			opt.HTTPClient = v.HTTPClient
		}
	}

	p := &AccessTokenProvider{
		username:     username,
		isSecure:     true,
		timeout:      opt.Timeout,
		expiryWindow: opt.ExpiryWindow,
		logger:       opt.Logger,
		httpClient:   opt.HTTPClient,
		reqHeaders: map[string]string{
			"Accept":     "application/json",
			"Connection": "keep-alive",
			"User-Agent": sdkutil.UserAgent(),
		},
	}

	p.password = make([]byte, len(password))
	copy(p.password, password)
	p.basicAuth = httputil.BasicAuth(p.username, p.password)

	return p, nil
}

// SetEndpoint sets the specified authorization server endpoint for the provider.
//
// This method is exported for use by the nosqldb.Client.
// Applications should not use this method.
func (p *AccessTokenProvider) SetEndpoint(endpoint string) *AccessTokenProvider {
	p.endpoint = endpoint + sdkutil.SecurityServiceURI
	return p
}

// AuthorizationScheme returns "Bearer" for this provider which means the bearer
// who holds the access token can access authorized resources.
func (p *AccessTokenProvider) AuthorizationScheme() string {
	return auth.BearerToken
}

// AuthorizationString returns an authorization string used for the specified
// request, which is in the form of:
//
//   Bearer <access_token>
//
// This method looks for the access token from local cache, if found, returns
// the token, otherwise acquires the access token from remote authorization
// server.
//
// If the token retrieved from cache is valid, and is about to expire in a
// duration of time that is within the specified expiry window, this method
// starts a new goroutine to renew the token and refresh the cache.
//
// A cached token may not get a chance to renew if it is not retrieved by the
// provider within the expiry window, which means it is not recently used.
func (p *AccessTokenProvider) AuthorizationString(req auth.Request) (auth string, err error) {
	if !p.isSecure || p.checkClosed() {
		return
	}

	p.mutex.RLock()
	token, ok, needRenew := p.getCachedToken()
	p.mutex.RUnlock()

	// Cached token is nil or expired.
	if !ok {
		return p.login()
	}

	if needRenew {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.renewToken()
		}()
	}

	return token.AuthString(), nil
}

// SignHTTPRequest is unused in kvstore on-prem logic
func (p *AccessTokenProvider) SignHTTPRequest(req *http.Request) error {
	return nil
}

// Close releases resources allocated by the provider and sets closed state for the provider.
func (p *AccessTokenProvider) Close() error {
	if !p.isSecure || p.checkClosed() {
		return nil
	}

	p.wg.Wait()

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if err := p.logout(); err != nil {
		// Log a warning message, do not return the error.
		p.logger.Warn("%v", err)
	}

	p.isClosed = true
	p.cachedToken = nil
	return nil
}

// checkClosed checks if the provider is closed.
func (p *AccessTokenProvider) checkClosed() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.isClosed
}

// getCachedToken looks for the token from cache and checks if the cached token
// is valid and needs to renew.
func (p *AccessTokenProvider) getCachedToken() (token *auth.Token, ok bool, needRenew bool) {
	if p.cachedToken == nil {
		return
	}

	token = p.cachedToken
	ok = !token.Expired()
	needRenew = token.NeedRefresh(p.expiryWindow)
	return
}

// login attempts to log into the server with user credentials supplied to the provider.
// If login succeeds this method returns an authorization string that contains
// an access token retrieved from server.
func (p *AccessTokenProvider) login() (auth string, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	token, err := p.doRequest(loginService, p.basicAuth)
	if err != nil {
		return
	}

	p.cachedToken = token
	return token.AuthString(), nil
}

// renewToken attempts to renew the token that currently in use.
func (p *AccessTokenProvider) renewToken() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	oldToken, ok, _ := p.getCachedToken()
	if !ok {
		return nil
	}

	token, err := p.doRequest(renewService, oldToken.AuthString())
	if err != nil {
		p.logger.Warn("%v", err)
		return err
	}

	p.cachedToken = token
	return nil
}

// logout attempts to logout the user to terminate the user session.
func (p *AccessTokenProvider) logout() error {
	token, ok, _ := p.getCachedToken()
	if !ok {
		return nil
	}

	_, err := p.doRequest(logoutService, token.AuthString())
	return err
}

// doRequest is a convenience method used to send HTTP requests to the specified
// service using the authorization string.
func (p *AccessTokenProvider) doRequest(service, auth string) (token *auth.Token, err error) {
	url := p.endpoint + service
	p.reqHeaders["Authorization"] = auth
	resp, err := httputil.DoRequest(context.Background(), p.httpClient, p.timeout, http.MethodGet, url, nil, p.reqHeaders, p.logger)
	if err != nil {
		return
	}

	if resp.Code != http.StatusOK {
		var action string
		switch service {
		case loginService:
			action = "login"
		case logoutService:
			action = "logout"
		case renewService:
			action = "renew access token"
		}

		return nil, nosqlerr.New(nosqlerr.InvalidAuthorization,
			"%s failed, HTTP status code: %d, response: %s", action, resp.Code, string(resp.Body))
	}

	if service == logoutService {
		return
	}

	token, err = parseTokenFromJSON(resp.Body)
	if err != nil {
		return nil, err
	}

	if token == nil {
		return nil, errors.New("got invalid access token")
	}

	return token, nil
}

// parseTokenFromJSON parses access token from the specified JSON.
//
// The specified JSON data must contain the required "token" field and an
// optional "expireAt" field, which represent the access token and expiry time
// of the token respectively.
func parseTokenFromJSON(data []byte) (*auth.Token, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	var accessToken string
	var expiresAt time.Time
	var ok bool

	accessToken, ok = jsonutil.GetStringFromObject(m, "token")
	if !ok {
		return nil, fmt.Errorf("cannot parse access token from JSON %q", string(data))
	}

	floatVal, ok := jsonutil.GetNumberFromObject(m, "expireAt")
	if ok {
		timeMs := int64(floatVal)
		expiresAt = time.Unix(0, timeMs*int64(time.Millisecond))
	}

	return auth.NewTokenWithExpiry(accessToken, auth.BearerToken, expiresAt), nil
}
