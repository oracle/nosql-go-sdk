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
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/stretchr/testify/suite"
)

type idcsTestSuite struct {
	suite.Suite
}

type testProviderInfo struct {
	idcsUrl       *string
	credsFile     *string
	entitlementId *string
	expectErr     bool
}

var testCasesForNewProvider = []*testProviderInfo{
	// nil provider info
	nil,
	// Valid IDCS properties.
	{
		idcsUrl:       stringPtr("https://idcsHost:443"),
		credsFile:     stringPtr("testdata/creds1.properties"),
		entitlementId: nil,
		expectErr:     false,
	},
	// Specify "entitlement_id" property.
	{
		idcsUrl:       stringPtr("https://idcsHost:443"),
		credsFile:     stringPtr("testdata/creds1.properties"),
		entitlementId: stringPtr("abcd-efgh"),
		expectErr:     false,
	},
	// Specify a non-exist file for "creds_file" property.
	{
		idcsUrl:       stringPtr("https://idcsHost:443"),
		credsFile:     stringPtr("testdata/notExists.properties"),
		entitlementId: nil,
		expectErr:     true,
	},
	// Do not specify "idcs_url" property.
	{
		idcsUrl:       nil,
		credsFile:     stringPtr("testdata/creds1.properties"),
		entitlementId: stringPtr("abcd-efgh"),
		expectErr:     true,
	},
	// Specify an empty "idcs_url" property.
	{
		idcsUrl:       stringPtr(""),
		credsFile:     stringPtr("testdata/creds1.properties"),
		entitlementId: stringPtr(""),
		expectErr:     true,
	},
}

type testProviderOptions struct {
	options       auth.ProviderOptions
	credsProvider CredentialsProvider
}

var testCasesForOptions = []*testProviderOptions{
	{
		options:       defaultOptions,
		credsProvider: nil,
	},
	{
		options: auth.ProviderOptions{
			Timeout:    8 * time.Second,
			Logger:     logger.New(nil, logger.Fine, true),
			HTTPClient: httputil.DefaultHTTPClient,
		},
		credsProvider: &mockCredsProvider{},
	},
}

func (suite *idcsTestSuite) TestNewProviderWithFile() {
	var p *AccessTokenProvider
	var err error
	var f string
	var msgPrefix string

	origCredsFile := defaultCredsFile
	// Reset defaultCredsFile to its original value on function return.
	defer func() {
		defaultCredsFile = origCredsFile
	}()

	for i, r := range testCasesForNewProvider {
		msgPrefix = fmt.Sprintf("Testcase %d: ", i+1)
		if r == nil {
			f = "testdata/NoSuchIDCSFile.properties"
		} else {
			props := createProp(r)
			f, err = createPropFile(props)
			if !suite.NoErrorf(err, msgPrefix+"failed to create idcs properties file %v", err) {
				continue
			}
			defer os.Remove(f)
		}

		p, err = NewAccessTokenProviderWithFile(f)
		if r == nil || r.expectErr {
			suite.Errorf(err, msgPrefix+"NewAccessTokenProviderWithFile(file=%s) "+
				"should have failed, but succeeded", f)
			continue
		}

		if suite.NoErrorf(err, msgPrefix+"NewAccessTokenProviderWithFile(file=%s) got error: %v", f, err) {
			suite.checkProvider(p, r, nil, msgPrefix)
		}

		// Test provider options.
		for j, o := range testCasesForOptions {
			msgPrefix = fmt.Sprintf("Testcase %d-%d: ", i+1, j+1)
			p, err = NewAccessTokenProviderWithFile(f, o.options)
			if o.credsProvider != nil {
				p.SetCredentialsProvider(o.credsProvider)
			}

			if suite.NoErrorf(err, msgPrefix+"NewAccessTokenProviderWithFile(options=%v) got error: %v", o.options, err) {
				suite.checkProvider(p, r, o, msgPrefix)
			}
		}
	}
}

func (suite *idcsTestSuite) TestNewProviderWithURL() {
	var p *AccessTokenProvider
	var err error

	origCredsFile := defaultCredsFile
	// Reset defaultCredsFile to its original value on function return.
	defer func() {
		defaultCredsFile = origCredsFile
	}()

	for i, r := range testCasesForNewProvider {
		if r == nil || r.idcsUrl == nil {
			continue
		}
		// NewAccessTokenProviderWithURL() cannot specify an entitlementId
		if r.entitlementId != nil {
			continue
		}

		msgPrefix := fmt.Sprintf("Testcase %d: ", i+1)
		// Change default credentials file.
		if r.credsFile != nil {
			defaultCredsFile = *r.credsFile
		} else {
			defaultCredsFile = "testdata/NoSuchCredsFile.properties"
		}
		p, err = NewAccessTokenProviderWithURL(*r.idcsUrl)
		if r.expectErr {
			suite.Errorf(err, msgPrefix+"NewAccessTokenProviderWithURL(idcsURL=%s) "+
				"should have failed, but succeeded", *r.idcsUrl)
			continue
		}

		if suite.NoErrorf(err, msgPrefix+"NewAccessTokenProviderWithURL(idcsURL=%s) got error: %v", *r.idcsUrl, err) {
			suite.checkProvider(p, r, nil, msgPrefix)
		}

		// Test provider options.
		for j, o := range testCasesForOptions {
			msgPrefix = fmt.Sprintf("Testcase %d-%d: ", i+1, j+1)
			p, err = NewAccessTokenProviderWithURL(*r.idcsUrl, o.options)
			if o.credsProvider != nil {
				p.SetCredentialsProvider(o.credsProvider)
			}

			if suite.NoErrorf(err, msgPrefix+"NewAccessTokenProviderWithURL(options=%v) got error: %v", o.options, err) {
				suite.checkProvider(p, r, o, msgPrefix)
			}
		}
	}
}

func (suite *idcsTestSuite) TestNewProviderWithEntitlementId() {
	var p *AccessTokenProvider
	var err error

	origCredsFile := defaultCredsFile
	defer func() {
		defaultCredsFile = origCredsFile
	}()

	for i, r := range testCasesForNewProvider {
		if r == nil || r.idcsUrl == nil || r.entitlementId == nil {
			continue
		}

		msgPrefix := fmt.Sprintf("Testcase %d: ", i+1)
		// Change default credentials file.
		if r.credsFile != nil {
			defaultCredsFile = *r.credsFile
		} else {
			defaultCredsFile = "testdata/NoSuchCredsFile.properties"
		}
		p, err = NewAccessTokenProviderWithEntitlementId(*r.idcsUrl, *r.entitlementId)
		if r.expectErr {
			suite.Errorf(err, msgPrefix+"NewAccessTokenProviderWithEntitlementId() should have failed, but succeeded")
			continue
		}

		if suite.NoErrorf(err, msgPrefix+"NewAccessTokenProviderWithEntitlementId() got error: %v", err) {
			suite.checkProvider(p, r, nil, msgPrefix)
		}

		// Test WithXXX functional options
		for j, o := range testCasesForOptions {
			msgPrefix = fmt.Sprintf("Testcase %d-%d: ", i+1, j+1)
			p, err = NewAccessTokenProviderWithEntitlementId(*r.idcsUrl, *r.entitlementId, o.options)
			if o.credsProvider != nil {
				p.SetCredentialsProvider(o.credsProvider)
			}

			if suite.NoErrorf(err,
				msgPrefix+"NewAccessTokenProviderWithEntitlementId(options=%v) got error: %v", o.options, err) {
				suite.checkProvider(p, r, o, msgPrefix)
			}
		}
	}
}

func (suite *idcsTestSuite) checkProvider(p *AccessTokenProvider,
	cfg *testProviderInfo, opt *testProviderOptions, msgPrefix string) {
	// Check andcFQS
	expAndcFQS := ""
	if cfg.entitlementId != nil && len(*cfg.entitlementId) > 0 {
		expAndcFQS = andcAudiencePrefix + *cfg.entitlementId + andcScope
	}
	suite.Equalf(expAndcFQS, p.andcFQS, msgPrefix+"got unexpected FQS of ANDC application.")

	// Check idcsURL
	if cfg.idcsUrl != nil {
		suite.Equalf(*cfg.idcsUrl, p.idcsURL, msgPrefix+"got unexpected IDCS URL.")
	}

	if opt == nil {
		return
	}

	// Check timeout value.
	expTimeout := defaultOptions.Timeout
	if opt.options.Timeout != 0 {
		expTimeout = opt.options.Timeout
	}
	suite.Equalf(expTimeout, p.timeout, msgPrefix+"got unexpected request timeout.")

	// Check CredentialsProvider.
	if opt.credsProvider != nil {
		suite.Equalf(opt.credsProvider, p.credsProvider, msgPrefix+"got unexpected CredentialsProvider.")
	}

	// Check logger.
	expLogger := defaultOptions.Logger
	if opt.options.Logger != nil {
		expLogger = opt.options.Logger
	}
	suite.Equalf(expLogger, p.logger, msgPrefix+"got unexpected logger.")

	// Check http client.
	expHttpClient := defaultOptions.HTTPClient
	if opt.options.HTTPClient != nil {
		expHttpClient = opt.options.HTTPClient
	}
	suite.Equalf(expHttpClient, p.httpClient, msgPrefix+"got unexpected httpClient.")
}

func stringPtr(s string) *string {
	return &s
}

func createProp(cfg *testProviderInfo) (props map[string]string) {
	props = make(map[string]string, 0)
	if cfg.idcsUrl != nil {
		props[idcsURLProp] = *cfg.idcsUrl
	}

	if cfg.credsFile != nil {
		props[credsFileProp] = *cfg.credsFile
	}

	if cfg.entitlementId != nil {
		props[entitlementIdProp] = *cfg.entitlementId
	}

	return
}

func createPropFile(props map[string]string) (string, error) {
	var buf bytes.Buffer
	for k, v := range props {
		buf.WriteString(k + "=" + v + "\n")
	}

	f, err := ioutil.TempFile("testdata", "test-idcs.properties.*~")
	if err != nil {
		return "", err
	}
	defer f.Close()

	err = ioutil.WriteFile(f.Name(), buf.Bytes(), os.FileMode(0600))
	return f.Name(), err
}

func TestIDCS(t *testing.T) {
	suite.Run(t, new(idcsTestSuite))
}
