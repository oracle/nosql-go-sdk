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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

// CredsProviderTestSuite contains tests for the PropertiesCredentialsProvider.
type CredsProviderTestSuite struct {
	suite.Suite
}

func (suite *CredsProviderTestSuite) TestNewCredentialsProvider() {
	files := []string{"", "~", "testdata", "testdata/not_exists_creds"}
	for i, f := range files {
		_, err := NewPropertiesCredentialsProviderWithFile(f)
		suite.Errorf(err, "Testcase %d: NewPropertiesCredentialsProviderWithFile(file=%q) "+
			"should have failed, but succeeded.", i+1, f)
	}
}

func (suite *CredsProviderTestSuite) TestOKGetCredentials() {
	// testdata/creds.properties:
	//
	// andc_username=testUser1
	// andc_user_pwd=Welcome12%%3!
	// andc_client_id=abcdefg
	// andc_client_secret=93ef-78bc-42ad-66ff
	//
	userCreds := &IDCSCredentials{"testUser1", []byte("Welcome12%%3!")}
	clientCreds := &IDCSCredentials{"abcdefg", []byte("93ef-78bc-42ad-66ff")}

	files := []string{"testdata/creds1.properties", "testdata/creds2.properties"}
	for _, f := range files {
		suite.doGetCredsTest(f, userCreds, clientCreds)
	}
}

func (suite *CredsProviderTestSuite) TestBadGetCredentials() {
	userCreds := &IDCSCredentials{"testUser1", []byte("Welcome12%%3!")}
	clientCreds := &IDCSCredentials{"abcdefg", []byte("93ef-78bc-42ad-66ff")}
	testCases := []struct {
		userCreds   *IDCSCredentials
		clientCreds *IDCSCredentials
	}{
		{nil, clientCreds},
		{userCreds, nil},
		{nil, nil},
	}

	for _, r := range testCases {
		file, err := suite.createCredsFile(r.userCreds, r.clientCreds)
		if !suite.NoError(err) {
			continue
		}
		defer os.Remove(file)

		suite.doGetCredsTest(file, r.userCreds, r.clientCreds)
	}
}

func (suite *CredsProviderTestSuite) createCredsFile(userCreds, clientCreds *IDCSCredentials) (string, error) {
	var buf bytes.Buffer
	if userCreds != nil {
		buf.WriteString(usernameProp + "=" + userCreds.Alias + "\n")
		buf.WriteString(passwordProp + "=" + string(userCreds.Secret) + "\n")
	}
	if clientCreds != nil {
		buf.WriteString(clientIdProp + "=" + clientCreds.Alias + "\n")
		buf.WriteString(clientSecretProp + "=" + string(clientCreds.Secret) + "\n")
	}

	f, err := ioutil.TempFile("testdata", "test-creds.*~")
	if err != nil {
		return "", err
	}
	defer f.Close()

	err = ioutil.WriteFile(f.Name(), buf.Bytes(), os.FileMode(0600))
	return f.Name(), err
}

func (suite *CredsProviderTestSuite) doGetCredsTest(file string, expectUserCreds, expectClientCreds *IDCSCredentials) {
	p, err := NewPropertiesCredentialsProviderWithFile(file)
	if !suite.NoErrorf(err, "NewPropertiesCredentialsProviderWithFile(file=%q) got error %v.", file, err) {
		return
	}

	creds, err := p.UserCredentials()
	switch {
	case expectUserCreds != nil:
		if suite.NoErrorf(err, "UserCredentials() failed, got error: %v", err) {
			suite.Equalf(*expectUserCreds, creds, "got unexpected user credentials.")
		}

	default:
		suite.Errorf(err, "UserCredentials() should have failed")
	}

	creds, err = p.OAuthClientCredentials()
	switch {
	case expectClientCreds != nil:
		if suite.NoErrorf(err, "OAuthClientCredentials() failed, got error: %v", err) {
			suite.Equalf(*expectClientCreds, creds, "got unexpected client credentials.")
		}

	default:
		suite.Errorf(err, "OAuthClientCredentials() should have failed")
	}
}

func TestCredsProvider(t *testing.T) {
	suite.Run(t, new(CredsProviderTestSuite))
}
