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
	"path/filepath"
	"sync"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
)

const (
	// Property name of IDCS account user.
	usernameProp = "andc_username"

	// Property name of IDCS account password.
	passwordProp = "andc_user_pwd"

	// Property name of OAuth client id.
	clientIdProp = "andc_client_id"

	// Property name of OAuth client secret.
	clientSecretProp = "andc_client_secret"
)

// The default credentials file ~/.andc/credentials
var defaultCredsFile = filepath.Join(userHomeDir(), ".andc", "credentials")

// IDCSCredentials is used to identify a user or OAuth client.
// It contains an alias and a secret used to authenticate with IDCS to acquire access tokens.
type IDCSCredentials struct {
	// Alias specifies a username or OAuth client id.
	Alias string

	// Secret specifies a user password or OAuth client secret.
	Secret []byte
}

// CredentialsProvider is an interface that provides credentials required to
// authenticate with IDCS. It provides two types of credentials:
//
//   1. User credentials that encapsulates username and password.
//   2. Client credentials that encapsulates OAuth client id and secret.
//
// These credentials are used by the AccessTokenProvider to acquire access tokens from IDCS.
type CredentialsProvider interface {
	// OAuthClientCredentials returns an IDCSCredentials that contains an
	// OAuth client id and a secret.
	OAuthClientCredentials() (IDCSCredentials, error)

	// UserCredentials returns an IDCSCredentials that contains a username
	// and a password.
	UserCredentials() (IDCSCredentials, error)
}

// PropertiesCredentialsProvider provides credentials read from a properties file,
// in which one credential is specified in a single line of the form:
//
//   propertyName=propertyvalue
//
// The supported property names are case-sensitive and listed as follows:
//
// 1. User Credentials:
//
//   andc_username
//   andc_user_pwd
//
// 2. OAuth client credentials:
//
//   andc_client_id
//   andc_client_secret
//
// This implements the CredentialsProvider interface.
type PropertiesCredentialsProvider struct {
	*sdkutil.Properties
	once sync.Once
}

// NewPropertiesCredentialsProvider creates a PropertiesCredentialsProvider with
// credentials from the default properties file ~/.andc/credentials.
func NewPropertiesCredentialsProvider() (*PropertiesCredentialsProvider, error) {
	return NewPropertiesCredentialsProviderWithFile(defaultCredsFile)
}

// NewPropertiesCredentialsProviderWithFile creates a PropertiesCredentialsProvider
// with credentials from the specified properties file.
func NewPropertiesCredentialsProviderWithFile(file string) (*PropertiesCredentialsProvider, error) {
	props, err := sdkutil.NewProperties(file)
	if err != nil {
		return nil, err
	}

	return &PropertiesCredentialsProvider{Properties: props}, nil
}

// init loads properties from the properties file.
func (p *PropertiesCredentialsProvider) init() error {
	p.once.Do(p.Load)
	return p.Err()
}

// OAuthClientCredentials gets the "andc_client_id" and "andc_client_secret"
// properties from the properties file, returns as an IDCSCredentials.
func (p *PropertiesCredentialsProvider) OAuthClientCredentials() (creds IDCSCredentials, err error) {
	if err = p.init(); err != nil {
		return
	}

	id, err := p.Get(clientIdProp)
	if err != nil {
		return
	}

	secret, err := p.Get(clientSecretProp)
	if err != nil {
		return
	}

	creds.Alias = id
	creds.Secret = make([]byte, len(secret))
	copy(creds.Secret, secret)
	return
}

// UserCredentials gets the "andc_username" and "andc_user_pwd" properties from
// the properties file, returns as an IDCSCredentials.
func (p *PropertiesCredentialsProvider) UserCredentials() (creds IDCSCredentials, err error) {
	if err = p.init(); err != nil {
		return
	}

	username, err := p.Get(usernameProp)
	if err != nil {
		return
	}

	password, err := p.Get(passwordProp)
	if err != nil {
		return
	}

	creds.Alias = username
	creds.Secret = make([]byte, len(password))
	copy(creds.Secret, password)
	return
}
