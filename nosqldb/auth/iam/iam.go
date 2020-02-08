//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// Package iam provides authorization provider implementations for clients
// that connect to cloud via IAM
package iam

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
)

const (
	requestHeaderDate                = "Date"
	requestHeaderAuthorization       = "Authorization"
	requestHeaderXNoSQLCompartmentID = "X-Nosql-Compartment-Id"
)

// SignatureProvider is an signature provider for use with cloud IAM.
//
// This implements the nosqldb.AuthorizationProvider interface.
type SignatureProvider struct {

	// the signer we use to sign each request
	signer HTTPRequestSigner

	// we need this for default compartmentID
	compartmentID string

	// cached signature string
	signature string

	// date of last signature string generation, in RFC1123 format
	signatureFormattedDate string

	// interval for new signature generations
	expiryInterval time.Duration

	// next time the signature expires
	signatureExpiresAt time.Time

	// lock for updating cached signatures
	mutex sync.RWMutex
}

// NewSignatureProvider creates a signature provider based on the contents of
// the OCI IAM credentials file passed in.
//
// ociProfile is optional; if empty, "DEFAULT" will be used.
//
// privateKeyPassword is only required if the private key uses a password and
// it is not specified in the "pass_phrase" field in the IAM config file.
//
// compartmentID is optional; if empty, the tenancyOCID is used in its place.
//
func NewSignatureProvider(configFilePath, ociProfile, privateKeyPassword, compartmentID string) (*SignatureProvider, error) {

	// default to OCI "DEFAULT" if none given
	if ociProfile == "" {
		ociProfile = "DEFAULT"
	}

	// open/read creds config file
	// note: if privateKeyPassword=="", it will be read from "pass_phrase" in
	//       the config file (if needed)
	configProvider, err := ConfigurationProviderFromFileWithProfile(configFilePath, ociProfile, privateKeyPassword)
	if configProvider == nil {
		return nil, err
	}
	// validate all fields in the file
	ok, err := IsConfigurationProviderValid(configProvider)
	if ok == false {
		return nil, err
	}

	// the default compartmentID is the tenancyID
	if compartmentID == "" {
		compartmentID, _ = configProvider.TenancyOCID()
	}

	// we currently don't sign the -body- of the requests
	signer := RequestSignerExcludeBody(configProvider)
	if signer == nil {
		return nil, fmt.Errorf("can't create request signer")
	}

	// create a new signature every 5 minutes
	expiryInterval, _ := time.ParseDuration("5m")

	p := &SignatureProvider{
		signature:              "",
		signatureExpiresAt:     time.Now(),
		signatureFormattedDate: "",
		expiryInterval:         expiryInterval,
		compartmentID:          compartmentID,
		signer:                 signer,
	}

	return p, nil
}


// NewRawSignatureProvider creates a signature provider based on the raw
// credentials given (no files necessary).
//
// privateKeyPassword is only required if the private key uses a password.
//
// compartmentID is optional; if empty, the tenancyOCID is used in its place.
//
func NewRawSignatureProvider(tenancy, user, region, fingerprint, compartmentID, privateKey string, privateKeyPassphrase *string) (*SignatureProvider, error) {

	configProvider := NewRawConfigurationProvider(tenancy, user, region, fingerprint, privateKey, privateKeyPassphrase);

	// validate all fields in the file
	ok, err := IsConfigurationProviderValid(configProvider)
	if ok == false {
		return nil, err
	}

	// the default compartmentID is the tenancyID
	if compartmentID == "" {
		compartmentID, _ = configProvider.TenancyOCID()
	}

	// we currently don't sign the -body- of the requests
	signer := RequestSignerExcludeBody(configProvider)
	if signer == nil {
		return nil, fmt.Errorf("can't create request signer")
	}

	// create a new signature every 5 minutes
	expiryInterval, _ := time.ParseDuration("5m")

	p := &SignatureProvider{
		signature:              "",
		signatureExpiresAt:     time.Now(),
		signatureFormattedDate: "",
		expiryInterval:         expiryInterval,
		compartmentID:          compartmentID,
		signer:                 signer,
	}

	return p, nil
}
// AuthorizationScheme returns "Signature" for this provider which means the requests
// must be signed before sending out
func (p *SignatureProvider) AuthorizationScheme() string {
	return auth.Signature
}

// AuthorizationString isn't used for IAM; instead, each individual request is
// signed via SignHTTPRequest()
func (p *SignatureProvider) AuthorizationString(req auth.Request) (auth string, err error) {
	return "", nil
}

// SignHTTPRequest signs the request, add the signature to the Authentication: header, add
// the Date: header, and add the "X-Nosql-Compartment-Id" header
//
// The Authorization header looks like:
//
//   Signature version=n,headers=<>,keyId=<>,algorithm="rsa-sha256",signature="..."
//
// This method uses the cached signature if it was generated within the expiry time
// specified in signatureExpiry. Else it gets the current date/time and uses that to
// generate a new signature.
func (p *SignatureProvider) SignHTTPRequest(req *http.Request) error {

	// no matter what, we set the compartmentID in the header
	req.Header.Set(requestHeaderXNoSQLCompartmentID, p.compartmentID)

	// use cached signature and date, if not expired
	now := time.Now()
	if p.signature != "" && p.signatureExpiresAt.After(now) {
		p.mutex.RLock()
		defer p.mutex.RUnlock()
		req.Header.Set(requestHeaderDate, p.signatureFormattedDate)
		req.Header.Set(requestHeaderAuthorization, p.signature)
		return nil
	}

	// calculate new signature
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.signatureFormattedDate = now.Format(time.RFC1123)
	req.Header.Set(requestHeaderDate, p.signatureFormattedDate)
	p.signer.Sign(req)
	p.signature = req.Header.Get(requestHeaderAuthorization)
	p.signatureExpiresAt = now.Add(p.expiryInterval)

	return nil
}

// Close releases resources allocated by the provider and sets closed state for the provider.
// Currently nothing to release
func (p *SignatureProvider) Close() error {
	return nil
}
