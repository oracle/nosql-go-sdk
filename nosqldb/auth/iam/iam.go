//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package iam provides authorization provider implementations for clients
// that connect to cloud via IAM (Oracle Identity and Access Management).
package iam

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
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

	// the configuration provider
	configProvider ConfigurationProvider

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

// NewSignatureProvider creates a signature provider using the "DEFAULT"
// profile specified in the default OCI configuration file ~/.oci/config.
// See https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm
// for details of the configuration file's contents and format.
//
// This signature provider uses the tenancyOCID that is the "tenancy" field
// specified in the configuration file as compartmentID.
func NewSignatureProvider() (*SignatureProvider, error) {
	return NewSignatureProviderFromFile("~/.oci/config", "", "", "")
}

// NewSignatureProviderFromFile creates a signature provider using the ociProfile
// specified in the OCI configuration file configFilePath.
// See https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm
// for details of the configuration file's contents and format.
//
// ociProfile is optional; if empty, "DEFAULT" will be used.
//
// privateKeyPassphrase is only required if the private key uses a passphrase and
// it is not specified in the "pass_phrase" field in the OCI configuration file.
//
// compartmentID is optional; if empty, the tenancyOCID is used in its place.
// If specified, it represents a compartment id or name.
// If using a nested compartment, specify the full compartment path relative to
// the root compartment as compartmentID.
// For example, if using rootCompartment.compartmentA.compartmentB, the
// compartmentID should be set to compartmentA.compartmentB.
func NewSignatureProviderFromFile(configFilePath, ociProfile, privateKeyPassphrase, compartmentID string) (*SignatureProvider, error) {

	// default to OCI "DEFAULT" if none given
	if ociProfile == "" {
		ociProfile = "DEFAULT"
	}

	// open/read creds config file
	// note: if privateKeyPassphrase=="", it will be read from "pass_phrase" in
	//       the config file (if needed)
	configProvider, err := ConfigurationProviderFromFileWithProfile(configFilePath, ociProfile, privateKeyPassphrase)
	if configProvider == nil {
		return nil, err
	}
	// validate all fields in the file
	ok, err := IsConfigurationProviderValid(configProvider)
	if ok == false {
		return nil, err
	}

	return NewSignatureProviderWithConfiguration(configProvider, compartmentID)
}

// NewRawSignatureProvider creates a signature provider based on the raw
// credentials given (no files necessary).
//
// privateKeyPassphrase is only required if the private key uses a passphrase.
//
// compartmentID is optional; if empty, the tenancyOCID is used in its place.
//
// privateKeyOrFile specifies the private key or full path to the private key file.
func NewRawSignatureProvider(tenancy, user, region, fingerprint, compartmentID, privateKeyOrFile string, privateKeyPassphrase *string) (*SignatureProvider, error) {

	privateKey := privateKeyOrFile
	if file, ok := fileExists(privateKeyOrFile); ok {
		pemData, err := ioutil.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("cannot read private key file %s: %v", file, err)
		}
		privateKey = string(pemData)
	}

	configProvider := NewRawConfigurationProvider(tenancy, user, region, fingerprint, privateKey, privateKeyPassphrase)

	// validate all required fields are provided
	ok, err := IsConfigurationProviderValid(configProvider)
	if ok == false {
		return nil, err
	}

	return NewSignatureProviderWithConfiguration(configProvider, compartmentID)
}

// NewSignatureProviderWithResourcePrincipal creates a signature provider with
// resource principal. This can be used for applications that access NoSQL cloud
// service from within a function that executes on Oracle Functions.
//
// The compartmentID specifies the OCID of compartment to which the Oracle
// NoSQL tables belong. If empty, the tenancy OCID is used.
//
// Resource principal is configured using the following environment variables:
//
//   OCI_RESOURCE_PRINCIPAL_VERSION
//   OCI_RESOURCE_PRINCIPAL_RPST
//   OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM
//   OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE
//   OCI_RESOURCE_PRINCIPAL_REGION
//
// Where OCI_RESOURCE_PRINCIPAL_VERSION specifies a resource principal version.
// Current version is 2.2.
//
// OCI_RESOURCE_PRINCIPAL_RPST specifies a resource principal session token or
// a path to the file that stores the token.
//
// OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM specifies an RSA private key in pem format
// or a path to private key file.
//
// OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE specifies a passphrase for the
// private key or a path to the file that stores the passphrase.
// This is optional, only required if the private key has a passphrase.
//
// OCI_RESOURCE_PRINCIPAL_REGION specifies an OCI region identifier.
//
// Note that if your application is deployed to Oracle Functions, these
// environment variables are already set inside the container in which the
// function executes.
func NewSignatureProviderWithResourcePrincipal(compartmentID string) (*SignatureProvider, error) {
	configProvider, err := newResourcePrincipalConfigurationProvider()
	if err != nil {
		return nil, err
	}

	return NewSignatureProviderWithConfiguration(configProvider, compartmentID)
}

// NewSignatureProviderWithInstancePrincipal creates a signature provider with
// instance principal. This can be used for applications that access NoSQL cloud
// service from within an Oracle Compute Instance.
//
// The compartmentID specifies the OCID of compartment to which the Oracle
// NoSQL tables belong. If empty, the tenancy OCID is used.
func NewSignatureProviderWithInstancePrincipal(compartmentID string) (*SignatureProvider, error) {
	configProvider, err := newInstancePrincipalConfigurationProvider()
	if err != nil {
		return nil, err
	}

	return NewSignatureProviderWithConfiguration(configProvider, compartmentID)
}

// NewSignatureProviderWithConfiguration creates a signature provider with
// the supplied configuration.
//
// The compartmentID specifies the OCID of compartment to which the Oracle
// NoSQL tables belong. If empty, the tenancy OCID is used.
//
// This function can be used in the following cases:
//
// 1. If other NewSignatureProviderXXX() variants declared in the package do
// not meet application requirements, you can provide an implementation of the
// ConfigurationProvider interface and create a signature provider with it.
//
// 2. If your application uses OCI-GO-SDK as a dependency, you can use one of
// the ConfigurationProvider implementations from OCI-GO-SDK and create a
// signature provider with it.
func NewSignatureProviderWithConfiguration(configProvider ConfigurationProvider, compartmentID string) (*SignatureProvider, error) {

	var err error
	// use the tenancy if compartmentID is not provided.
	if compartmentID == "" {
		compartmentID, err = configProvider.TenancyOCID()
		if err != nil {
			return nil, err
		}
	}

	// we currently don't sign the -body- of the requests
	signer := RequestSignerExcludeBody(configProvider)

	// create a new signature every 5 minutes
	expiryInterval, _ := time.ParseDuration("5m")

	p := &SignatureProvider{
		signature:              "",
		signatureExpiresAt:     time.Now(),
		signatureFormattedDate: "",
		expiryInterval:         expiryInterval,
		compartmentID:          compartmentID,
		signer:                 signer,
		configProvider:         configProvider,
	}

	return p, nil
}

// fileExists checks if a file exists and is not a directory.
// It returns a possibly expanded file path and a bool flag representing
// if the file exists.
func fileExists(file string) (string, bool) {
	expandedPath, err := sdkutil.ExpandPath(file)
	if err != nil {
		return file, false
	}

	info, err := os.Stat(expandedPath)
	if os.IsNotExist(err) || info.IsDir() {
		return expandedPath, false
	}

	return expandedPath, true
}

// Profile returns the profile used for the signature provider.
func (p *SignatureProvider) Profile() ConfigurationProvider {
	return p.configProvider
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
	p.signatureFormattedDate = now.UTC().Format(http.TimeFormat)
	req.Header.Set(requestHeaderDate, p.signatureFormattedDate)
	err := p.signer.Sign(req)
	if err != nil {
		return err
	}
	p.signature = req.Header.Get(requestHeaderAuthorization)
	p.signatureExpiresAt = now.Add(p.expiryInterval)

	return nil
}

// Close releases resources allocated by the provider and sets closed state for the provider.
// Currently nothing to release
func (p *SignatureProvider) Close() error {
	return nil
}
