// Copyright (c) 2016, 2021 Oracle and/or its affiliates. All rights reserved.
// This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

package iam

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"os"
	"path"
)

const (
	// supported version for resource principal
	resourcePrincipalVersion2_2 = "2.2"

	// environment variable that specifies a resource principal version
	resourcePrincipalVersionEnvVar = "OCI_RESOURCE_PRINCIPAL_VERSION"

	// environment variable that specifies a security token or a path to the token file
	resourcePrincipalRPSTEnvVar = "OCI_RESOURCE_PRINCIPAL_RPST"

	// environment variable that specifies an RSA private key in pem format or a path to the key file
	resourcePrincipalPrivatePEMEnvVar = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM"

	// environment variable that specifies the passphrase to a key or a path to the passphrase file
	resourcePrincipalPrivatePEMPassphraseEnvVar = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE"

	// environment variable that specifies a region
	resourcePrincipalRegionEnvVar = "OCI_RESOURCE_PRINCIPAL_REGION"

	// tenancyOCIDClaimKey is the key used to look up the resource tenancy in an RPST
	tenancyOCIDClaimKey = "res_tenant"

	errMsgEnvVarNotPresent = "can not create resource principal, environment variable: %s, not present"
)

// newResourcePrincipalConfigurationProvider creates a resource principal
// configuration provider using well known environment variables to look up
// token information. The environment variables can either paths or contain the
// material value of the keys. However in the case of the keys and tokens paths
// and values can not be mixed.
func newResourcePrincipalConfigurationProvider() (ConfigurationProvider, error) {
	var version string
	var ok bool
	if version, ok = os.LookupEnv(resourcePrincipalVersionEnvVar); !ok {
		return nil, fmt.Errorf(errMsgEnvVarNotPresent, resourcePrincipalVersionEnvVar)
	}

	switch version {
	case resourcePrincipalVersion2_2:
		rpst := requireEnv(resourcePrincipalRPSTEnvVar)
		if rpst == nil {
			return nil, fmt.Errorf(errMsgEnvVarNotPresent, resourcePrincipalRPSTEnvVar)
		}
		private := requireEnv(resourcePrincipalPrivatePEMEnvVar)
		if private == nil {
			return nil, fmt.Errorf(errMsgEnvVarNotPresent, resourcePrincipalPrivatePEMEnvVar)
		}
		// passphrase is optional
		passphrase := requireEnv(resourcePrincipalPrivatePEMPassphraseEnvVar)
		region := requireEnv(resourcePrincipalRegionEnvVar)
		if region == nil {
			return nil, fmt.Errorf(errMsgEnvVarNotPresent, resourcePrincipalRegionEnvVar)
		}
		return newResourcePrincipalKeyProvider22(*rpst, *private, passphrase, *region)
	default:
		return nil, fmt.Errorf("can not create resource principal, environment variable: %s, must be valid",
			resourcePrincipalVersionEnvVar)
	}
}

func requireEnv(key string) *string {
	if val, ok := os.LookupEnv(key); ok {
		return &val
	}
	return nil
}

// resourcePrincipalKeyProvider is a key provider that reads from the specified
// environment variables. The environment variables can host the material
// keys/passphrases or they can be paths to files that need to be read.
type resourcePrincipalKeyProvider struct {
	FederationClient  federationClient
	KeyProviderRegion string
}

func newResourcePrincipalKeyProvider22(sessionTokenLocation, privatePemLocation string,
	passphraseLocation *string, region string) (*resourcePrincipalKeyProvider, error) {

	//Check both the the passphrase and the key are paths
	if passphraseLocation != nil && (!isPath(privatePemLocation) && isPath(*passphraseLocation) ||
		isPath(privatePemLocation) && !isPath(*passphraseLocation)) {
		return nil, fmt.Errorf("can not create resource principal: both key and passphrase need to be path or none needs to be path")
	}

	var supplier sessionKeySupplier
	var err error

	//File based case
	if isPath(privatePemLocation) {
		supplier, err = newFileBasedKeySessionSupplier(privatePemLocation, passphraseLocation)
		if err != nil {
			return nil, fmt.Errorf("can not create resource principal, due to: %s ", err.Error())
		}
	} else {
		//else the content is in the env vars
		var passphrase []byte
		if passphraseLocation != nil {
			passphrase = []byte(*passphraseLocation)
		}
		supplier, err = newStaticKeySessionSupplier([]byte(privatePemLocation), passphrase)
		if err != nil {
			return nil, fmt.Errorf("can not create resource principal, due to: %s ", err.Error())
		}
	}

	var fd federationClient
	if isPath(sessionTokenLocation) {
		fd, _ = newFileBasedFederationClient(sessionTokenLocation, supplier)
	} else {
		fd, err = newStaticFederationClient(sessionTokenLocation, supplier)
		if err != nil {
			return nil, fmt.Errorf("can not create resource principal, due to: %s ", err.Error())
		}
	}

	rs := resourcePrincipalKeyProvider{
		FederationClient:  fd,
		KeyProviderRegion: region,
	}
	return &rs, nil
}

func (p *resourcePrincipalKeyProvider) PrivateRSAKey() (privateKey *rsa.PrivateKey, err error) {
	if privateKey, err = p.FederationClient.PrivateKey(); err != nil {
		err = fmt.Errorf("failed to get private key: %s", err.Error())
		return nil, err
	}
	return privateKey, nil
}

func (p *resourcePrincipalKeyProvider) KeyID() (string, error) {
	var securityToken string
	var err error
	if securityToken, err = p.FederationClient.SecurityToken(); err != nil {
		return "", fmt.Errorf("failed to get security token: %s", err.Error())
	}
	return fmt.Sprintf("ST$%s", securityToken), nil
}

func (p *resourcePrincipalKeyProvider) Region() (string, error) {
	return p.KeyProviderRegion, nil
}

var (
	// errNonStringClaim is returned if the token has a claim for a key, but it's not a string value
	errNonStringClaim = errors.New("claim does not have a string value")
)

func (p *resourcePrincipalKeyProvider) TenancyOCID() (string, error) {
	if claim, err := p.FederationClient.GetClaim(tenancyOCIDClaimKey); err != nil {
		return "", err
	} else if tenancy, ok := claim.(string); ok {
		return tenancy, nil
	} else {
		return "", errNonStringClaim
	}
}

func (p *resourcePrincipalKeyProvider) KeyFingerprint() (string, error) {
	return "", nil
}

func (p *resourcePrincipalKeyProvider) UserOCID() (string, error) {
	return "", nil
}

// By contract for the the content of a resource principal to be considered path, it needs to be
// an absolute path.
func isPath(str string) bool {
	return path.IsAbs(str)
}
