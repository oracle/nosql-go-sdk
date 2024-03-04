// Copyright (c) 2016, 2023 Oracle and/or its affiliates. All rights reserved.
// This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

package iam

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
)

// federationClient is a client to retrieve the security token for an instance
// principal necessary to sign a request. It also provides the private key whose
// corresponding public key is used to retrieve the security token.
type federationClient interface {
	claimHolder
	PrivateKey() (*rsa.PrivateKey, error)
	SecurityToken() (string, error)
	ExpirationTime() time.Time
}

// claimHolder is implemented by any token interface that provides access to the
// security claims embedded in the token.
type claimHolder interface {
	GetClaim(key string) (interface{}, error)
}

type genericFederationClient struct {
	SessionKeySupplier   sessionKeySupplier
	RefreshSecurityToken func() (securityToken, error)

	securityToken securityToken
	mux           sync.Mutex
}

var _ federationClient = &genericFederationClient{}

func (c *genericFederationClient) PrivateKey() (*rsa.PrivateKey, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.renewKeyAndSecurityTokenIfNotValid(); err != nil {
		return nil, err
	}
	return c.SessionKeySupplier.PrivateKey(), nil
}

func (c *genericFederationClient) SecurityToken() (token string, err error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err = c.renewKeyAndSecurityTokenIfNotValid(); err != nil {
		return "", err
	}
	return c.securityToken.String(), nil
}

func (c *genericFederationClient) ExpirationTime() time.Time {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.renewKeyAndSecurityTokenIfNotValid(); err != nil {
		return time.Now().Add(-time.Second)
	}
	return c.securityToken.ExpirationTime()
}

func (c *genericFederationClient) renewKeyAndSecurityTokenIfNotValid() (err error) {
	if c.securityToken == nil || !c.securityToken.Valid() {
		if err = c.renewKeyAndSecurityToken(); err != nil {
			return fmt.Errorf("failed to renew security token: %s", err.Error())
		}
	}
	return nil
}

func (c *genericFederationClient) renewKeyAndSecurityToken() (err error) {
	if err = c.SessionKeySupplier.Refresh(); err != nil {
		return fmt.Errorf("failed to refresh session key: %s", err.Error())
	}

	if c.securityToken, err = c.RefreshSecurityToken(); err != nil {
		return fmt.Errorf("failed to refresh security token: %s", err.Error())
	}

	return nil
}

func (c *genericFederationClient) GetClaim(key string) (interface{}, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.renewKeyAndSecurityTokenIfNotValid(); err != nil {
		return nil, err
	}
	return c.securityToken.GetClaim(key)
}

func newFileBasedFederationClient(securityTokenPath string, supplier sessionKeySupplier) (*genericFederationClient, error) {
	return &genericFederationClient{
		SessionKeySupplier: supplier,
		RefreshSecurityToken: func() (token securityToken, err error) {
			var content []byte
			if content, err = os.ReadFile(securityTokenPath); err != nil {
				return nil, fmt.Errorf("failed to read security token from :%s. Due to: %s", securityTokenPath, err.Error())
			}

			var newToken securityToken
			if newToken, err = newInstancePrincipalToken(string(content)); err != nil {
				return nil, fmt.Errorf("failed to read security token from :%s. Due to: %s", securityTokenPath, err.Error())
			}

			return newToken, nil
		},
	}, nil
}

func newStaticFederationClient(sessionToken string, supplier sessionKeySupplier) (*genericFederationClient, error) {
	var newToken securityToken
	var err error
	if newToken, err = newInstancePrincipalToken(sessionToken); err != nil {
		return nil, fmt.Errorf("failed to read security token. Due to: %s", err.Error())
	}

	return &genericFederationClient{
		SessionKeySupplier: supplier,
		RefreshSecurityToken: func() (token securityToken, err error) {
			return newToken, nil
		},
	}, nil
}

// x509FederationClient retrieves a security token from Auth service.
type x509FederationClient struct {
	tenancyID                         string
	sessionKeySupplier                sessionKeySupplier
	leafCertificateRetriever          x509CertificateRetriever
	intermediateCertificateRetrievers []x509CertificateRetriever
	securityToken                     securityToken
	authClient                        *authClient
	mux                               sync.Mutex
}

func newX509FederationClient(region common.Region, tenancyID string,
	leafCertificateRetriever x509CertificateRetriever, intermediateCertificateRetrievers []x509CertificateRetriever) (federationClient, error) {

	client := &x509FederationClient{
		tenancyID:                         tenancyID,
		leafCertificateRetriever:          leafCertificateRetriever,
		intermediateCertificateRetrievers: intermediateCertificateRetrievers,
	}
	client.sessionKeySupplier = newSessionKeySupplier()
	var err error
	client.authClient, err = newAuthClient(region, client)
	if err != nil {
		return nil, err
	}
	return client, nil
}

var (
	genericHeaders = []string{"date", "(request-target)"}
	bodyHeaders    = []string{"content-length", "content-type", "x-content-sha256"}
)

type authClient struct {
	//HTTPClient performs the http network operations
	HTTPClient *http.Client

	//Signer performs auth operation
	Signer HTTPRequestSigner

	//The host of the service
	Host string

	//The user agent
	UserAgent string

	//Base path for all operations of this client
	BasePath string
}

func newAuthClient(region common.Region, provider KeyProvider) (*authClient, error) {
	client := &authClient{
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		Signer:    RequestSigner(provider, genericHeaders, bodyHeaders),
		UserAgent: sdkutil.UserAgent(),
		BasePath:  "v1/x509",
	}

	var err error
	if regionURL, ok := os.LookupEnv("OCI_SDK_AUTH_CLIENT_REGION_URL"); ok {
		client.Host = regionURL
	} else {
		client.Host, err = region.EndpointForService("auth")
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (client *authClient) Call(request *http.Request) (*http.Response, error) {
	request.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	err := client.Signer.Sign(request)
	if err != nil {
		return nil, err
	}

	response, err := client.HTTPClient.Do(request)
	if err != nil {
		return nil, err
	}

	code := response.StatusCode / 100
	if code == 4 || code == 5 {
		body, _ := io.ReadAll(response.Body)
		if len(body) == 0 {
			return nil, fmt.Errorf("error status code: %d", response.StatusCode)
		}
		return nil, fmt.Errorf("error status code: %d, message: %s", response.StatusCode, string(body))
	}
	return response, nil
}

// For authClient to sign requests to X509 Federation Endpoint
func (c *x509FederationClient) KeyID() (string, error) {
	tenancy := c.tenancyID
	fingerprint := fingerprint(c.leafCertificateRetriever.Certificate())
	return fmt.Sprintf("%s/fed-x509-sha256/%s", tenancy, fingerprint), nil
}

// For authClient to sign requests to X509 Federation Endpoint
func (c *x509FederationClient) PrivateRSAKey() (*rsa.PrivateKey, error) {
	key := c.leafCertificateRetriever.PrivateKey()
	if key == nil {
		return nil, fmt.Errorf("can not read private key from leaf certificate. Likely an error in the metadata service")
	}

	return key, nil
}

func (c *x509FederationClient) PrivateKey() (*rsa.PrivateKey, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.renewSecurityTokenIfNotValid(); err != nil {
		return nil, err
	}
	return c.sessionKeySupplier.PrivateKey(), nil
}

func (c *x509FederationClient) SecurityToken() (token string, err error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err = c.renewSecurityTokenIfNotValid(); err != nil {
		return "", err
	}
	return c.securityToken.String(), nil
}

func (c *x509FederationClient) ExpirationTime() time.Time {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.renewSecurityTokenIfNotValid(); err != nil {
		return time.Now().Add(-time.Second)
	}
	return c.securityToken.ExpirationTime()
}

func (c *x509FederationClient) renewSecurityTokenIfNotValid() (err error) {
	if c.securityToken == nil || !c.securityToken.Valid() {
		if err = c.renewSecurityToken(); err != nil {
			return fmt.Errorf("failed to renew security token: %s", err.Error())
		}
	}
	return nil
}

func (c *x509FederationClient) renewSecurityToken() (err error) {
	if err = c.sessionKeySupplier.Refresh(); err != nil {
		return fmt.Errorf("failed to refresh session key: %s", err.Error())
	}

	if err = c.leafCertificateRetriever.Refresh(); err != nil {
		return fmt.Errorf("failed to refresh leaf certificate: %s", err.Error())
	}

	updatedTenancyID := extractTenancyIDFromCertificate(c.leafCertificateRetriever.Certificate())
	if c.tenancyID != updatedTenancyID {
		err = fmt.Errorf("unexpected update of tenancy OCID in the leaf certificate. Previous tenancy: %s, Updated: %s", c.tenancyID, updatedTenancyID)
		return
	}

	for _, retriever := range c.intermediateCertificateRetrievers {
		if err = retriever.Refresh(); err != nil {
			return fmt.Errorf("failed to refresh intermediate certificate: %s", err.Error())
		}
	}

	if c.securityToken, err = c.getSecurityToken(); err != nil {
		return fmt.Errorf("failed to get security token: %s", err.Error())
	}

	return nil
}

func (c *x509FederationClient) makeHTTPRequest(request *x509FederationRequest) (*http.Request, error) {
	httpRequest := http.Request{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Method:     http.MethodPost,
		Header:     make(http.Header),
		URL:        &url.URL{},
	}

	host := strings.ToLower(c.authClient.Host)
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		c.authClient.Host = fmt.Sprintf("https://%s", c.authClient.Host)
	}

	clientURL, err := url.Parse(c.authClient.Host)
	if err != nil {
		return nil, fmt.Errorf("host is invalid. %s", err.Error())
	}

	httpRequest.URL.Host = clientURL.Host
	httpRequest.URL.Scheme = clientURL.Scheme
	currentPath := httpRequest.URL.Path
	if !strings.Contains(currentPath, fmt.Sprintf("/%s", c.authClient.BasePath)) {
		httpRequest.URL.Path = path.Clean(fmt.Sprintf("/%s/%s", c.authClient.BasePath, currentPath))
	}

	httpRequest.Header.Set("opc-client-info", sdkutil.UserAgent())
	httpRequest.Header.Set("User-Agent", sdkutil.UserAgent())
	httpRequest.Header.Set("Accept", "*/*")

	rawJSON, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	bodyBytes := bytes.NewReader(rawJSON)
	httpRequest.Header.Set("Content-Length", strconv.Itoa(len(rawJSON)))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Body = io.NopCloser(bodyBytes)
	httpRequest.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bodyBytes), nil
	}

	return &httpRequest, nil
}

func (c *x509FederationClient) getSecurityToken() (securityToken, error) {
	request := c.makeX509FederationRequest()
	httpRequest, err := c.makeHTTPRequest(request)
	if err != nil {
		return nil, fmt.Errorf("failed to make http request: %s", err.Error())
	}

	var httpResponse *http.Response

	for retry := 0; retry < 5; retry++ {
		if httpResponse, err = c.authClient.Call(httpRequest); err == nil {
			break
		}
		time.Sleep(250 * time.Microsecond)
	}

	defer closeBodyIfValid(httpResponse)

	if err != nil {
		return nil, fmt.Errorf("failed to get security token: %s", err.Error())
	}

	response := x509FederationResponse{}
	var content []byte
	content, err = io.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(content, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the response: %s", err.Error())
	}
	return newInstancePrincipalToken(response.Token)
}

func (c *x509FederationClient) GetClaim(key string) (interface{}, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := c.renewSecurityTokenIfNotValid(); err != nil {
		return nil, err
	}
	return c.securityToken.GetClaim(key)
}

type x509FederationRequest struct {
	Certificate              string   `json:"certificate,omitempty"`
	IntermediateCertificates []string `json:"intermediateCertificates,omitempty"`
	PublicKey                string   `json:"publicKey,omitempty"`
	FingerprintAlgorithm     string   `json:"fingerprintAlgorithm,omitempty"`
	Purpose                  string   `json:"purpose,omitempty"`
}

// type X509FederationRequest struct {
// 	X509FederationDetails
// }

// type X509FederationDetails struct {
// 	Certificate              string   `json:"certificate"`
// 	IntermediateCertificates []string `json:"intermediateCertificates"`
// 	PublicKey                string   `json:"publicKey"`
// }

// type X509FederationDetails struct {
// 	Certificate              string   `json:"certificate,omitempty"`
// 	IntermediateCertificates []string `json:"intermediateCertificates,omitempty"`
// 	PublicKey                string   `json:"publicKey,omitempty"`
// }

type x509FederationResponse struct {
	Token string `json:"token"`
}

func (c *x509FederationClient) makeX509FederationRequest() *x509FederationRequest {
	certificate := c.sanitizeCertificateString(string(c.leafCertificateRetriever.CertificatePemRaw()))
	publicKey := c.sanitizeCertificateString(string(c.sessionKeySupplier.PublicKeyPemRaw()))
	var intermediateCertificates []string
	for _, retriever := range c.intermediateCertificateRetrievers {
		intermediateCertificates = append(intermediateCertificates, c.sanitizeCertificateString(string(retriever.CertificatePemRaw())))
	}

	return &x509FederationRequest{
		// X509FederationDetails: X509FederationDetails{
		Certificate:              certificate,
		IntermediateCertificates: intermediateCertificates,
		PublicKey:                publicKey,
		FingerprintAlgorithm:     `SHA256`,
		Purpose:                  `DEFAULT`,
		// },
	}
}

func (c *x509FederationClient) sanitizeCertificateString(certString string) string {
	certString = strings.Replace(certString, "-----BEGIN CERTIFICATE-----", "", -1)
	certString = strings.Replace(certString, "-----END CERTIFICATE-----", "", -1)
	certString = strings.Replace(certString, "-----BEGIN PUBLIC KEY-----", "", -1)
	certString = strings.Replace(certString, "-----END PUBLIC KEY-----", "", -1)
	certString = strings.Replace(certString, "\n", "", -1)
	return certString
}

// sessionKeySupplier provides an RSA keypair which can be re-generated by calling Refresh().
type sessionKeySupplier interface {
	Refresh() error
	PrivateKey() *rsa.PrivateKey
	PublicKeyPemRaw() []byte
}

// genericKeySupplier implements sessionKeySupplier and provides an arbitrary refresh mechanism.
type genericKeySupplier struct {
	RefreshFn func() (*rsa.PrivateKey, []byte, error)

	privateKey      *rsa.PrivateKey
	publicKeyPemRaw []byte
}

func (s genericKeySupplier) PrivateKey() *rsa.PrivateKey {
	if s.privateKey == nil {
		return nil
	}

	c := *s.privateKey
	return &c
}

func (s genericKeySupplier) PublicKeyPemRaw() []byte {
	if s.publicKeyPemRaw == nil {
		return nil
	}

	c := make([]byte, len(s.publicKeyPemRaw))
	copy(c, s.publicKeyPemRaw)
	return c
}

func (s *genericKeySupplier) Refresh() (err error) {
	privateKey, publicPem, err := s.RefreshFn()
	if err != nil {
		return err
	}

	s.privateKey = privateKey
	s.publicKeyPemRaw = publicPem
	return nil
}

// create a sessionKeySupplier that reads keys from file every time it refreshes
func newFileBasedKeySessionSupplier(privateKeyPemPath string, passphrasePath *string) (*genericKeySupplier, error) {
	return &genericKeySupplier{
		RefreshFn: func() (*rsa.PrivateKey, []byte, error) {
			var err error
			var passContent []byte
			if passphrasePath != nil {
				if passContent, err = os.ReadFile(*passphrasePath); err != nil {
					return nil, nil, fmt.Errorf("can not read passphrase from file: %s, due to %s", *passphrasePath, err.Error())
				}
			}

			var keyPemContent []byte
			if keyPemContent, err = os.ReadFile(privateKeyPemPath); err != nil {
				return nil, nil, fmt.Errorf("can not read privateKey pem from file: %s, due to %s", privateKeyPemPath, err.Error())
			}

			var privateKey *rsa.PrivateKey
			if privateKey, err = PrivateKeyFromBytesWithPassword(keyPemContent, passContent); err != nil {
				return nil, nil, fmt.Errorf("can not create privateKey from contents of: %s, due to: %s", privateKeyPemPath, err.Error())
			}

			var publicKeyAsnBytes []byte
			if publicKeyAsnBytes, err = x509.MarshalPKIXPublicKey(privateKey.Public()); err != nil {
				return nil, nil, fmt.Errorf("failed to marshal the public part of the new keypair: %s", err.Error())
			}
			publicKeyPemRaw := pem.EncodeToMemory(&pem.Block{
				Type:  "PUBLIC KEY",
				Bytes: publicKeyAsnBytes,
			})
			return privateKey, publicKeyPemRaw, nil
		},
	}, nil
}

func newStaticKeySessionSupplier(privateKeyPemContent, passphrase []byte) (*genericKeySupplier, error) {
	var err error
	var privateKey *rsa.PrivateKey

	if privateKey, err = PrivateKeyFromBytesWithPassword(privateKeyPemContent, passphrase); err != nil {
		return nil, fmt.Errorf("can not create private key, due to: %s", err.Error())
	}

	var publicKeyAsnBytes []byte
	if publicKeyAsnBytes, err = x509.MarshalPKIXPublicKey(privateKey.Public()); err != nil {
		return nil, fmt.Errorf("failed to marshal the public part of the new keypair: %s", err.Error())
	}
	publicKeyPemRaw := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyAsnBytes,
	})

	return &genericKeySupplier{
		RefreshFn: func() (key *rsa.PrivateKey, bytes []byte, err error) {
			return privateKey, publicKeyPemRaw, nil
		},
	}, nil
}

// inMemorySessionKeySupplier implements sessionKeySupplier to vend an RSA keypair.
// Refresh() generates a new RSA keypair with a random source, and keeps it in memory.
//
// inMemorySessionKeySupplier is not thread-safe.
type inMemorySessionKeySupplier struct {
	keySize         int
	privateKey      *rsa.PrivateKey
	publicKeyPemRaw []byte
}

// newSessionKeySupplier creates and returns a sessionKeySupplier instance which generates key pairs of size 2048.
func newSessionKeySupplier() sessionKeySupplier {
	return &inMemorySessionKeySupplier{keySize: 2048}
}

// Refresh() is failure atomic, i.e., PrivateKey() and PublicKeyPemRaw() would return their previous values
// if Refresh() fails.
func (s *inMemorySessionKeySupplier) Refresh() (err error) {
	var privateKey *rsa.PrivateKey
	privateKey, err = rsa.GenerateKey(rand.Reader, s.keySize)
	if err != nil {
		return fmt.Errorf("failed to generate a new keypair: %s", err)
	}

	var publicKeyAsnBytes []byte
	if publicKeyAsnBytes, err = x509.MarshalPKIXPublicKey(privateKey.Public()); err != nil {
		return fmt.Errorf("failed to marshal the public part of the new keypair: %s", err.Error())
	}
	publicKeyPemRaw := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyAsnBytes,
	})

	s.privateKey = privateKey
	s.publicKeyPemRaw = publicKeyPemRaw
	return nil
}

func (s *inMemorySessionKeySupplier) PrivateKey() *rsa.PrivateKey {
	if s.privateKey == nil {
		return nil
	}

	c := *s.privateKey
	return &c
}

func (s *inMemorySessionKeySupplier) PublicKeyPemRaw() []byte {
	if s.publicKeyPemRaw == nil {
		return nil
	}

	c := make([]byte, len(s.publicKeyPemRaw))
	copy(c, s.publicKeyPemRaw)
	return c
}

type securityToken interface {
	fmt.Stringer
	Valid() bool
	ExpirationTime() time.Time

	claimHolder
}

type instancePrincipalToken struct {
	tokenString string
	jwtToken    *jwtToken
}

func newInstancePrincipalToken(tokenString string) (newToken securityToken, err error) {
	var jwtToken *jwtToken
	if jwtToken, err = parseJwt(tokenString); err != nil {
		return nil, fmt.Errorf("failed to parse the token string \"%s\": %s", tokenString, err.Error())
	}
	newToken = &instancePrincipalToken{tokenString, jwtToken}
	if !newToken.Valid() {
		return nil, fmt.Errorf("expired or invalid token string \"%s\"", tokenString)
	}
	return
}

func (t *instancePrincipalToken) String() string {
	return t.tokenString
}

func (t *instancePrincipalToken) Valid() bool {
	return !t.jwtToken.expired()
}

func (t *instancePrincipalToken) ExpirationTime() time.Time {
	return t.jwtToken.expirationTime()
}

var (
	// errNoSuchClaim is returned when a token does not hold the claim sought
	errNoSuchClaim = errors.New("no such claim")
)

func (t *instancePrincipalToken) GetClaim(key string) (interface{}, error) {
	if value, ok := t.jwtToken.payload[key]; ok {
		return value, nil
	}
	return nil, errNoSuchClaim
}
