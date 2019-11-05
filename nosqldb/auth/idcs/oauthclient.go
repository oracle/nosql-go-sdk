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
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
)

const (
	// IDCS service endpoints.
	roleEndpoint   = "/admin/v1/AppRoles"
	grantEndpoint  = "/admin/v1/Grants"
	statusEndpoint = "/admin/v1/AppStatusChanger"
	// Name of credentials file template.
	credentialsFileTmpl = "credentials.tmpl"

	andcAppFilter = "?filter=serviceTypeURN+eq+%22ANDC_ServiceEntitlement%22" +
		"+and+isOAuthResource+eq+true"

	addAppJSON = `{"displayName": %q,` +
		`"isOAuthClient": true,` +
		`"isOAuthResource": false,` +
		`"isUnmanagedApp": true,` +
		`"active": true,` +
		`"description": "Custom OAuth Client for application access to NoSQL Database Cloud Service",` +
		`"clientType": "confidential",` +
		`"allowedGrants": ["password", "refresh_token", "client_credentials"],` +
		`"trustScope": "Explicit",` +
		`"allowedScopes": [{"fqs": %q},{"fqs": %q}],` +
		`"schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:App"],` +
		`"basedOnTemplate": {"value": "CustomWebAppTemplateId"}}`

	grantRoleJSON = `{"app": {"value": %q},` +
		`"entitlement": {"attributeName": "appRoles", "attributeValue": %q},` +
		`"grantMechanism": "ADMINISTRATOR_TO_APP",` +
		`"grantee": {"value": %q, "type": "App"},` +
		`"schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:Grant"]}`

	deactivateAppJSON = `{"active": false, ` +
		`"schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:AppStatusChanger"]}`
)

// OAuthClient represents a custom OAuth client created by IDCS.
type OAuthClient struct {
	// Client name.
	Name string

	// IDCS URL.
	IDCSUrl string

	// Token file that contains access token.
	TokenFile string

	// The directory to store credentials file template.
	TmplFileDir string

	// Request timeout.
	Timeout time.Duration

	// Whether to turn on verbose mode.
	Verbose bool

	idcsHost   string
	out        io.Writer
	httpClient *http.Client
	logger     *logger.Logger
}

// NewOAuthClient creates an OAuthClient instance using the specified name, idcsUrl and tokens read
// from the specified tokenFile.
func NewOAuthClient(name, idcsUrl, tokenFile, tmplFileDir string, timeout time.Duration, verbose bool) (*OAuthClient, error) {
	if _, err := os.Stat(tokenFile); err != nil {
		return nil, err
	}

	if timeout < time.Millisecond {
		return nil, errors.New("timeout value must be at least 1 millisecond.")
	}

	c := &OAuthClient{
		Name:        name,
		IDCSUrl:     idcsUrl,
		TokenFile:   tokenFile,
		TmplFileDir: tmplFileDir,
		Timeout:     timeout,
		Verbose:     verbose,
		out:         os.Stdout,
		httpClient:  http.DefaultClient,
	}

	url, hostname, err := parseIDCSURL(c.IDCSUrl)
	if err != nil {
		return nil, err
	}

	c.IDCSUrl = url
	c.idcsHost = hostname

	var level logger.LogLevel
	if verbose {
		level = logger.Info
	} else {
		level = logger.Warn
	}

	c.logger = logger.New(os.Stdout, level, false)
	return c, nil
}

// Create creates an OAuth client and grants "ANDC_FullAccessRole" to it.
func (c *OAuthClient) Create() {
	c.output("Creating OAuth Client %q ...", c.Name)
	token, err := getAccessTokenFromFile(c.TokenFile)
	if err != nil {
		c.output("Failed to get access token from file %s: %v", c.TokenFile, err)
		return
	}

	// Get fully qualified scopes of PSM and ANDC applications.
	authHeader := auth.BearerToken + " " + token
	psmAudience, err := c.getPSMAudience(authHeader)
	if err != nil {
		c.output("Failed to get PSM application audience: %v", err)
		return
	}
	psmFQS := psmAudience + psmScope

	andcApp, err := c.getANDCApp(authHeader)
	if err != nil {
		c.output("Failed to get ANDC application info: %v", err)
		return
	}
	andcFQS := andcApp.audience + andcScope
	c.verboseOutput("Found scopes %q and %q", psmFQS, andcFQS)

	// Add a custom OAuth client.
	payload := fmt.Sprintf(addAppJSON, c.Name, psmFQS, andcFQS)
	clientInfo, err := c.addApp(authHeader, payload)
	if err != nil {
		c.output("Failed to create OAuth client %q: %v", c.Name, err)
		return
	}
	c.verboseOutput("OAuth client %q has been created successfully.", c.Name)

	// Get ANDC application role id.
	roleName := "ANDC_FullAccessRole"
	url := c.IDCSUrl + roleEndpoint + "?filter=displayName+eq+%22" + roleName + "%22"
	roleId, err := c.getResourceId(authHeader, url, "role")
	if err != nil {
		c.output("Failed to get role id of %q.", roleName)
		return
	}
	c.verboseOutput("Found %q role id %s.", roleName, roleId)

	// Grant ANDC_FullAccessRole to the custom client.
	payload = fmt.Sprintf(grantRoleJSON, andcApp.appId, roleId, clientInfo.appId)
	err = c.grantRole(authHeader, payload)
	if err != nil {
		c.output("Failed to grant role %q to OAuth client %q.", roleName, c.Name)
		return
	}
	c.verboseOutput("Successfully granted role %q to OAuth client %q.", roleName, c.Name)
	c.output("OAuth client %q has been created and granted %q role.\nClient ID: %s\nClient Secret: %s",
		c.Name, roleName, clientInfo.oauthId, clientInfo.secret)

	// Create a credentials file template.
	file, err := c.createCredentialsFileTemplate(clientInfo.oauthId, clientInfo.secret)
	if err != nil {
		c.output("Failed to create credentials template file: %v", err)
		return
	}
	c.output("Successfully created credentials template file %s.", file)
}

// Delete deactivates and deletes the OAuth client.
func (c *OAuthClient) Delete() {
	c.output("Deleting OAuth Client %q ...", c.Name)
	token, err := getAccessTokenFromFile(c.TokenFile)
	if err != nil {
		c.output("Failed to get access token from file %s: %v", c.TokenFile, err)
		return
	}

	// Get OAuth client application id
	authHeader := auth.BearerToken + " " + token
	url := c.IDCSUrl + appEndpoint + "?filter=displayName+eq+%22" + c.Name + "%22"
	appId, err := c.getResourceId(authHeader, url, "client")
	if err != nil {
		c.output("Failed to get application id of OAuth client %q: %v", c.Name, err)
		return
	}
	c.verboseOutput("Found OAuth client application id: %s.", appId)

	// Deactivate OAuth client
	err = c.deactivateApp(authHeader, appId)
	if err != nil {
		c.output("Failed to deactivate OAuth client %q: %v", c.Name, err)
		return
	}
	c.verboseOutput("OAuth client %q has been deactivated.", c.Name)

	// Remove the OAuth client
	err = c.removeClient(authHeader, appId)
	if err != nil {
		c.output("Failed to remove OAuth client %q: %v", c.Name, err)
		return
	}
	c.output("OAuth client %q has been deleted.", c.Name)
}

// Verify verifies if OAuth client has required grants, scopes and roles.
func (c *OAuthClient) Verify() {
	c.output("Verifying OAuth Client %q ...", c.Name)
	token, err := getAccessTokenFromFile(c.TokenFile)
	if err != nil {
		c.output("Failed to get access token from file %s: %v", c.TokenFile, err)
		return
	}

	authHeader := auth.BearerToken + " " + token
	data, err := c.getClientMetadata(authHeader)
	if err != nil {
		c.output("Failed to get metadata of OAuth client %q: %v", c.Name, err)
		return
	}

	resources, err := parseResourcesFromScimListResponse(data, true /* allow empty resource */)
	if err != nil {
		c.output("Failed to parse JSON %s, got error %v", string(data), err)
		return
	}
	// Got an empty resource list.
	if resources == nil {
		c.output("OAuth client %q does not exist, or the token file is invalid. "+
			"User who downloads the token must have Identity Domain Administrator role.", c.Name)
		return
	}

	verificationErrs := make([]error, 0, 3)

	grants, err := getArrayValuesFromResources(resources, "allowedGrants")
	if err != nil {
		c.output("Failed to parse \"allowedGrants\" from JSON %s, got error %v", string(data), err)
		err = fmt.Errorf("Failed to verify grants of OAuth client, got error %v", err)
	} else {
		// Verify if client has required grants.
		err = c.verifyGrants(grants)
	}

	if err != nil {
		verificationErrs = append(verificationErrs, err)
	}

	scopes, err := getArrayElementValuesFromResources(resources, false, "allowedScopes", "fqs")
	if err != nil {
		c.output("Failed to parse \"allowedScopes\" from JSON %s, got error %v", string(data), err)
		err = fmt.Errorf("Failed to verify service scopes of OAuth client, got error %v", err)
	} else {
		// Verify if client has PSM and ANDC FQS.
		err = c.verifyScopes(scopes)
	}

	if err != nil {
		verificationErrs = append(verificationErrs, err)
	}

	// verify if client has ANDC role
	roles, err := getArrayElementValuesFromResources(resources, false, "grantedAppRoles", "display")
	if err != nil {
		c.output("Failed to parse \"grantedAppRoles\" from JSON %s, got error %v", string(data), err)
		err = fmt.Errorf("Failed to verify application roles of the OAuth client, got error %v", err)
	} else {
		err = c.verifyRoles(roles)
	}

	if err != nil {
		verificationErrs = append(verificationErrs, err)
	}

	if len(verificationErrs) > 0 {
		c.output("Verification failed")
		for _, e := range verificationErrs {
			c.output("%v", e)
		}
	} else {
		c.output("Verification succeeded.")
	}
}

func (c *OAuthClient) getClientMetadata(authHeader string) ([]byte, error) {
	url := c.IDCSUrl + appEndpoint + "?filter=displayName+eq+%22" + c.Name + "%22"
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doGetRequest(url, headers)
	if err != nil {
		return nil, err
	}

	if resp.Code > 299 {
		return nil, wrapIDCSErrors(resp, "Getting client "+c.Name)
	}

	return resp.Body, nil
}

// verifyGrants verifies if OAuth client has required grants.
func (c *OAuthClient) verifyGrants(allowedGrants []string) error {
	c.verboseOutput("OAuth client %q allowed grants: %v", c.Name, allowedGrants)
	requiredGrants := []string{"password", "refresh_token", "client_credentials"}
	match := 0
	for _, g := range allowedGrants {
		if strings.EqualFold(g, requiredGrants[0]) ||
			strings.EqualFold(g, requiredGrants[1]) ||
			strings.EqualFold(g, requiredGrants[2]) {
			match++
		}
	}

	if match < 3 {
		return fmt.Errorf("At least one required grants in %v are missing, "+
			"allowed grants of the OAuth client are %v.", requiredGrants, allowedGrants)
	}

	c.verboseOutput("The \"allowedGrants\" of OAuth client %q has been verified.", c.Name)
	return nil
}

// verifyScopes verifies if OAuth client has required service scopes.
func (c *OAuthClient) verifyScopes(allowedScopes []string) error {
	c.verboseOutput("OAuth client %q allowed scopes: %v", c.Name, allowedScopes)
	requiredScopes := []string{psmScope, andcScope}
	match := 0
	for _, fqs := range allowedScopes {
		if strings.Contains(fqs, requiredScopes[0]) || strings.Contains(fqs, requiredScopes[1]) {
			match++
		}
	}

	if match < 2 {
		return fmt.Errorf("At least one required OAuth scopes in %v are missing, "+
			"allowed scopes of the OAuth client are %v", requiredScopes, allowedScopes)
	}

	c.verboseOutput("The service scopes of OAuth client %q has been verified.", c.Name)
	return nil
}

// verifyRoles verifies if OAuth client has been granted the ANDC_FullAccessRole role.
func (c *OAuthClient) verifyRoles(grantedAppRoles []string) error {
	c.verboseOutput("OAuth client %q granted roles: %v", c.Name, grantedAppRoles)
	requiredRole := "ANDC_FullAccessRole"
	match := 0
	for _, r := range grantedAppRoles {
		if r == requiredRole {
			match++
			break
		}
	}

	if match < 1 {
		return fmt.Errorf("OAuth client does not have required role \"%s\".", requiredRole)
	}

	c.verboseOutput("The granted role of OAuth client %q has been verified.", c.Name)
	return nil
}

func (c *OAuthClient) doPostRequest(url string, body []byte, headers map[string]string) (resp *httputil.Response, err error) {
	return httputil.DoRequest(context.Background(), c.httpClient, c.Timeout, http.MethodPost, url, body, headers, c.logger)
}
func (c *OAuthClient) doGetRequest(url string, headers map[string]string) (resp *httputil.Response, err error) {
	return httputil.DoRequest(context.Background(), c.httpClient, c.Timeout, http.MethodGet, url, nil, headers, c.logger)
}

func (c *OAuthClient) doPutRequest(url string, body []byte, headers map[string]string) (resp *httputil.Response, err error) {
	return httputil.DoRequest(context.Background(), c.httpClient, c.Timeout, http.MethodPut, url, body, headers, c.logger)
}

func (c *OAuthClient) doDeleteRequest(url string, headers map[string]string) (resp *httputil.Response, err error) {
	return httputil.DoRequest(context.Background(), c.httpClient, c.Timeout, http.MethodDelete, url, nil, headers, c.logger)
}

// getPSMAudience sends a GET request to IDCS Apps endpoint to retrieve the
// "audience" attribute of PSM application.
func (c *OAuthClient) getPSMAudience(authHeader string) (string, error) {
	url := c.IDCSUrl + appEndpoint + psmAppFilter
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doGetRequest(url, headers)
	if err != nil {
		return "", err
	}

	if resp.Code > 299 {
		return "", wrapIDCSErrors(resp, "Getting account metadata")
	}

	resource, err := parseResourcesFromScimListResponse(resp.Body, false)
	if err != nil {
		return "", err
	}

	aud := "audience"
	values, err := getStringValuesFromResources(resource, aud)
	if err != nil {
		return "", err
	}

	return values[aud], nil
}

// getANDCApp sends a GET request to IDCS Apps endpoint to retrieve the
// ANDC application information including application id and audience.
func (c *OAuthClient) getANDCApp(authHeader string) (andc *andcAppInfo, err error) {
	url := c.IDCSUrl + appEndpoint + andcAppFilter
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doGetRequest(url, headers)
	if err != nil {
		return nil, err
	}

	if resp.Code >= 299 {
		return nil, wrapIDCSErrors(resp, "Getting service metadata")
	}

	res, err := parseResourcesFromScimListResponse(resp.Body, false)
	if err != nil {
		return nil, err
	}

	aud := "audience"
	id := "id"
	values, err := getStringValuesFromResources(res, aud, id)
	if err != nil {
		return nil, err
	}

	return &andcAppInfo{
		appId:    values[id],
		audience: values[aud],
	}, nil
}

// addApp sends a POST request to IDCS Apps endpoint to create a custom
// OAuth client.
func (c *OAuthClient) addApp(authHeader, payload string) (*clientInfo, error) {
	url := c.IDCSUrl + appEndpoint
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doPostRequest(url, []byte(payload), headers)
	if err != nil {
		return nil, err
	}

	if resp.Code == 409 {
		return nil, fmt.Errorf("an OAuth client with name %q already exists. "+
			"To recreate, run with the %q flag. To verify if "+
			"existing client is configured correctly, run with the %q flag",
			c.Name, "-delete", "-verify")
	}

	if resp.Code > 299 {
		return nil, wrapIDCSErrors(resp, "Adding custom client")
	}

	id := "id"
	name := "name"
	secret := "clientSecret"
	values, err := jsonutil.GetStringValues(resp.Body, id, name, secret)
	if err != nil {
		return nil, err
	}

	return &clientInfo{
		appId:   values[id],
		oauthId: values[name],
		secret:  values[secret],
	}, nil
}

func (c *OAuthClient) getResourceId(authHeader, url, resource string) (string, error) {
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doGetRequest(url, headers)
	if err != nil {
		return "", err
	}

	if resp.Code > 299 {
		return "", wrapIDCSErrors(resp, "Getting id of "+resource)
	}

	data, err := parseResourcesFromScimListResponse(resp.Body, false)
	if err != nil {
		return "", err
	}

	id := "id"
	values, err := getStringValuesFromResources(data, id)
	if err != nil {
		return "", err
	}

	return values[id], nil
}

func (c *OAuthClient) grantRole(authHeader, payload string) error {
	url := c.IDCSUrl + grantEndpoint
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doPostRequest(url, []byte(payload), headers)
	if err != nil {
		return err
	}

	if resp.Code >= 200 && resp.Code <= 299 {
		return nil
	}

	return wrapIDCSErrors(resp, "Granting required role to client")
}

func (c *OAuthClient) deactivateApp(authHeader, id string) error {
	url := c.IDCSUrl + statusEndpoint + "/" + id
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doPutRequest(url, []byte(deactivateAppJSON), headers)
	if err != nil {
		return err
	}

	if resp.Code >= 200 && resp.Code <= 299 {
		return nil
	}
	return wrapIDCSErrors(resp, "deactivating OAuth client "+c.Name)
}

func (c *OAuthClient) removeClient(authHeader, id string) error {
	url := c.IDCSUrl + appEndpoint + "/" + id
	headers := createHttpHeaders(c.idcsHost, authHeader, httputil.AppScimJson)
	resp, err := c.doDeleteRequest(url, headers)
	if err != nil {
		return err
	}

	if resp.Code >= 200 && resp.Code <= 299 {
		return nil
	}
	return wrapIDCSErrors(resp, "removing OAuth client "+c.Name)
}

func (c *OAuthClient) output(msgFmt string, msgArgs ...interface{}) {
	fmt.Fprintf(os.Stdout, msgFmt, msgArgs...)
	fmt.Fprintln(os.Stdout)
}

func (c *OAuthClient) verboseOutput(msgFmt string, msgArgs ...interface{}) {
	if !c.Verbose {
		return
	}
	c.output(msgFmt, msgArgs...)
}

func (c *OAuthClient) createCredentialsFileTemplate(clientId, clientSecret string) (string, error) {
	file := credentialsFileTmpl
	if c.TmplFileDir != "" {
		if err := os.MkdirAll(c.TmplFileDir, os.FileMode(0700)); err != nil {
			return "", err
		}
		file = filepath.Join(c.TmplFileDir, file)
	}

	var buf bytes.Buffer
	buf.WriteString(clientIdProp + "=" + clientId + "\n")
	buf.WriteString(clientSecretProp + "=" + clientSecret + "\n")
	buf.WriteString(usernameProp + "=\n")
	buf.WriteString(passwordProp + "=\n")
	if err := ioutil.WriteFile(file, buf.Bytes(), os.FileMode(0600)); err != nil {
		return "", err
	}
	return file, nil
}

// getAccessTokenFromFile reads in the user provided JSON file, returns the
// access token if it exists in the file.
func getAccessTokenFromFile(file string) (string, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}

	token, err := jsonutil.GetString(data, "app_access_token")
	if err != nil {
		return "", err
	}

	return token, nil
}

func wrapIDCSErrors(resp *httputil.Response, action string) error {
	return handleIDCSErrors(resp, action,
		" Access token in the token file might have expired,"+
			" or the token file was generated with incorrect scopes,"+
			" requires Identity Domain Administrator")
}

type andcAppInfo struct {
	appId, audience string
}

type clientInfo struct {
	appId, oauthId, secret string
}
