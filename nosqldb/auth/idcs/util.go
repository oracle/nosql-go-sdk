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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"runtime"

	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

// Authorization grant type.
type grantType string

const (
	// Password grant.
	passwordGrant grantType = "password"

	// Client credentials grant.
	clientCredentialsGrant grantType = "client_credentials"
)

// accessTokenRequest represents a request to acquire access token.
type accessTokenRequest struct {
	// User credentials.
	userCreds IDCSCredentials

	// Client credentials.
	clientCreds IDCSCredentials

	// Grant type.
	grantType grantType

	// The scope of access request.
	scope string
}

// body returns the content of access token request.
func (r *accessTokenRequest) body() (body string, err error) {
	switch r.grantType {
	case passwordGrant:
		body = fmt.Sprintf("grant_type=password&username=%s&scope=%s&password=%s",
			r.userCreds.Alias, r.scope, string(r.userCreds.Secret))
		return

	case clientCredentialsGrant:
		body = fmt.Sprintf("grant_type=client_credentials&scope=%s", r.scope)
		return

	default:
		return "", errors.New("unsupported grant type")
	}
}

// authHeader returns the authorization header according to client credentials.
func (r *accessTokenRequest) authHeader() string {
	return httputil.BasicAuth(r.clientCreds.Alias, r.clientCreds.Secret)
}

// userHomeDir returns current user's home directory.
//
// Starting with go1.12, use os.UserHomeDir()
func userHomeDir() string {
	current, err := user.Current()
	if err != nil {
		if runtime.GOOS == "windows" {
			return os.Getenv("USERPROFILE")
		}
		return os.Getenv("HOME")
	}
	return current.HomeDir
}

// parseIDCSURL parses IDCS URL from the specified IDCS endpoint, returns the
// parsed IDCS URL, IDCS host and any errors if occurred during parsing.
func parseIDCSURL(endpoint string) (idcsUrl, hostname string, err error) {
	if endpoint == "" {
		return "", "", nosqlerr.NewIllegalArgument("IDCS URL must be non-empty")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", nosqlerr.NewIllegalArgument("Invalid IDCS URL: %v", err)
	}

	if u.Scheme == "" {
		u.Scheme = "https"
	}

	// Reassembles IDCS url by (scheme + :// + host), which strips the trailing
	// slash if the provided url is https://host/
	return u.Scheme + "://" + u.Host, u.Hostname(), nil
}

// createHttpHeaders is used to create common HTTP headers required by OAuth requests.
func createHttpHeaders(host, authString, contentType string) map[string]string {
	headers := make(map[string]string, 6)
	headers["Host"] = host
	headers["Authorization"] = authString
	headers["Content-Type"] = contentType
	headers["Connection"] = "keep-alive"
	headers["Cache-Control"] = "no-store"
	headers["User-Agent"] = sdkutil.UserAgent()
	return headers
}

// handleIDCSErrors inspects the error code from IDCS response and returns
// appropriate errors to users.
//
// Possible error codes returned from IDCS for SCIM endpoints are as follows:
//
// Unexpected case:
//
//   307, 308 - redirect-related errors
//   400 - bad request, indicates code error
//   403 - request operation is not allowed
//   404 - this PSMApp doesn't exist
//   405 - method not allowed
//   409 - version mismatch
//   412 - precondition failed for update op
//   413 - request too long
//   415 - not acceptable
//   501 - this method not implemented
//
// Security failure case:
//
//   401 - no permission
//
// Service unavailable:
//
//   500 - internal server error
//   503 - service unavailable
//
func handleIDCSErrors(resp *httputil.Response, action, errMsg string) error {
	switch resp.Code {
	case 401:
		return nosqlerr.New(nosqlerr.InsufficientPermission,
			"%s is unauthorized. %s. Error response: %s", action, errMsg, string(resp.Body))

	case 500, 503:
		return nosqlerr.NewRequestTimeout("%s error, expect to retry. "+
			"Error response: %s, status code: %d", action, string(resp.Body), resp.Code)

	default:
		return nosqlerr.NewIllegalState("%s error, IDCS error response: %s", action, string(resp.Body))
	}
}

// handleTokenErrorResponse inspects the error code from IDCS response for
// access token request and returns appropriate errors to users.
func handleTokenErrorResponse(resp *httputil.Response, logger *logger.Logger) error {
	if resp.Code >= 500 {
		msg := fmt.Sprintf("Error acquiring access token, expect to retry. "+
			"Error response: %s, status code: %d", string(resp.Body), resp.Code)
		logger.Info(msg)
		return nosqlerr.NewRequestTimeout(msg)
	}

	if resp.Code == 400 && len(resp.Body) == 0 {
		// IDCS doesn't return error message when credentials has invalid URL encoded characters.
		return nosqlerr.NewIllegalArgument("Error acquiring access token, "+
			"status code: %d, CredentialsProvider supplies invalid credentials.", resp.Code)
	}

	return nosqlerr.New(nosqlerr.InvalidAuthorization,
		"Error acquiring access token from Identity Cloud Service. "+
			"IDCS error response: %s, status code: %d", string(resp.Body), resp.Code)
}

// parseResourcesFromScimListResponse parses the "Resources" from the SCIM JSON
// response returned from IDCS, which is in the form of:
//
//   {
//     "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
//     "totalResults": 1,
//     "Resources": [
//       {
//         ...
//         "allowedGrants": ["password", "client_credentials"],
//       }
//     ]
//   }
//
func parseResourcesFromScimListResponse(data []byte, allowEmptyList bool) (map[string]interface{}, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}

	field := "totalResults"
	total, ok := v[field]
	if !ok {
		return nil, fmt.Errorf("the required field \"%s\" is missing", field)
	}

	f, err := jsonutil.ExpectNumber(total)
	if err != nil {
		return nil, fmt.Errorf("unexpected value of the \"%s\" field, %v", field, err)
	}

	n := int(f)
	if n <= 0 {
		if allowEmptyList {
			// return a nil map and nil error
			return nil, nil
		}
		return nil, nosqlerr.New(nosqlerr.InsufficientPermission,
			"got empty resource list, either resource does not exist or "+
				"user does not have required role")
	}

	if n > 1 {
		return nil, fmt.Errorf("unexpected number of metadata result, expects 1, got %d", n)
	}

	field = "Resources"
	res, ok := v[field]
	if !ok {
		return nil, fmt.Errorf("the required field \"%s\" is missing", field)
	}

	// The value of "Resources" field should be an array that contains only one
	// element because totalResults=1
	resArray, err := jsonutil.ExpectArray(res)
	if err != nil {
		return nil, fmt.Errorf("unexpected value of the \"%s\" field, %v", field, err)
	}

	n = len(resArray)
	if n != 1 {
		return nil, fmt.Errorf("unexpected number of \"Resources\", expects 1, got %d", n)
	}

	resObj, err := jsonutil.ExpectObject(resArray[0])
	if err != nil {
		return nil, fmt.Errorf("unexpected value of array element of the \"%s\" field, %v", field, err)
	}
	return resObj, nil
}

// getArrayElementValuesFromResources gets the element values of specified
// array from resource response. The array element must be a JSON object.
// For example, this function can be used to parse the resource shown below:
//
//   {
//     "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
//     "totalResults": 1,
//     "Resources": [
//       {
//         "allowedScopes": [
//           {
//             "fqs": "http://test.us.oracle.com:7777urn:opc:resource:consumer::all",
//             "idOfDefiningApp": "deabd3635565402ebd4848286ae5a3a4"
//           },
//           {
//             "fqs": "urn:opc:andc:entitlementid=123456789urn:opc:andc:resource:consumer::all",
//             "idOfDefiningApp": "897fe6f66712491497c20a9fa9cddaf0"
//           }
//         ],
//       }
//     ],
//     "startIndex": 1,
//     "itemsPerPage": 50
//   }
//
func getArrayElementValuesFromResources(data map[string]interface{}, allowArrayNotExists bool,
	arrayName, nestedObjectKey string) ([]string, error) {

	v, ok := data[arrayName]
	if !ok {
		if allowArrayNotExists {
			// return a nil slice and nil error
			return nil, nil
		}
		return nil, fmt.Errorf("the required field \"%s\" is missing", arrayName)
	}

	array, err := jsonutil.ExpectArray(v)
	if err != nil {
		return nil, fmt.Errorf("unexpected value of the \"%s\" field, %v", arrayName, err)
	}

	result := make([]string, 0, len(array))
	for _, elem := range array {
		m, err := jsonutil.ExpectObject(elem)
		if err != nil {
			return nil, fmt.Errorf("unexpected value of array element of the \"%s\" field, %v", arrayName, err)
		}

		v, ok := m[nestedObjectKey]
		if !ok {
			continue
		}

		s, err := jsonutil.ExpectString(v)
		if err != nil {
			return nil, fmt.Errorf("unexpected value of the \"%s\" field, %v", nestedObjectKey, err)
		}

		result = append(result, s)
	}

	return result, nil
}

// getArrayValuesFromResources gets the specified array values from resource
// response.
// This function can be used to parse the resource shown below:
//
//   {
//     "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
//     "totalResults": 1,
//     "Resources": [
//       {
//         ...
//         "allowedGrants": ["password", "client_credentials"],
//       }
//     ]
//   }
//
func getArrayValuesFromResources(data map[string]interface{}, arrayName string) ([]string, error) {

	v, ok := data[arrayName]
	if !ok {
		return nil, fmt.Errorf("cannot find the %q field from resource response", arrayName)
	}

	array, err := jsonutil.ExpectArray(v)
	if err != nil {
		return nil, fmt.Errorf("unexpected value of the %q field, %v", arrayName, err)
	}

	result := make([]string, 0, len(array))
	for _, elem := range array {
		s, err := jsonutil.ExpectString(elem)
		if err != nil {
			return nil, fmt.Errorf("Unexpected value of the array element of %q field, %v", arrayName, err)
		}

		result = append(result, s)
	}

	return result, nil
}

// getStringValuesFromResources gets the values of specified string fields from
// resource response.
// This function can be used to parse the resources shown below:
//
//   {
//     "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
//     "totalResults": 1,
//     "Resources": [
//       {
//        "isOAuthResource": true,
//        ...
//        "audience": "http://example.com:7777",
//        "name": "PSMApp-cacct-14b4f579eec*********_APPID",
//        "clientSecret": "775cd0ca-34ea-4317-9a0c-********",
//        ...
//       }
//     ]
//   }
//
func getStringValuesFromResources(data map[string]interface{}, fieldNames ...string) (map[string]string, error) {

	result := make(map[string]string, len(fieldNames))
	for _, name := range fieldNames {
		v, ok := data[name]
		if !ok {
			continue
		}

		s, err := jsonutil.ExpectString(v)
		if err != nil {
			continue
		}

		result[name] = s
	}

	return result, nil
}
