//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package iam

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type iamTestSuite struct {
	suite.Suite
}

// fields typically read from an OCI IAM config file
type testProviderInfo struct {
	user        *string
	fingerprint *string
	keyFile     *string
	tenancy     *string
	region      *string
	shortDesc   *string
	compartment *string
	expectAuth  *string
	expectErr   bool
}

const (
	testKeyFile       = "testdata/test-iam.valid.pem"
	testBadKeyFile    = "testdata/test-iam.corrupted.pem"
	testExpectedAuth  = "Signature version=\"1\",headers=\"date (request-target) host\",keyId=\"ocid1.tenancy.oc1..aaaaaaaaba3pv6wkcr4jqae5f15p2b2m2yt2j6rx32uzr4h25vqstifsfdsq/ocid1.user.oc1..aaaaaaaat5nvwcna5j6aqzjcaty5eqbb6qt2jvpkanghtgdaqedqw3rynjq/20:3b:97:13:55:1c:5b:0d:d3:37:d8:50:4e:c5:3a:34\",algorithm=\"rsa-sha256\",signature=\"FY0I/Jwl2oiQrug9/tB/tPiajq2zDqiLdU+YtxDaQ5onMvF90RtSGjPRwqbLl9+n4MPhgVVMXgpPXWe9l5TZ30/yF9O97CDLVOEGZ2DhSclSmejLVVuNrl14v559VKfxookpXwjYxLA1mT4mgq50MV/6e+mRi18U62uiJ3seZZI=\""
	testCompartmentID = "ocid1.compartment.oc1..aaaaaaaaba3pv6wkcr4bbchskrnmgf4hjsakfd843hjsj4h25vqstifsfdsq"
)

var testCasesForNewProvider = []*testProviderInfo{
	// nil provider info
	nil,
	// Valid IAM properties.
	// most of these are defined in http_signer_test.go
	{
		user:        SP(testUserOCID),
		fingerprint: SP(testFingerprint),
		keyFile:     SP(testKeyFile),
		tenancy:     SP(testTenancyOCID),
		region:      SP(testRegion),
		compartment: nil,
		expectAuth:  SP(testExpectedAuth),
		shortDesc:   SP("Basic passing case"),
		expectErr:   false,
	},
	// Use an alternate compartmentID
	{
		user:        SP(testUserOCID),
		fingerprint: SP(testFingerprint),
		keyFile:     SP(testKeyFile),
		tenancy:     SP(testTenancyOCID),
		region:      SP(testRegion),
		compartment: SP(testCompartmentID),
		// Note: compartmentID is not used in signature calculation, so we expect
		//       the same signature with any compartmentID
		expectAuth: SP(testExpectedAuth),
		shortDesc:  SP("Basic passing case with alternate compartmentID"),
		expectErr:  false,
	},
	// Specify a non-exist file for "key_file" property.
	{
		user:        SP(testUserOCID),
		fingerprint: SP(testFingerprint),
		keyFile:     nil,
		tenancy:     SP(testTenancyOCID),
		region:      SP(testRegion),
		compartment: nil,
		expectAuth:  nil,
		shortDesc:   SP("Missing key file"),
		expectErr:   true,
	},
	// specify a mangled key
	{
		user:        SP(testUserOCID),
		fingerprint: SP(testFingerprint),
		keyFile:     SP(testBadKeyFile),
		tenancy:     SP(testTenancyOCID),
		region:      SP(testRegion),
		compartment: nil,
		expectAuth:  nil,
		shortDesc:   SP("Corrupted key file"),
		expectErr:   true,
	},
	// Do not specify user property.
	{
		user:        SP(""),
		fingerprint: SP(testFingerprint),
		keyFile:     SP(testKeyFile),
		tenancy:     SP(testTenancyOCID),
		region:      SP(testRegion),
		compartment: nil,
		expectAuth:  nil,
		shortDesc:   SP("Missing \"user=\" property"),
		expectErr:   true,
	},
	// Do not specify fingerprint property.
	{
		user:        SP(testUserOCID),
		fingerprint: SP(""),
		keyFile:     SP(testKeyFile),
		tenancy:     SP(testTenancyOCID),
		region:      SP(testRegion),
		compartment: nil,
		expectAuth:  nil,
		shortDesc:   SP("Missing \"fingerprint=\" property"),
		expectErr:   true,
	},
	// TODO:
	// invalid/mangled user, tenancy, region, fingerprint
	// invalid format, empty, binary config file
	// calls to SignHTTPRequest():
	// compare signature given valid date string
	// compare signature with invalid / empty date
}

func (suite *iamTestSuite) TestNewSignatureProvider() {
	var p *SignatureProvider
	var err error
	var f string
	var msgPrefix string
	var compID string

	for i, r := range testCasesForNewProvider {
		if r == nil {
			msgPrefix = fmt.Sprintf("Testcase %d (nonexistent prop file): ", i+1)
			f = "testdata/NoSuchIAMFile.properties"
		} else {
			msgPrefix = fmt.Sprintf("Testcase %d (%s): ", i+1, *r.shortDesc)
			f, err = createPropFile(*r)
			if !suite.NoErrorf(err, msgPrefix+"failed to create iam properties file %v", err) {
				continue
			}
			defer os.Remove(f)
		}

		if r != nil && r.compartment != nil {
			compID = *r.compartment
		} else {
			compID = ""
		}

		p, err = NewSignatureProvider(f, "DEFAULT", "", compID)

		if r == nil || r.expectErr {
			suite.Errorf(err, msgPrefix+"NewSignatureProvider() should have failed, but succeeded")
			continue
		}

		if suite.NoErrorf(err, msgPrefix+"NewSignatureProvider() got error: %v", err) {
			err = suite.checkSignatureGeneration(p, r, msgPrefix)
			suite.NoErrorf(err, msgPrefix+"http header signing error")
		}
	}
}

func SP(s string) *string {
	return &s
}

func createPropFile(props testProviderInfo) (string, error) {
	var buf bytes.Buffer

	// TODO: non-default
	buf.WriteString("[DEFAULT]\n")

	if props.user != nil {
		buf.WriteString("user=" + *props.user + "\n")
	}
	if props.fingerprint != nil {
		buf.WriteString("fingerprint=" + *props.fingerprint + "\n")
	}
	if props.tenancy != nil {
		buf.WriteString("tenancy=" + *props.tenancy + "\n")
	}
	if props.region != nil {
		buf.WriteString("region=" + *props.region + "\n")
	}
	if props.keyFile != nil {
		buf.WriteString("key_file=" + *props.keyFile + "\n")
	}

	f, err := ioutil.TempFile("testdata", "test-iam.properties.*~")
	if err != nil {
		return "", err
	}

	err = ioutil.WriteFile(f.Name(), buf.Bytes(), os.FileMode(0600))
	return f.Name(), err
}

func (suite *iamTestSuite) checkSignatureGeneration(p *SignatureProvider, r *testProviderInfo, prefix string) error {

	// create an http request, sign it, then verify the signature
	body := bytes.NewBufferString("CREATE TABLE IF NOT EXISTS testData (id LONG, test_string STRING, PRIMARY KEY(id))")

	req, err := http.NewRequest("POST", "/V0/nosql/data", body)
	if err != nil {
		return fmt.Errorf("Can't create new http request")
	}

	// the Host and Date headers are the only ones currently used for
	// signature generation (besides the URI). The rest are here for completeness.
	req.Header.Set("Host", "10.123.213.031:8088")
	req.Header.Set("Date", "Fri, 18 Oct 2019 00:17:18 GMT")
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("User-Agent", "NoSQL-GoSDK/5.0.0 (go1.12.7; linux/amd64)")
	req.Header.Set("X-Nosql-Request-Id", "1292")

	p.signer.Sign(req)

	gotAuth := req.Header.Get(requestHeaderAuthorization)

	if r.expectAuth != nil && gotAuth != *r.expectAuth {
		return fmt.Errorf("authorization header failed: expected=%s\nactual=%s", *r.expectAuth, gotAuth)
	}
	return nil
}

func TestIAM(t *testing.T) {
	suite.Run(t, new(iamTestSuite))
}
