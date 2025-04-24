// Copyright (c) 2016, 2025 Oracle and/or its affiliates. All rights reserved.
// This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

package iam

import (
	"bytes"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"

	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
)

// PrivateKeyFromBytes is a helper function that will produce a RSA private
// key from bytes. This function is deprecated in favour of PrivateKeyFromBytesWithPassword
// Deprecated
func PrivateKeyFromBytes(pemData []byte, password *string) (key *rsa.PrivateKey, e error) {
	if password == nil {
		return PrivateKeyFromBytesWithPassword(pemData, nil)
	}

	return PrivateKeyFromBytesWithPassword(pemData, []byte(*password))
}

// PrivateKeyFromBytesWithPassword is a helper function that will produce a RSA private
// key from bytes and a password.
func PrivateKeyFromBytesWithPassword(pemData, password []byte) (key *rsa.PrivateKey, e error) {
	if pemBlock, _ := pem.Decode(pemData); pemBlock != nil {
		decrypted := pemBlock.Bytes
		if x509.IsEncryptedPEMBlock(pemBlock) {
			if password == nil {
				e = fmt.Errorf("private key password is required for encrypted private keys")
				return
			}
			if decrypted, e = x509.DecryptPEMBlock(pemBlock, password); e != nil {
				return
			}
		}

		key, e = x509.ParsePKCS1PrivateKey(decrypted)
		if e != nil {
			e = nil
			parseResult, e := x509.ParsePKCS8PrivateKey(decrypted)
			if e == nil {
				key = parseResult.(*rsa.PrivateKey)
			}
		}

	} else {
		e = fmt.Errorf("PEM data was not found in buffer")
		return
	}
	return
}

func makeACopy(original []string) []string {
	tmp := make([]string, len(original))
	copy(tmp, original)
	return tmp
}

// httpGet makes a simple HTTP GET request to the given URL, expecting only "200 OK" status code.
// This is basically for the Instance Metadata Service.
func httpGet(client httputil.RequestExecutor, url string) (body bytes.Buffer, statusCode int, err error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}

	request.Header.Add("Authorization", "Bearer Oracle")
	response, err := client.Do(request)
	if err != nil {
		return
	}
	defer closeBodyIfValid(response)

	if _, err = body.ReadFrom(response.Body); err != nil {
		return
	}

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("HTTP Get failed: URL: %s, Status: %s, Message: %s",
			url, response.Status, body.String())
		return
	}

	return
}

func closeBodyIfValid(httpResponse *http.Response) error {
	if httpResponse == nil || httpResponse.Body == nil {
		return nil
	}

	return httpResponse.Body.Close()
}

func extractTenancyIDFromCertificate(cert *x509.Certificate) string {
	for _, nameAttr := range cert.Subject.Names {
		value := nameAttr.Value.(string)
		if strings.HasPrefix(value, "opc-tenant:") {
			return value[len("opc-tenant:"):]
		}
	}
	return ""
}

func fingerprint(certificate *x509.Certificate) string {
	fingerprint := sha256.Sum256(certificate.Raw)
	return colonSeparatedString(fingerprint)
}

func colonSeparatedString(fingerprint [sha256.Size]byte) string {
	spaceSeparated := fmt.Sprintf("% x", fingerprint)
	return strings.Replace(spaceSeparated, " ", ":", -1)
}
