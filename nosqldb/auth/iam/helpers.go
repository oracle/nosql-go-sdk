// Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.

package iam

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
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
