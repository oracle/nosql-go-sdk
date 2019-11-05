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
	"io/ioutil"
	"testing"
)

func TestParseFQSFromResponse(t *testing.T) {
	file := "testdata/res1.json"
	data, err := ioutil.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}

	_, err = parseFQSFromJSON(data)
	if err != nil {
		t.Error(err)
	}

	// wantRes := []string{
	// 	"http://test.us.oracle.com:7777urn:opc:resource:consumer::all",
	// 	"urn:opc:andc:entitlementid=123456789urn:opc:andc:resource:consumer::all",
	// }

}
