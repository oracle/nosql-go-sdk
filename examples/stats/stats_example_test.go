//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package main

import (
	"flag"
	"testing"

	"github.com/oracle/nosql-go-sdk/nosqldb"
)

func TestCreateClientRequiresExplicitTLSVerificationBypass(t *testing.T) {
	registeredFlag := flag.Lookup("insecureSkipVerify")
	if registeredFlag == nil {
		t.Fatal("insecureSkipVerify flag is not registered")
	}
	if registeredFlag.DefValue != "false" {
		t.Fatalf("insecureSkipVerify default = %q, want false", registeredFlag.DefValue)
	}

	originalMode := *configMode
	originalConfigFile := *configFile
	originalInsecureSkipVerify := *insecureSkipVerify
	t.Cleanup(func() {
		*configMode = originalMode
		*configFile = originalConfigFile
		*insecureSkipVerify = originalInsecureSkipVerify
	})

	*configMode = "onprem"
	*configFile = ""

	tests := []struct {
		name               string
		insecureSkipVerify bool
	}{
		{name: "verification enabled by default", insecureSkipVerify: false},
		{name: "verification bypass explicitly enabled", insecureSkipVerify: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			*insecureSkipVerify = test.insecureSkipVerify
			client, err := createClient("https://localhost:8080", nosqldb.StatsProfileNone)
			if err != nil {
				t.Fatalf("create client: %v", err)
			}
			defer client.Close()

			if client.InsecureSkipVerify != test.insecureSkipVerify {
				t.Fatalf("InsecureSkipVerify = %t, want %t",
					client.InsecureSkipVerify, test.insecureSkipVerify)
			}
		})
	}
}
