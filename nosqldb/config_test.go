//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func TestEndpoint(t *testing.T) {
	tests := []struct {
		input string
		valid bool
		want  string
	}{
		{"", false, ""},
		{"://", false, ""},
		{"http://", false, ""},
		{"http://:8080", false, ""},
		{"https://:443", false, ""},
		{"ftp://localhost", false, ""},
		{"httpX://localhost", false, ""},
		{"https://foo.com:xyz", false, ""},
		{"https://foo.com:-10", false, ""},
		{"https://foo.com:9090:876", false, ""},
		{"hTTp://http.foo.com:9090", true, "http://http.foo.com:9090"},
		{"HTTPs://https.foo.com:9090", true, "https://https.foo.com:9090"},
		{"localhost", true, "https://localhost:443"},
		{"localhost:443", true, "https://localhost:443"},
		{"https://localhost", true, "https://localhost:443"},
		{"https://localhost:443", true, "https://localhost:443"},
		{"https://localhost:443/", true, "https://localhost:443"},
		{"localhost:80", true, "http://localhost:80"},
		{"localhost:8080", true, "http://localhost:8080"},
		{"http://localhost:8080", true, "http://localhost:8080"},
		{"hTtp://foo.com:8080", true, "http://foo.com:8080"},
		{"hTtpS://foo.com:443", true, "https://foo.com:443"},
		{"ndcs.uscom-east-1.oraclecloud.com", true, "https://ndcs.uscom-east-1.oraclecloud.com:443"},
		{"https://ndcs.uscom-east-1.oraclecloud.com", true, "https://ndcs.uscom-east-1.oraclecloud.com:443"},
		{"https://ndcs.uscom-east-1.oraclecloud.com:443", true, "https://ndcs.uscom-east-1.oraclecloud.com:443"},
		{"https://ndcs.uscom-east-1.oraclecloud.com:443/", true, "https://ndcs.uscom-east-1.oraclecloud.com:443"},
		{"192.168.0.12", true, "https://192.168.0.12:443"},
		{"http://192.168.0.12", true, "http://192.168.0.12:8080"},
		{"http://[fe80::1%25en0]", true, "http://[fe80::1%25en0]:8080"},
		{"https://[fe80::1]/", true, "https://[fe80::1]:443"},
	}

	for _, r := range tests {
		cfg := Config{
			Endpoint: r.input,
		}
		err := cfg.parseEndpoint()
		if err != nil && r.valid {
			t.Errorf("endpoint %q is valid, but got error %q", r.input, err.Error())
			continue
		}

		if !r.valid {
			if err == nil {
				t.Errorf("endpoint %q is invalid, but got nil error", r.input)
			}
			continue
		}

		if cfg.Endpoint != r.want {
			t.Errorf("parseEndpoint(%q) got %s; want %s", r.input, cfg.Endpoint, r.want)
		}
	}
}

func TestRequestConfig(t *testing.T) {
	defaultConfig := &RequestConfig{
		RequestTimeout:      defaultRequestTimeout,
		TableRequestTimeout: defaultTableRequestTimeout,
		SecurityInfoTimeout: defaultSecurityInfoTimeout,
		Consistency:         defaultConsistency,
	}
	tests := []struct {
		input *RequestConfig
		want  *RequestConfig
	}{
		{
			input: nil,
			want:  defaultConfig,
		},
		{
			input: &RequestConfig{},
			want:  defaultConfig,
		},
		{
			input: &RequestConfig{
				RequestTimeout:      6 * time.Second,
				TableRequestTimeout: 6 * time.Second,
				SecurityInfoTimeout: 15 * time.Second,
				Consistency:         types.Eventual,
			},
			want: &RequestConfig{
				RequestTimeout:      6 * time.Second,
				TableRequestTimeout: 6 * time.Second,
				SecurityInfoTimeout: 15 * time.Second,
				Consistency:         types.Eventual,
			},
		},
		{
			input: &RequestConfig{
				RequestTimeout:      6 * time.Second,
				TableRequestTimeout: 6 * time.Second,
			},
			want: &RequestConfig{
				RequestTimeout:      6 * time.Second,
				TableRequestTimeout: 6 * time.Second,
				SecurityInfoTimeout: defaultSecurityInfoTimeout,
				Consistency:         defaultConsistency,
			},
		},
		{
			input: &RequestConfig{
				SecurityInfoTimeout: 15 * time.Second,
				Consistency:         types.Absolute,
			},
			want: &RequestConfig{
				RequestTimeout:      defaultRequestTimeout,
				TableRequestTimeout: defaultTableRequestTimeout,
				SecurityInfoTimeout: 15 * time.Second,
				Consistency:         types.Absolute,
			},
		},
	}

	for i, r := range tests {
		if v := r.input.DefaultRequestTimeout(); v != r.want.RequestTimeout {
			t.Errorf("Test %d: got request timeout: %s; want %s", i+1, v, r.want.RequestTimeout)
		}

		if v := r.input.DefaultTableRequestTimeout(); v != r.want.TableRequestTimeout {
			t.Errorf("Test %d: got table request timeout: %s; want %s", i+1, v, r.want.TableRequestTimeout)
		}

		if v := r.input.DefaultSecurityInfoTimeout(); v != r.want.SecurityInfoTimeout {
			t.Errorf("Test %d: got security info timeout: %s; want %s", i+1, v, r.want.SecurityInfoTimeout)
		}

		if v := r.input.DefaultConsistency(); v != r.want.Consistency {
			t.Errorf("Test %d: got consistency: %s; want %s", i+1, v, r.want.Consistency)
		}

	}
}
