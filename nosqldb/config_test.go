//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/assert"
)

func TestValidateConfig(t *testing.T) {
	ep := "https://nosql.us-ashburn-1.oci.oraclecloud.com"
	tests := []struct {
		desc                   string
		mode, endpoint, region string
		ok                     bool
	}{
		// unsupported configuration mode
		{
			desc:     "unsupported configuration mode xyz",
			mode:     "xyz",
			endpoint: ep,
			region:   "us-ashburn-1",
			ok:       false,
		},
		// cloud service
		{
			desc:     "specify Endpoint and Region for mode=cloud",
			mode:     "cloud",
			endpoint: ep,
			region:   "us-ashburn-1",
			ok:       false,
		},
		{
			desc:     "specify Endpoint and Region for cloud service",
			mode:     "",
			endpoint: ep,
			region:   "us-ashburn-1",
			ok:       false,
		},
		{
			desc:     "specify a Region for cloud service",
			mode:     "cloud",
			endpoint: "",
			region:   "us-ashburn-1",
			ok:       true,
		},
		// This is ok as the region may be specified in OCI config.
		{
			desc:     "neither an Endpoint nor Region is specified for cloud service",
			mode:     "cloud",
			endpoint: "",
			region:   "",
			ok:       true,
		},
		// cloud simulator
		{
			desc:     "specify Endpoint and Region for cloud simulator",
			mode:     "cloudsim",
			endpoint: ep,
			region:   "us-ashburn-1",
			ok:       false,
		},
		{
			desc:     "specify an Endpoint for cloud simulator",
			mode:     "cloudsim",
			endpoint: "http://localhost:8080",
			region:   "",
			ok:       true,
		},
		{
			desc:     "specify a Region for cloud simulator",
			mode:     "cloudsim",
			endpoint: "",
			region:   "us-ashburn-1",
			ok:       false,
		},
		{
			desc:     "neither an Endpoint nor Region is specified for cloud simulator",
			mode:     "cloudsim",
			endpoint: "",
			region:   "",
			ok:       false,
		},
		// on-premise
		{
			desc:     "specify Endpoint and Region for on-premise server",
			mode:     "onprem",
			endpoint: ep,
			region:   "us-ashburn-1",
			ok:       false,
		},
		{
			desc:     "specify an Endpoint for on-premise server",
			mode:     "onprem",
			endpoint: "http://localhost:8080",
			region:   "",
			ok:       true,
		},
		{
			desc:     "specify a Region for on-premise server",
			mode:     "onprem",
			endpoint: "",
			region:   "us-ashburn-1",
			ok:       false,
		},
		{
			desc:     "neither an Endpoint nor Region is specified for on-premise server",
			mode:     "onprem",
			endpoint: "",
			region:   "",
			ok:       false,
		},
	}

	for _, r := range tests {
		c := &Config{
			Mode:     r.mode,
			Endpoint: r.endpoint,
			Region:   Region(r.region),
		}

		err := c.validate()
		if r.ok {
			assert.NoErrorf(t, err, "%: should be valid, got error", r.desc, err)
		} else {
			assert.Errorf(t, err, "%: should be invalid", r.desc)
		}
	}
}

// TestRegionConfig tests the cases that specify a region for cloud service
// either in Config.Region, OCI configuration file, or specify the service
// endpoint explicitly.
func TestRegionConfig(t *testing.T) {
	//  Profile DEFAULT specifies eu-zurich-1 as region.
	sp0, err := iam.NewSignatureProviderFromFile("testdata/dummy_config", "DEFAULT", "", "")
	if !assert.NoErrorf(t, err, "cannot create a signature provider using DEFAULT profile") {
		return
	}

	// Profile USER01 does not specify a region.
	sp1, err := iam.NewSignatureProviderFromFile("testdata/dummy_config", "USER01", "", "")
	if !assert.NoErrorf(t, err, "cannot create a signature provider using profile USER01") {
		return
	}

	// Profile USER02 specifies an invalid region "bad-xyz-1".
	sp2, err := iam.NewSignatureProviderFromFile("testdata/dummy_config", "USER02", "", "")
	if !assert.NoErrorf(t, err, "cannot create a signature provider using profile USER02") {
		return
	}

	tests := []struct {
		desc         string
		cfg          *Config
		wantEndpoint string
		ok           bool
	}{
		{
			desc:         "specify a Config.Region that is invalid",
			cfg:          &Config{Region: "ab-cde-1"},
			wantEndpoint: "",
			ok:           false,
		},
		{
			desc:         "specify an invalid region in OCI config file",
			cfg:          &Config{Region: "", AuthorizationProvider: sp2},
			wantEndpoint: "",
			ok:           false,
		},
		{
			desc:         "neither Config.Region nor region in OCI config is specified",
			cfg:          &Config{Region: "", AuthorizationProvider: sp1},
			wantEndpoint: "",
			ok:           false,
		},
		{
			desc:         "use the specified Config.Region",
			cfg:          &Config{Region: "us-ashburn-1", AuthorizationProvider: sp1},
			wantEndpoint: "nosql.us-ashburn-1.oci.oraclecloud.com",
			ok:           true,
		},
		{
			desc:         "use the specified Config.Region rather than region in OCI config",
			cfg:          &Config{Region: "us-ashburn-1", AuthorizationProvider: sp0},
			wantEndpoint: "nosql.us-ashburn-1.oci.oraclecloud.com",
			ok:           true,
		},
		{
			desc:         "use the region specified in OCI config",
			cfg:          &Config{Region: "", AuthorizationProvider: sp0},
			wantEndpoint: "nosql.eu-zurich-1.oci.oraclecloud.com",
			ok:           true,
		},
		{
			desc:         "use the specified Config.Endpoint",
			cfg:          &Config{Endpoint: "nosql.us-phoenix-1.oci.oraclecloud.com", AuthorizationProvider: sp1},
			wantEndpoint: "nosql.us-phoenix-1.oci.oraclecloud.com",
			ok:           true,
		},
	}

	for _, r := range tests {
		err := r.cfg.setDefaults()
		if !r.ok {
			assert.Errorf(t, err, "%s: should have failed", r.desc)
		} else {
			if assert.NoErrorf(t, err, "%s: got unexpected error", r.desc, err) {
				// Do not compare with r.cfg.Endpoint as it has been normalized to https://host:443
				gotHost := r.cfg.host
				assert.Equalf(t, r.wantEndpoint, gotHost, "%s: got unexpected endpoint", r.desc)
			}
		}
	}
}

func TestParseEndpoint(t *testing.T) {
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

	// Validate and parse the specified endpoint.
	for _, r := range tests {
		cfg := Config{
			Endpoint: r.input,
		}
		err := cfg.parseEndpoint()
		if !r.valid {
			assert.Errorf(t, err, "parseEndpoint(ep=%q) should have failed")
			continue
		}

		if assert.NoErrorf(t, err, "parseEndpoint(ep=%q) got error %v", r.input, err) {
			assert.Equalf(t, r.want, cfg.Endpoint, "parseEndpoint(ep=%q) got unexpected result", r.input)
		}
	}

	// Validate specified Region and parse endpoint from Region.
	for _, r := range regionTests {
		cfg := Config{
			Region: r.region,
		}

		err := cfg.parseEndpoint()
		if r.wantEndpoint == "" {
			assert.Errorf(t, err, "parseEndpoint() should have failed for region %q", string(r.region))
		} else {
			want := "https://" + r.wantEndpoint + ":443"
			assert.Equalf(t, want, cfg.Endpoint, "got unexpected endpoint")
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
