//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type regionEP struct {
	region       Region
	wantEndpoint string
}

var regionTests = []regionEP{
	// invalid region
	{Region(""), ""},
	{Region("RegionNA"), ""},
	// realm: oc1
	{RegionCAToronto1, "nosql.ca-toronto-1.oci.oraclecloud.com"},
	{RegionCAMontreal1, "nosql.ca-montreal-1.oci.oraclecloud.com"},
	{RegionPHX, "nosql.us-phoenix-1.oci.oraclecloud.com"},
	{RegionIAD, "nosql.us-ashburn-1.oci.oraclecloud.com"},
	{RegionFRA, "nosql.eu-frankfurt-1.oci.oraclecloud.com"},
	{RegionEUAmsterdam1, "nosql.eu-amsterdam-1.oci.oraclecloud.com"},
	{RegionLHR, "nosql.uk-london-1.oci.oraclecloud.com"},
	{RegionAPTokyo1, "nosql.ap-tokyo-1.oci.oraclecloud.com"},
	{RegionAPOsaka1, "nosql.ap-osaka-1.oci.oraclecloud.com"},
	{RegionAPSeoul1, "nosql.ap-seoul-1.oci.oraclecloud.com"},
	{RegionAPChuncheon1, "nosql.ap-chuncheon-1.oci.oraclecloud.com"},
	{RegionAPMumbai1, "nosql.ap-mumbai-1.oci.oraclecloud.com"},
	{RegionAPHyderabad1, "nosql.ap-hyderabad-1.oci.oraclecloud.com"},
	{RegionEUZurich1, "nosql.eu-zurich-1.oci.oraclecloud.com"},
	{RegionSASaopaulo1, "nosql.sa-saopaulo-1.oci.oraclecloud.com"},
	{RegionAPSydney1, "nosql.ap-sydney-1.oci.oraclecloud.com"},
	{RegionAPMelbourne1, "nosql.ap-melbourne-1.oci.oraclecloud.com"},
	{RegionMEJeddah1, "nosql.me-jeddah-1.oci.oraclecloud.com"},
	// realm: oc2
	{RegionUSLangley1, "nosql.us-langley-1.oci.oraclegovcloud.com"},
	{RegionUSLuke1, "nosql.us-luke-1.oci.oraclegovcloud.com"},
	// realm: oc3
	{RegionUSGovAshburn1, "nosql.us-gov-ashburn-1.oci.oraclegovcloud.com"},
	{RegionUSGovChicago1, "nosql.us-gov-chicago-1.oci.oraclegovcloud.com"},
	{RegionUSGovPhoenix1, "nosql.us-gov-phoenix-1.oci.oraclegovcloud.com"},
	// realm: oc4
	{RegionUKGovLondon1, "nosql.uk-gov-london-1.oci.oraclegovcloud.uk"},
}

func TestEndpointForRegion(t *testing.T) {
	for _, r := range regionTests {
		ep, err := r.region.Endpoint()
		if r.wantEndpoint == "" {
			assert.Errorf(t, err, "Endpoint() should have failed for region %q", string(r.region))
		} else {
			assert.Equalf(t, r.wantEndpoint, ep, "got unexpected endpoint")
		}
	}
}

func TestEndpointForService(t *testing.T) {
	tests := []struct {
		region       Region
		service      string
		wantEndpoint string
		wantErr      bool
	}{
		{RegionIAD, "auth", "auth.us-ashburn-1.oraclecloud.com", false},
		{RegionPHX, "foo", "foo.us-phoenix-1.oraclecloud.com", false},
		{"RegionNA", "bar", "", true},
	}

	for _, r := range tests {
		ep, err := r.region.EndpointForService(r.service)
		if r.wantErr {
			assert.Errorf(t, err, "EndpointForService() should have failed for an invalid region %s", r.region)
		} else {
			assert.Equalf(t, r.wantEndpoint, ep, "EndpointForService() got unexpected service endpoint")
		}
	}
}

func TestStringToRegion(t *testing.T) {
	goodRegionKeyOrIDs := []string{
		"yyz", "ca-toronto-1",
		"yul", "ca-montreal-1",
		"phx", "us-phoenix-1",
		"iad", "us-ashburn-1",
		"fra", "eu-frankfurt-1",
		"ams", "eu-amsterdam-1",
		"lhr", "uk-london-1",
		"nrt", "ap-tokyo-1",
		"kix", "ap-osaka-1",
		"icn", "ap-seoul-1",
		"yny", "ap-chuncheon-1",
		"bom", "ap-mumbai-1",
		"hyd", "ap-hyderabad-1",
		"zrh", "eu-zurich-1",
		"gru", "sa-saopaulo-1",
		"syd", "ap-sydney-1",
		"mel", "ap-melbourne-1",
		"jed", "me-jeddah-1",
		"us-langley-1",
		"us-luke-1",
		"us-gov-ashburn-1",
		"us-gov-chicago-1",
		"us-gov-phoenix-1",
		"ltn", "uk-gov-london-1",
	}

	var err error
	for _, s := range goodRegionKeyOrIDs {
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%q) got error %v", s, err)
		s = strings.ToUpper(s)
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%q) got error %v", s, err)
	}

	badRegionKeyOrIDs := []string{
		"", "oci",
		"xyz", "ca-toronto-x",
		"abcdefghijklmn-opqrst-uvw",
	}
	for _, s := range badRegionKeyOrIDs {
		_, err = StringToRegion(s)
		assert.Errorf(t, err, "StringToRegion(%q) should have failed", s)
	}
}
