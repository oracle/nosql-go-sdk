//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package common

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	{RegionCaToronto1, "nosql.ca-toronto-1.oci.oraclecloud.com"},
	{RegionCaMontreal1, "nosql.ca-montreal-1.oci.oraclecloud.com"},
	{RegionUsPhoenix1, "nosql.us-phoenix-1.oci.oraclecloud.com"},
	{RegionUsAshburn1, "nosql.us-ashburn-1.oci.oraclecloud.com"},
	{RegionEuFrankfurt1, "nosql.eu-frankfurt-1.oci.oraclecloud.com"},
	{RegionEuAmsterdam1, "nosql.eu-amsterdam-1.oci.oraclecloud.com"},
	{RegionUkLondon1, "nosql.uk-london-1.oci.oraclecloud.com"},
	{RegionApTokyo1, "nosql.ap-tokyo-1.oci.oraclecloud.com"},
	{RegionApOsaka1, "nosql.ap-osaka-1.oci.oraclecloud.com"},
	{RegionApSeoul1, "nosql.ap-seoul-1.oci.oraclecloud.com"},
	{RegionApChuncheon1, "nosql.ap-chuncheon-1.oci.oraclecloud.com"},
	{RegionApMumbai1, "nosql.ap-mumbai-1.oci.oraclecloud.com"},
	{RegionApHyderabad1, "nosql.ap-hyderabad-1.oci.oraclecloud.com"},
	{RegionEuZurich1, "nosql.eu-zurich-1.oci.oraclecloud.com"},
	{RegionSaSaopaulo1, "nosql.sa-saopaulo-1.oci.oraclecloud.com"},
	{RegionApSydney1, "nosql.ap-sydney-1.oci.oraclecloud.com"},
	{RegionApMelbourne1, "nosql.ap-melbourne-1.oci.oraclecloud.com"},
	{RegionMeJeddah1, "nosql.me-jeddah-1.oci.oraclecloud.com"},
	// realm: oc2
	{RegionUsLangley1, "nosql.us-langley-1.oci.oraclegovcloud.com"},
	{RegionUsLuke1, "nosql.us-luke-1.oci.oraclegovcloud.com"},
	// realm: oc3
	{RegionUsGovAshburn1, "nosql.us-gov-ashburn-1.oci.oraclegovcloud.com"},
	{RegionUsGovChicago1, "nosql.us-gov-chicago-1.oci.oraclegovcloud.com"},
	{RegionUsGovPhoenix1, "nosql.us-gov-phoenix-1.oci.oraclegovcloud.com"},
	// realm: oc4
	{RegionUkGovLondon1, "nosql.uk-gov-london-1.oci.oraclegovcloud.uk"},
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
		{RegionUsAshburn1, "auth", "auth.us-ashburn-1.oraclecloud.com", false},
		{RegionUsPhoenix1, "foo", "foo.us-phoenix-1.oraclecloud.com", false},
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

func setupOCIRegionsEnv() {
	// OCI environment
	os.Setenv("OCI_REGION_METADATA", `{"realmKey":"OC50","realmDomainComponent":"oraclecloud50.com","regionKey":"ENV","regionIdentifier":"us-fromenv-1"}`)
}

func setupOCIRegionsFile() {
	// OCI config file... we may have to create a ~/.oci directory
	configDir := filepath.Join(getHomeFolder(), ".oci")
	os.Mkdir(configDir, 0777)
	configFile := filepath.Join(getHomeFolder(), ".oci/regions-config.json")
	os.WriteFile(configFile, []byte(`[{"realmKey":"OC55","realmDomainComponent":"oraclecloud55.com","regionKey":"FIL","regionIdentifier":"us-fromfil-1"},{"realmKey":"OC70","realmDomainComponent":"oraclecloud70.com","regionKey":"ZZZ","regionIdentifier":"us-fromzzz-1"}]`), 0777)
}

func unsetOCIRegionsEnv() {
	os.Unsetenv("OCI_REGION_METADATA")
}

func unsetOCIRegionsFile() {
	configFile := filepath.Join(getHomeFolder(), ".oci/regions-config.json")
	os.Remove(configFile)
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

	// OCI environment
	envRegionKeys := []string{
		"env", "us-fromenv-1",
	}
	setupOCIRegionsEnv()
	defer unsetOCIRegionsEnv()
	for _, s := range envRegionKeys {
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%q) got error %v", s, err)
		s = strings.ToUpper(s)
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%q) got error %v", s, err)
		readCfgFile, readEnvVar, visitIMDS = true, true, false
	}
	unsetOCIRegionsEnv()

	// OCI config file
	fileRegionKeys := []string{
		"fil", "us-fromfil-1",
		"zzz", "us-fromzzz-1",
	}
	setupOCIRegionsFile()
	defer unsetOCIRegionsFile()
	for _, s := range fileRegionKeys {
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%q) got error %v", s, err)
		s = strings.ToUpper(s)
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%q) got error %v", s, err)
	}
	unsetOCIRegionsFile()

	// Invalid regions
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

var jsonRegionFile *string = flag.String("regionfile", "", "path to JSON regions file")

// TestEndpointsFromJSON is intended for internal validation of regions code.
// To run a test with a given regions.json file:
// cd common ; go test . -run TestEndpointsFromJSON -regionfile /path/to/regions.json
func TestEndpointsFromJSON(t *testing.T) {
	// if a json file is given in the environment, use it to run tests
	if jsonRegionFile == nil || *jsonRegionFile == "" {
		t.Skip("no regionfile given, skipping internal json-based region tests")
	}

	jFile, err := os.Open(*jsonRegionFile)
	require.NoErrorf(t, err, "can't open region file %s: %v", *jsonRegionFile, err)
	defer jFile.Close()

	byteValue, _ := io.ReadAll(jFile)

	// JSONRealm contains a name and pre/postfixes for urls
	type JSONRealm struct {
		Name           string `json:"name"`
		EndpointPrefix string `json:"epprefix"`
		EndpointSuffix string `json:"epsuffix"`
		AuthPrefix     string `json:"authprefix"`
		AuthSuffix     string `json:"authsuffix"`
	}

	// JSONRealms contains an array of JSONRealm
	type JSONRealms struct {
		JSONRealms []JSONRealm `json:"realms"`
	}

	// JSONRegion contains a name, three letter code, and realm
	type JSONRegion struct {
		Name  string `json:"name"`
		Code  string `json:"tlc"`
		Realm string `json:"realm"`
	}

	// JSONRegions contains an array of JSONRegion
	type JSONRegions struct {
		JSONRegions []JSONRegion `json:"regions"`
	}

	var realms JSONRealms
	err = json.Unmarshal(byteValue, &realms)
	require.NoErrorf(t, err, "Error reading json realms from %s: %v", *jsonRegionFile, err)

	var regions JSONRegions
	err = json.Unmarshal(byteValue, &regions)
	require.NoErrorf(t, err, "Error reading json regions from %s: %v", *jsonRegionFile, err)

	for _, region := range regions.JSONRegions {

		// verify we get a region when we look up based on name
		_, err = StringToRegion(region.Name)
		assert.NoErrorf(t, err, "StringToRegion(%s) got error %v", region.Name, err)
		s := strings.ToUpper(region.Name)
		_, err = StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%s) got error %v", s, err)

		// verify we get a region when we look up based on three letter code
		_, err = StringToRegion(region.Code)
		assert.NoErrorf(t, err, "StringToRegion(%s) got error %v", region.Code, err)
		s = strings.ToUpper(region.Code)
		regionStruct, err := StringToRegion(s)
		assert.NoErrorf(t, err, "StringToRegion(%s) got error %v", s, err)

		// find realm
		var realm JSONRealm
		found := false
		for i := 0; i < len(realms.JSONRealms); i++ {
			if realms.JSONRealms[i].Name == region.Realm {
				realm = realms.JSONRealms[i]
				found = true
				break
			}
		}
		if !assert.True(t, found, "Can't find realm \"%s\" in json realms", region.Realm) {
			continue
		}

		assert.True(t, realm.Name == region.Realm)

		// verify auth URL
		expAuthURL := fmt.Sprintf("%s%s%s", realm.AuthPrefix, region.Name, realm.AuthSuffix)
		actAuthURL, _ := regionStruct.EndpointForService("auth")
		if assert.NotNil(t, actAuthURL, "Auth url for region '%s' is nil", region.Name) {
			assert.Equal(t, expAuthURL, actAuthURL, "Wrong auth URL for region '%s'", region.Name)
		}

		// verify endpoint URL
		expEndpointURL := fmt.Sprintf("%s%s%s", realm.EndpointPrefix, region.Name, realm.EndpointSuffix)
		actEndpointURL, _ := regionStruct.Endpoint()
		if assert.NotNil(t, actEndpointURL, "Endpoint url for region '%s' is nil", region.Name) {
			assert.Equal(t, expEndpointURL, actEndpointURL, "Wrong endpoint URL for region '%s'", region.Name)
		}
	}

}
