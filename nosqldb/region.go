//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"strings"
)

// Region type for OCI regions.
type Region string

const (
	// RegionCAToronto1 represents the region for Canada Southeast (Toronto).
	RegionCAToronto1 Region = "ca-toronto-1"
	// RegionPHX represents the region for US West (Phoenix).
	RegionPHX Region = "us-phoenix-1"
	// RegionIAD represents the region for US East (Ashburn).
	RegionIAD Region = "us-ashburn-1"
	// RegionFRA represents the region for Germany Central (Frankfurt).
	RegionFRA Region = "eu-frankfurt-1"
	// RegionLHR represents the region for UK South (London).
	RegionLHR Region = "uk-london-1"
	// RegionAPTokyo1 represents the region for Japan East (Tokyo).
	RegionAPTokyo1 Region = "ap-tokyo-1"
	// RegionAPSeoul1 represents the region for South Korea Central (Seoul).
	RegionAPSeoul1 Region = "ap-seoul-1"
	// RegionAPMumbai1 represents the region for India West (Mumbai).
	RegionAPMumbai1 Region = "ap-mumbai-1"
	// RegionEUZurich1 represents the region for Switzerland North (Zurich).
	RegionEUZurich1 Region = "eu-zurich-1"
	// RegionSASaopaulo1 represents the region for Brazil East (Sao Paulo).
	RegionSASaopaulo1 Region = "sa-saopaulo-1"
	// RegionAPSydney1 represents the region for Australia East (Sydney).
	RegionAPSydney1 Region = "ap-sydney-1"
	// RegionUSLangley1 represents the region for Langley.
	RegionUSLangley1 Region = "us-langley-1"
	// RegionUSLuke1 represents the region for Luke.
	RegionUSLuke1 Region = "us-luke-1"

	// RegionUSGovAshburn1 represents the government region for Ashburn.
	RegionUSGovAshburn1 Region = "us-gov-ashburn-1"
	// RegionUSGovChicago1 represents the government region for Chicago.
	RegionUSGovChicago1 Region = "us-gov-chicago-1"
	// RegionUSGovPhoenix1 represents the government region for Phoenix.
	RegionUSGovPhoenix1 Region = "us-gov-phoenix-1"
	// RegionUKGovLondon1 represents the government region for London.
	RegionUKGovLondon1 Region = "uk-gov-london-1"
)

var realm = map[string]string{
	"oc1": "oraclecloud.com",
	"oc2": "oraclegovcloud.com",
	"oc3": "oraclegovcloud.com",
	"oc4": "oraclegovcloud.uk",
}

var regionRealm = map[Region]string{
	RegionPHX:         "oc1",
	RegionIAD:         "oc1",
	RegionFRA:         "oc1",
	RegionLHR:         "oc1",
	RegionCAToronto1:  "oc1",
	RegionAPTokyo1:    "oc1",
	RegionAPSeoul1:    "oc1",
	RegionAPSydney1:   "oc1",
	RegionAPMumbai1:   "oc1",
	RegionEUZurich1:   "oc1",
	RegionSASaopaulo1: "oc1",

	RegionUSLangley1: "oc2",
	RegionUSLuke1:    "oc2",

	RegionUSGovAshburn1: "oc3",
	RegionUSGovChicago1: "oc3",
	RegionUSGovPhoenix1: "oc3",

	RegionUKGovLondon1: "oc4",
}

func (region Region) secondLevelDomain() string {
	if realmID, ok := regionRealm[region]; ok {
		if secondLevelDomain, ok := realm[realmID]; ok {
			return secondLevelDomain
		}
	}

	// Cannot find realm for region, return an empty string.
	return ""
}

// Endpoint returns the NoSQL service endpoint for the region.
// An error is returned if region is invalid.
func (region Region) Endpoint() (string, error) {
	domain := region.secondLevelDomain()
	if len(domain) == 0 {
		return "", fmt.Errorf("region named: %s, is not recognized", string(region))
	}

	// Endpoint format: nosql.{regionID}.oci.{secondLevelDomain}
	return fmt.Sprintf("nosql.%s.oci.%s", string(region), domain), nil
}

// StringToRegion converts a string that represents the region key or
// region identifier to Region type.
// The supported region keys and region identifiers are available at
// https://docs.cloud.oracle.com/iaas/Content/General/Concepts/regions.htm.
func StringToRegion(regionKeyOrID string) (r Region, err error) {
	switch strings.ToLower(regionKeyOrID) {
	case "yyz", "ca-toronto-1":
		r = RegionCAToronto1
	case "phx", "us-phoenix-1":
		r = RegionPHX
	case "iad", "us-ashburn-1":
		r = RegionIAD
	case "fra", "eu-frankfurt-1":
		r = RegionFRA
	case "lhr", "uk-london-1":
		r = RegionLHR
	case "nrt", "ap-tokyo-1":
		r = RegionAPTokyo1
	case "icn", "ap-seoul-1":
		r = RegionAPSeoul1
	case "bom", "ap-mumbai-1":
		r = RegionAPMumbai1
	case "zrh", "eu-zurich-1":
		r = RegionEUZurich1
	case "gru", "sa-saopaulo-1":
		r = RegionSASaopaulo1
	case "syd", "ap-sydney-1":
		r = RegionAPSydney1
	case "us-langley-1":
		r = RegionUSLangley1
	case "us-luke-1":
		r = RegionUSLuke1
	case "us-gov-ashburn-1":
		r = RegionUSGovAshburn1
	case "us-gov-chicago-1":
		r = RegionUSGovChicago1
	case "us-gov-phoenix-1":
		r = RegionUSGovPhoenix1
	case "ltn", "uk-gov-london-1":
		r = RegionUKGovLondon1
	default:
		r = Region(regionKeyOrID)
		err = fmt.Errorf("region named: %s, is not recognized", regionKeyOrID)
	}
	return
}
