//
// Copyright (c) 2019, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package common provides common utilities used for NoSQL client.
package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

// Region type for OCI regions.
type Region string

const (
	instanceMetadataRegionInfoURLV2 = "http://169.254.169.254/opc/v2/instance/regionInfo"

	// Region Metadata Configuration File
	regionMetadataCfgDirName  = ".oci"
	regionMetadataCfgFileName = "regions-config.json"

	// Region Metadata Environment Variable
	regionMetadataEnvVarName = "OCI_REGION_METADATA"

	// Default Realm Environment Variable: not used by NoSQL SDK
	// defaultRealmEnvVarName = "OCI_DEFAULT_REALM"

	// Region Metadata
	regionIdentifierPropertyName     = "regionIdentifier"     // e.g. "ap-sydney-1"
	realmKeyPropertyName             = "realmKey"             // e.g. "oc1"
	realmDomainComponentPropertyName = "realmDomainComponent" // e.g. "oraclecloud.com"
	regionKeyPropertyName            = "regionKey"            // e.g. "SYD"
)

// External region metadata info flags, used to control adding these metadata region info only once.
var readCfgFile, readEnvVar, visitIMDS bool = true, true, false

// realmDomain returns the realm domain for the region.
// ex: region "us-ashburn-1" returns "oraclecloud.com"
func (region Region) realmDomain() string {
	if realmID, ok := regionRealm[region]; ok {
		if realmDomain, ok := realm[realmID]; ok {
			return realmDomain
		}
	}

	// Note: the OCI SDKs check for OCI_DEFAULT_REALM envorinment setting here.
	// The NoSQL SDK does not use this, as the Region should already have
	// a valid domain (or else the StringToRegion method would fail).

	// Cannot find realm for region, return an empty string.
	return ""
}

// Endpoint returns the NoSQL service endpoint for the region.
// An error is returned if region is invalid.
func (region Region) Endpoint() (string, error) {
	domain := region.realmDomain()
	if len(domain) == 0 {
		return "", fmt.Errorf("region named %s is not recognized", string(region))
	}

	// Endpoint format: nosql.{regionID}.oci.{realmDomain}
	return fmt.Sprintf("nosql.%s.oci.%s", string(region), domain), nil
}

// EndpointForService returns an endpoint for a service.
// An error is returned if region is invalid.
func (region Region) EndpointForService(service string) (string, error) {
	domain := region.realmDomain()
	if len(domain) == 0 {
		return "", fmt.Errorf("region named %s is not recognized", string(region))
	}

	return fmt.Sprintf("%s.%s.%s", service, region, domain), nil
}

// StringToRegion converts a string that represents the region key or
// region identifier to Region type.
// The supported region keys and region identifiers are available at
// https://docs.cloud.oracle.com/iaas/Content/General/Concepts/regions.htm.
func StringToRegion(regionKeyOrID string) (r Region, err error) {
	regionStr := strings.ToLower(regionKeyOrID)
	// check if region identifier is provided
	r = Region(regionStr)
	_, ok := regionRealm[r]
	if ok {
		return
	}

	// check if region key is provided
	r, ok = shortNameRegion[regionStr]
	if ok {
		return
	}

	//fmt.Printf("region named: %s, is not recognized from hard-coded region list, will check Region metadata info\n", regionStr)
	r, err = checkAndAddRegionMetadata(regionStr)
	if err == nil {
		return
	}

	return "", fmt.Errorf("region named %s is not recognized", regionKeyOrID)
}

// The following code is taken from the OCI Go SDK, with minor modifications.

// check region info from original map
func checkAndAddRegionMetadata(region string) (Region, error) {
	switch {
	case setRegionMetadataFromCfgFile(&region):
	case setRegionMetadataFromEnvVar(&region):
	case setRegionFromInstanceMetadataService(&region):
	default:
		return "", fmt.Errorf("failed to get region metadata information")
		//return Region(region)
	}
	return Region(region), nil
}

// EnableInstanceMetadataServiceLookup provides the interface to lookup IMDS region info
func EnableInstanceMetadataServiceLookup() {
	//fmt.Println("Set visitIMDS 'true' to enable IMDS Lookup.")
	visitIMDS = true
}

// setRegionMetadataFromEnvVar checks if region metadata env variable is provided, once it's there, parse and added it
// to region map, and it can make sure the env var can only be visited once.
// Once successfully find the expected region(region name or short code), return true, region name will be stored in
// the input pointer.
func setRegionMetadataFromEnvVar(region *string) bool {
	if !readEnvVar {
		//fmt.Println("metadata region env variable had already been checked, no need to check again.")
		return false //no need to check it again.
	}
	// Mark readEnvVar Flag as false since it has already been visited.
	readEnvVar = false
	// check from env variable
	if jsonStr, existed := os.LookupEnv(regionMetadataEnvVarName); existed {
		//fmt.Println("Raw content of region metadata env var:", jsonStr)
		var regionSchema map[string]string
		if err := json.Unmarshal([]byte(jsonStr), &regionSchema); err != nil {
			//fmt.Println("Can't unmarshal env var, the error info is", err)
			return false
		}
		// check if the specified region is in the env var.
		if checkSchemaItems(regionSchema) {
			// set mapping table
			addRegionSchema(regionSchema)
			if *region == "" ||
				regionSchema[regionKeyPropertyName] == *region ||
				regionSchema[regionIdentifierPropertyName] == *region {
				*region = regionSchema[regionIdentifierPropertyName]
				return true
			}
		}
		return false
	}
	//fmt.Println("The Region Metadata Schema wasn't set in env variable - OCI_REGION_METADATA.")
	return false
}

// setRegionMetadataFromCfgFile checks if region metadata config file is provided, once it's there, parse and add all
// the valid regions to region map, the configuration file can only be visited once.
// Once successfully find the expected region(region name or short code), return true, region name will be stored in
// the input pointer.
func setRegionMetadataFromCfgFile(region *string) bool {
	if !readCfgFile {
		//fmt.Println("metadata region config file had already been checked, no need to check again.")
		return false //no need to check it again.
	}
	// Mark readCfgFile Flag as false since it has already been visited.
	readCfgFile = false
	homeFolder := getHomeFolder()
	configFile := filepath.Join(homeFolder, regionMetadataCfgDirName, regionMetadataCfgFileName)
	if jsonArr, ok := readAndParseConfigFile(&configFile); ok {
		added := false
		for _, jsonItem := range jsonArr {
			if checkSchemaItems(jsonItem) {
				addRegionSchema(jsonItem)
				if jsonItem[regionKeyPropertyName] == *region ||
					jsonItem[regionIdentifierPropertyName] == *region {
					*region = jsonItem[regionIdentifierPropertyName]
					added = true
				}
			}
		}
		return added
	}
	return false
}

func readAndParseConfigFile(configFileName *string) (fileContent []map[string]string, ok bool) {
	if content, err := ioutil.ReadFile(*configFileName); err == nil {
		//fmt.Println("Raw content of region metadata config file content:", string(content[:]))
		if err := json.Unmarshal(content, &fileContent); err != nil {
			//fmt.Println("Can't unmarshal config file, the error info is", err)
			return
		}
		ok = true
		return
	}
	//fmt.Println("No Region Metadata Config File provided.")
	return
}

// check map regionRealm's region name, if it's already there, no need to add it.
func addRegionSchema(regionSchema map[string]string) {
	r := Region(strings.ToLower(regionSchema[regionIdentifierPropertyName]))
	if _, ok := regionRealm[r]; !ok {
		// set mapping table
		shortNameRegion[regionSchema[regionKeyPropertyName]] = r
		realm[regionSchema[realmKeyPropertyName]] = regionSchema[realmDomainComponentPropertyName]
		regionRealm[r] = regionSchema[realmKeyPropertyName]
		return
	}
	//fmt.Println("Region {} has already been added, no need to add again.", regionSchema[regionIdentifierPropertyName])
}

// check region schema content if all the required contents are provided
func checkSchemaItems(regionSchema map[string]string) bool {
	if checkSchemaItem(regionSchema, regionIdentifierPropertyName) &&
		checkSchemaItem(regionSchema, realmKeyPropertyName) &&
		checkSchemaItem(regionSchema, realmDomainComponentPropertyName) &&
		checkSchemaItem(regionSchema, regionKeyPropertyName) {
		return true
	}
	return false
}

// check region schema item is valid, if so, convert it to lower case.
func checkSchemaItem(regionSchema map[string]string, key string) bool {
	if val, ok := regionSchema[key]; ok {
		if val != "" {
			regionSchema[key] = strings.ToLower(val)
			return true
		}
		//fmt.Println("Region metadata schema {} is provided,but content is empty.", key)
		return false
	}
	//fmt.Println("Region metadata schema {} is not provided, please update the content", key)
	return false
}

// setRegionFromInstanceMetadataService checks if region metadata can be provided from InstanceMetadataService.
// Once successfully find the expected region(region name or short code), return true, region name will be stored in
// the input pointer.
// setRegionFromInstanceMetadataService will only be checked on the instance, by default it will not be enabled unless
// user explicitly enable it.
func setRegionFromInstanceMetadataService(region *string) bool {
	// example of content:
	// {
	// 	"realmKey" : "oc1",
	// 	"realmDomainComponent" : "oraclecloud.com",
	// 	"regionKey" : "YUL",
	// 	"regionIdentifier" : "ca-montreal-1"
	// }
	// Mark visitIMDS Flag as false since it has already been visited.
	if !visitIMDS {
		//fmt.Println("check from IMDS is disabled or IMDS had already been successfully visited, no need to check again.")
		return false
	}
	content, err := getRegionInfoFromInstanceMetadataService()
	if err != nil {
		//fmt.Printf("Failed to get instance metadata. Error: %v\n", err)
		return false
	}

	// Mark visitIMDS Flag as false since we have already successfully get the region info from IMDS.
	visitIMDS = false

	var regionInfo map[string]string
	err = json.Unmarshal(content, &regionInfo)
	if err != nil {
		//fmt.Printf("Failed to unmarshal the response content: %v \nError: %v\n", string(content), err)
		return false
	}

	if checkSchemaItems(regionInfo) {
		addRegionSchema(regionInfo)
		if *region == "" ||
			regionInfo[regionKeyPropertyName] == *region ||
			regionInfo[regionIdentifierPropertyName] == *region {
			*region = regionInfo[regionIdentifierPropertyName]
		}
	} else {
		//fmt.Println("Region information is not valid.")
		return false
	}

	return true
}

// getRegionInfoFromInstanceMetadataService calls instance metadata service and get the region information
func getRegionInfoFromInstanceMetadataService() ([]byte, error) {
	request, _ := http.NewRequest(http.MethodGet, instanceMetadataRegionInfoURLV2, nil)
	request.Header.Add("Authorization", "Bearer Oracle")

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to call instance metadata service. Error: %v", err)
	}

	statusCode := resp.StatusCode

	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to get region information from response body. Error: %v", err)
	}

	if statusCode != http.StatusOK {
		err = fmt.Errorf("HTTP Get failed: URL: %s, Status: %s, Message: %s",
			instanceMetadataRegionInfoURLV2, resp.Status, string(content))
		return nil, err
	}

	return content, nil
}

func getHomeFolder() string {
	current, e := user.Current()
	if e != nil {
		//Give up and try to return something sensible
		home := os.Getenv("HOME")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return current.HomeDir
}
