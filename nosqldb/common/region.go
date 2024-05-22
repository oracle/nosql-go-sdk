//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package common provides common utilities used for NoSQL client.
package common

import (
	"fmt"
	"strings"
)

// Region type for OCI regions.
type Region string

const (
	// OC1 REGIONS

	// RegionIAD represents the region for US East (Ashburn).
	RegionIAD Region = "us-ashburn-1"
	// RegionPHX represents the region for US West (Phoenix).
	RegionPHX Region = "us-phoenix-1"
	// RegionUSSaltLake2 represents the region for US MidWest (Salt Lake).
	RegionUSSaltLake2 Region = "us-saltlake-2"
	// RegionUSSanJose1 represents the region for US West (San Jose).
	RegionUSSanJose1 Region = "us-sanjose-1"
	// RegionUSChicago1 represents the region for US Central (Chicago).
	RegionUSChicago1 Region = "us-chicago-1"
	// RegionCAMontreal1 represents the region for Canada Southeast (Montreal).
	RegionCAMontreal1 Region = "ca-montreal-1"
	// RegionCAToronto1 represents the region for Canada Southeast (Toronto).
	RegionCAToronto1 Region = "ca-toronto-1"
	// RegionLHR represents the region for UK South (London).
	RegionLHR Region = "uk-london-1"
	// RegionUKCardiff1 represents the region for the UK (Cardiff).
	RegionUKCardiff1 Region = "uk-cardiff-1"
	// RegionEUAmsterdam1 represents the region for Netherlands Northwest (Amsterdam).
	RegionEUAmsterdam1 Region = "eu-amsterdam-1"
	// RegionEUMadrid1 represents the region for Spain (Madrid).
	RegionEUMadrid1 Region = "eu-madrid-1"
	// RegionEUMilan1 represents the region for Italy (Milan).
	RegionEUMilan1 Region = "eu-milan-1"
	// RegionEUParis1 represents the region for France (Paris).
	RegionEUParis1 Region = "eu-paris-1"
	// RegionFRA represents the region for Germany Central (Frankfurt).
	RegionFRA Region = "eu-frankfurt-1"
	// RegionEUZurich1 represents the region for Switzerland North (Zurich).
	RegionEUZurich1 Region = "eu-zurich-1"
	// RegionAPTokyo1 represents the region for Japan East (Tokyo).
	RegionAPTokyo1 Region = "ap-tokyo-1"
	// RegionAPOsaka1 represents the region for Japan Central (Osaka).
	RegionAPOsaka1 Region = "ap-osaka-1"
	// RegionAPSeoul1 represents the region for South Korea Central (Seoul).
	RegionAPSeoul1 Region = "ap-seoul-1"
	// RegionAPChuncheon1 represents the region for South Korea North (Chuncheon).
	RegionAPChuncheon1 Region = "ap-chuncheon-1"
	// RegionAPMumbai1 represents the region for India West (Mumbai).
	RegionAPMumbai1 Region = "ap-mumbai-1"
	// RegionAPHyderabad1 represents the region for India South (Hyderabad).
	RegionAPHyderabad1 Region = "ap-hyderabad-1"
	// RegionAPSydney1 represents the region for Australia East (Sydney).
	RegionAPSydney1 Region = "ap-sydney-1"
	// RegionAPMelbourne1 represents the region for Australia Southeast (Melbourne).
	RegionAPMelbourne1 Region = "ap-melbourne-1"
	// RegionSABogota1 represents the region for Colombia
	RegionSABogota1 Region = "sa-bogota-1"
	// RegionSASaopaulo1 represents the region for Brazil East (Sao Paulo).
	RegionSASaopaulo1 Region = "sa-saopaulo-1"
	// RegionSASantiago1 represents the region for Chile (Santiago).
	RegionSASantiago1 Region = "sa-santiago-1"
	// RegionSAValparaiso1 represents the region for Chile (Valparaiso).
	RegionSAValparaiso1 Region = "sa-valparaiso-1"
	// RegionSAVinhedo1 represents the region for Brazil (Vinhedo).
	RegionSAVinhedo1 Region = "sa-vinhedo-1"
	// RegionMEJeddah1 represents the region for Saudi Arabia West (Jeddah).
	RegionMEJeddah1 Region = "me-jeddah-1"
	// RegionMEDubai1 represents the region for Saudi Arabia East (Dubai).
	RegionMEDubai1 Region = "me-dubai-1"
	// RegionMXQueretaro1 represents the region for Mexico (Queretaro).
	RegionMXQueretaro1 Region = "mx-queretaro-1"
	// RegionMXMonterrey1 represents the region for Mexico (Monterrey).
	RegionMXMonterrey1 Region = "mx-monterrey-1"
	// RegionILJerusalem1 represents the region for Israel (Jerusalem).
	RegionILJerusalem1 Region = "il-jerusalem-1"
	// RegionAFJohannesburg represents the region for Johannesburg
	RegionAFJohannesburg Region = "af-johannesburg-1"
	// RegionAPSingapore represents the region for singapore
	RegionAPSingapore Region = "ap-singapore-1"
	// RegionAPSingapore2 represents the second region for singapore
	RegionAPSingapore2 Region = "ap-singapore-2"
	// RegionEUMarseille represents the region for Marseille
	RegionEUMarseille Region = "eu-marseille-1"
	// RegionEUStockholm represents the region for Stockholm
	RegionEUStockholm Region = "eu-stockholm-1"
	// RegionMEAbudhabi represents the region for Abudhabi
	RegionMEAbudhabi Region = "me-abudhabi-1"

	// OC2 REGIONS

	// RegionUSLangley1 represents the region for Langley.
	RegionUSLangley1 Region = "us-langley-1"
	// RegionUSLuke1 represents the region for Luke.
	RegionUSLuke1 Region = "us-luke-1"

	// OC3 REGIONS

	// RegionUSGovAshburn1 represents the government region for Ashburn.
	RegionUSGovAshburn1 Region = "us-gov-ashburn-1"
	// RegionUSGovChicago1 represents the government region for Chicago.
	RegionUSGovChicago1 Region = "us-gov-chicago-1"
	// RegionUSGovPhoenix1 represents the government region for Phoenix.
	RegionUSGovPhoenix1 Region = "us-gov-phoenix-1"

	// OC4 REGIONS

	// RegionUKGovLondon1 represents the government region for London.
	RegionUKGovLondon1 Region = "uk-gov-london-1"
	// RegionUKGovCardiff1 represents the government region for Cardiff.
	RegionUKGovCardiff1 Region = "uk-gov-cardiff-1"

	// OC5 REGIONS

	// RegionUSTacoma1 represents the Tacoma US region
	RegionUSTacoma1 Region = "us-tacoma-1"

	// OC8 REGIONS

	// RegionAPChiyoda1 represents the region for Japan East (Chiyoda).
	RegionAPChiyoda1 Region = "ap-chiyoda-1"
	// RegionAPIbaraki1 represents the region for Japan East (Ibaraki).
	RegionAPIbaraki1 Region = "ap-ibaraki-1"

	// OC9 REGIONS

	// RegionMEDCCMuscat represents the dedicated region for muscat
	RegionMEDCCMuscat Region = "me-dcc-muscat-1"

	// OC10 REGIONS

	// RegionAPDCCCanberra represents the dedicated region for canberra
	RegionAPDCCCanberra Region = "ap-dcc-canberra-1"

	// OC14 REGIONS

	// RegionEUDCCDublin1 represents the dedicated region for Dublin1
	RegionEUDCCDublin1 Region = "eu-dcc-dublin-1"
	// RegionEUDCCDublin2 represents the dedicated region for Dublin2
	RegionEUDCCDublin2 Region = "eu-dcc-dublin-2"
	// RegionEUDCCMilan1 represents the dedicated region for Milan1
	RegionEUDCCMilan1 Region = "eu-dcc-milan-1"
	// RegionEUDCCMilan2 represents the dedicated region for Milan2
	RegionEUDCCMilan2 Region = "eu-dcc-milan-2"
	// RegionEUDCCRating1 represents the dedicated region for Rating1
	RegionEUDCCRating1 Region = "eu-dcc-rating-1"
	// RegionEUDCCRating2 represents the dedicated region for Rating2
	RegionEUDCCRating2 Region = "eu-dcc-rating-2"

	// OC15 REGIONS

	// RegionAPDCCGazipur1 represents the region for Bangladesh
	RegionAPDCCGazipur1 Region = "ap-dcc-gazipur-1"

	// OC16 REGIONS

	// RegionUSWestJordan1 represents the region for west jordan (Utah)
	RegionUSWestJordan1 Region = "us-westjordan-1"

	// OC17 REGIONS

	// RegionUSDCCPhoenix1 represents dedicated region 1 for Phoenix
	RegionDCCPhoenix1 Region = "us-dcc-phoenix-1"
	// RegionUSDCCPhoenix2 represents dedicated region 2 for Phoenix
	RegionDCCPhoenix2 Region = "us-dcc-phoenix-2"
	// RegionUSDCCPhoenix4 represents dedicated region 4 for Phoenix
	RegionDCCPhoenix4 Region = "us-dcc-phoenix-4"

	// OC19 REGIONS

	// RegionEUFrankfurt2 represents the region for Frankfurt 2
	RegionEUFrankfurt2 Region = "eu-frankfurt-2"
	// RegionEUMadrid2 represents the region for Madrid 2
	RegionEUMadrid2 Region = "eu-madrid-2"

	// OC20 REGIONS

	// RegionEUJovanovac1 represents the region for Jovanovac 1 (Serbia)
	RegionEUJovanovac1 Region = "eu-jovanovac-1"

	// OC21 REGIONS

	// RegionMEDCCDoha1 represents the region for Qatar
	RegionMEDCCDoha1 Region = "me-dcc-doha-1"

	// OC22 REGIONS

	// RegionEUJovanovac1 represents the dedicated region for Rome
	RegionEUDCCRome1 Region = "eu-dcc-rome-1"

	// OC24 REGIONS

	// RegionEUDCCZurich1 represents the dedicated region for Zurich
	RegionEUDCCZurich1 Region = "eu-dcc-zurich-1"

	// OC25 REGIONS

	// RegionAPDCCTokyo1 represents the dedicated region for Tokyo
	RegionAPDCCTokyo1 Region = "ap-dcc-tokyo-1"
	// RegionAPDCCOsaka1 represents the dedicated region for Osaka
	RegionAPDCCOsaka1 Region = "ap-dcc-osaka-1"

	// OC26 REGIONS

	// RegionMEAbudhabi3 represents the dedicated region for Abudabhi
	RegionMEAbudhabi3 Region = "me-abudhabi-3"

	// OC27 REGIONS

	// RegionUSDCCSWJordan1 represents the dedicated region for SWJordan1
	RegionUSDCCSWJordan1 Region = "us-dcc-swjordan-1"

	// OC28 REGIONS

	// RegionUSDCCSWJordan1 represents the dedicated region for SWJordan2
	RegionUSDCCSWJordan2 Region = "us-dcc-swjordan-2"

	// OC31 REGIONS

	// RegionAPHobsonville1 represents the dedicated region for Hobsonville
	RegionAPHobsonville1 Region = "ap-hobsonville-1"
)

var realm = map[string]string{
	"oc1":  "oraclecloud.com",
	"oc2":  "oraclegovcloud.com",
	"oc3":  "oraclegovcloud.com",
	"oc4":  "oraclegovcloud.uk",
	"oc5":  "oraclecloud5.com",
	"oc8":  "oraclecloud8.com",
	"oc9":  "oraclecloud9.com",
	"oc10": "oraclecloud10.com",
	"oc14": "oraclecloud14.com",
	"oc15": "oraclecloud15.com",
	"oc16": "oraclecloud16.com",
	"oc17": "oraclecloud17.com",
	"oc19": "oraclecloud.eu",
	"oc20": "oraclecloud20.com",
	"oc21": "oraclecloud21.com",
	"oc22": "psn-pco.it",
	"oc24": "oraclecloud24.com",
	"oc25": "nricloud.jp",
	"oc26": "oraclecloud26.com",
	"oc27": "oraclecloud27.com",
	"oc28": "oraclecloud28.com",
	"oc31": "sovereigncloud.nz",
}

var regionRealm = map[Region]string{
	RegionPHX:            "oc1",
	RegionIAD:            "oc1",
	RegionFRA:            "oc1",
	RegionLHR:            "oc1",
	RegionUSSaltLake2:    "oc1",
	RegionUSSanJose1:     "oc1",
	RegionUSChicago1:     "oc1",
	RegionUKCardiff1:     "oc1",
	RegionCAToronto1:     "oc1",
	RegionCAMontreal1:    "oc1",
	RegionAPTokyo1:       "oc1",
	RegionAPOsaka1:       "oc1",
	RegionAPSeoul1:       "oc1",
	RegionAPChuncheon1:   "oc1",
	RegionAPSydney1:      "oc1",
	RegionAPMumbai1:      "oc1",
	RegionAPHyderabad1:   "oc1",
	RegionAPMelbourne1:   "oc1",
	RegionMEJeddah1:      "oc1",
	RegionMEDubai1:       "oc1",
	RegionMXQueretaro1:   "oc1",
	RegionMXMonterrey1:   "oc1",
	RegionILJerusalem1:   "oc1",
	RegionEUZurich1:      "oc1",
	RegionEUAmsterdam1:   "oc1",
	RegionEUMadrid1:      "oc1",
	RegionEUMilan1:       "oc1",
	RegionEUParis1:       "oc1",
	RegionSABogota1:      "oc1",
	RegionSASaopaulo1:    "oc1",
	RegionSASantiago1:    "oc1",
	RegionSAVinhedo1:     "oc1",
	RegionSAValparaiso1:  "oc1",
	RegionAFJohannesburg: "oc1",
	RegionAPSingapore:    "oc1",
	RegionAPSingapore2:   "oc1",
	RegionEUMarseille:    "oc1",
	RegionMEAbudhabi:     "oc1",
	RegionEUStockholm:    "oc1",

	RegionUSLangley1: "oc2",
	RegionUSLuke1:    "oc2",

	RegionUSGovAshburn1: "oc3",
	RegionUSGovChicago1: "oc3",
	RegionUSGovPhoenix1: "oc3",

	RegionUKGovLondon1:  "oc4",
	RegionUKGovCardiff1: "oc4",

	RegionUSTacoma1: "oc5",

	RegionAPChiyoda1: "oc8",
	RegionAPIbaraki1: "oc8",

	RegionMEDCCMuscat: "oc9",

	RegionAPDCCCanberra: "oc10",

	RegionEUDCCDublin1: "oc14",
	RegionEUDCCDublin2: "oc14",
	RegionEUDCCMilan1:  "oc14",
	RegionEUDCCMilan2:  "oc14",
	RegionEUDCCRating1: "oc14",
	RegionEUDCCRating2: "oc14",

    RegionAPDCCGazipur1: "oc15",

	RegionUSWestJordan1: "oc16",

	RegionDCCPhoenix1: "oc17",
	RegionDCCPhoenix2: "oc17",
	RegionDCCPhoenix4: "oc17",

	RegionEUFrankfurt2: "oc19",
	RegionEUMadrid2:    "oc19",

	RegionEUJovanovac1: "oc20",

	RegionMEDCCDoha1: "oc21",

	RegionEUDCCRome1: "oc22",

	RegionEUDCCZurich1: "oc24",

	RegionAPDCCTokyo1: "oc25",
	RegionAPDCCOsaka1: "oc25",

	RegionMEAbudhabi3: "oc26",

	RegionUSDCCSWJordan1: "oc27",

	RegionUSDCCSWJordan2: "oc28",

	RegionAPHobsonville1: "oc31",
}

var shortNameRegion = map[string]Region{
	"phx": RegionPHX,
	"iad": RegionIAD,
	"aga": RegionUSSaltLake2,
	"sjc": RegionUSSanJose1,
	"ord": RegionUSChicago1,
	"fra": RegionFRA,
	"lhr": RegionLHR,
	"cwl": RegionUKCardiff1,
	"ams": RegionEUAmsterdam1,
	"mad": RegionEUMadrid1,
	"lin": RegionEUMilan1,
	"cdg": RegionEUParis1,
	"zrh": RegionEUZurich1,
	"mel": RegionAPMelbourne1,
	"bom": RegionAPMumbai1,
	"hyd": RegionAPHyderabad1,
	"bog": RegionSABogota1,
	"gru": RegionSASaopaulo1,
	"scl": RegionSASantiago1,
	"vap": RegionSAValparaiso1,
	"vcp": RegionSAVinhedo1,
	"icn": RegionAPSeoul1,
	"yny": RegionAPChuncheon1,
	"nja": RegionAPChiyoda1,
	// Note: ukb is actually Kobe, but OCI uses it for Ibaraki
	"ukb": RegionAPIbaraki1,
	"ibr": RegionAPIbaraki1,
	"nrt": RegionAPTokyo1,
	"kix": RegionAPOsaka1,
	"yul": RegionCAMontreal1,
	"yyz": RegionCAToronto1,
	"jed": RegionMEJeddah1,
	"dxb": RegionMEDubai1,
	"qro": RegionMXQueretaro1,
	"mty": RegionMXMonterrey1,
	"mtz": RegionILJerusalem1,
	"syd": RegionAPSydney1,
	"jnb": RegionAFJohannesburg,
	"sin": RegionAPSingapore,
	"xsp": RegionAPSingapore2,
	"mrs": RegionEUMarseille,
	"auh": RegionMEAbudhabi,
	"arn": RegionEUStockholm,
	"ltn": RegionUKGovLondon1,
	"brs": RegionUKGovCardiff1,
	"lfi": RegionUSLangley1,
	"luf": RegionUSLuke1,
	"tiw": RegionUSTacoma1,
	"ric": RegionUSGovAshburn1,
	"pia": RegionUSGovChicago1,
	"tus": RegionUSGovPhoenix1,
	"mct": RegionMEDCCMuscat,
	"wga": RegionAPDCCCanberra,
	"ork": RegionEUDCCDublin1,
	"snn": RegionEUDCCDublin2,
	"bgy": RegionEUDCCMilan1,
	"mxp": RegionEUDCCMilan2,
	"dus": RegionEUDCCRating1,
	"dtm": RegionEUDCCRating2,
	"dac": RegionAPDCCGazipur1,
	"sgu": RegionUSWestJordan1,
	"ifp": RegionDCCPhoenix1,
	"gcn": RegionDCCPhoenix2,
	"yum": RegionDCCPhoenix4,
	"str": RegionEUFrankfurt2,
	"vll": RegionEUMadrid2,
	"doh": RegionMEDCCDoha1,
	"beg": RegionEUJovanovac1,
	"nap": RegionEUDCCRome1,
	"avz": RegionEUDCCZurich1,
	"tyo": RegionAPDCCTokyo1,
	"uky": RegionAPDCCOsaka1,
	"ahu": RegionMEAbudhabi3,
	"ozz": RegionUSDCCSWJordan1,
	"drs": RegionUSDCCSWJordan2,
	"izq": RegionAPHobsonville1,
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
		return "", fmt.Errorf("region named %s is not recognized", string(region))
	}

	// Endpoint format: nosql.{regionID}.oci.{secondLevelDomain}
	return fmt.Sprintf("nosql.%s.oci.%s", string(region), domain), nil
}

// EndpointForService returns an endpoint for a service.
// An error is returned if region is invalid.
func (region Region) EndpointForService(service string) (string, error) {
	domain := region.secondLevelDomain()
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

	return "", fmt.Errorf("region named %s is not recognized", regionKeyOrID)
}
