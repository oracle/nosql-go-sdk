//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// Package examples provides common functions used by the examples that are in sub folders.
//
// To build the examples, use the command:
//
//   cd nosql-go-sdk
//   make build-examples
//
// If build succeeds, the example binaries "basic", "delete" and "index" can
// be found at the nosql-go-sdk/bin/examples directory.
//
// The examples can run with the Oracle NoSQL Cloud Simulator, the Oracle NoSQL
// Cloud Service and the Oracle NoSQL Database on-premise.
//
// 1. Run examples against the Oracle NoSQL Cloud Simulator.
//
// (1) Download the Oracle NoSQL Cloud Simulator and start an instance of the
// Cloud Simulator. See https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/download-oracle-nosql-cloud-simulator.html
// for more details.
//
// (2) Assume the Cloud Simulator is running at localhost:8080, use the command:
//
//   cd nosql-go-sdk/bin/examples
//   # Run the basic example.
//   # To run other examples, replace "basic" with the desired example binary.
//   ./basic localhost:8080
//
// 2. Run examples against the Oracle NoSQL Cloud Service with IAM configuration.
//
// (1) Create an IAM configuration file (e.g. ~/.oci/config).
// See https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm
// for the format and contents of an IAM configuration file.
//
//   [DEFAULT]
//   user=ocid1.user.oc1..aaaaaaaa65vwl75tewwm32rgqvm6i34unq
//   fingerprint=20:3b:97:13:55:1c:5b:0d:d3:37:d8:50:4e:c5:3a:34
//   key_file=~/.oci/oci_api_key.pem
//   pass_phrase=examplephrase
//   tenancy=ocid1.tenancy.oc1..aaaaaaaaba3pv6wuzr4h25vqstifsfdsq
//   region=us-ashburn-1
//
// (2) Assume the Oracle NoSQL Cloud Service is running at the US East (Ashburn)
// region, the corresponding region id is "us-ashburn-1", use the command:
//
//   cd nosql-go-sdk/bin/examples
//   # Run the basic example.
//   # To run other examples, replace "basic" with the desired example binary
//   #
//   # usage 1:
//   # Use the "DEFAULT" profile from the default configuration file ~/.oci/config.
//   # Note the region id must be specified in that profile.
//   ./basic -config=cloud
//   #
//   # usage 2:
//   # Use the "DEFAULT" profile from a file other than ~/.oci/config, and
//   # specify the region id on command line:
//   ./basic -config=cloud -configFile=/path/to/iam_config us-ashburn-1
//   #
//   # usage 3:
//   # You may also include -iamProfileID=<profile> to use a profile other
//   # than "DEFAULT", and/or include -iamCompartmentID=<compartmentOCID> to
//   # use a compartment ID other than the "tenancy" OCID from the IAM config file.
//   ./basic -config=cloud -configFile=/path/to/iam_config -iamProfileID=profile -iamCompartmentID=comp_id us-ashburn-1
//
// 3. Run examples against the Oracle NoSQL Database on-premise.
//
// (1) Download the Oracle NoSQL Database Server and Oracle NoSQL Database
// Proxy (aka HTTP Proxy) at https://www.oracle.com/database/technologies/nosql-database-server-downloads.html.
// Start an instance of the HTTP Proxy that connects to the Oracle NoSQL Database Server.
//
// (2) If the Oracle NoSQL Database Server has security configuration enabled,
// create a configuration file that specifies the username and password used to
// authenticate with the server, for example (~/kvstore_config):
//
//   username=user1
//   password=NoSql00__123456
//
// (3) Assume the HTTP Proxy is running at https://localhost:8080, use the command to
// run examples against the Oracle NoSQL Database Server that has security
// configuration enabled:
//
//   cd nosql-go-sdk/bin/examples
//   # Run the basic example.
//   # To run other examples, replace "basic" with the desired example binary.
//   ./basic -config=onprem -configFile=~/kvstore_config https://localhost:8080
//
// Assume the HTTP Proxy is running at http://localhost:8080, use the command to
// run examples against the Oracle NoSQL Database Server that has security
// configuration disabled:
//
//   cd nosql-go-sdk/bin/examples
//   # Run the basic example.
//   # To run other examples, replace "basic" with the desired example binary.
//   ./basic -config=onprem localhost:8080
//
package examples

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/cloudsim"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/kvstore"
	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// Command line flags.
var (
	config = flag.String("config", "cloudsim", "Specify a configuration for the Oracle NoSQL Database "+
		"that the examples are run against.\nThe available configurations are:\n"+
		"\tcloud    : connect to the Oracle NoSQL Cloud Service with IAM configuration\n"+
		"\tonprem   : connect to the Oracle NoSQL Database Server on-premise\n"+
		"\tcloudsim : connect to the Oracle NoSQL Cloud Simulator\n")

	compartmentID = flag.String("iamCompartmentID", "", "(optional) IAM `compartment ID` to use for requests (defaults to tenantID).")
	profileID     = flag.String("iamProfileID", "", "(optional) `profile ID` to find in IAM config file (defaults to \"DEFAULT\").")

	// configFile specifies the path to the configuration file.
	//
	// This flag is required if connect to the Oracle NoSQL Cloud Service using
	// an IAM configuration file other than the default file ~/.oci/config,
	// or the Oracle NoSQL Database Server on-premise with security enabled.
	//
	// The format and content of the configuration file is dependent on how you
	// connect the Oracle NoSQL Database. The sample files for different
	// configurations are displayed as follows:
	//
	// 1. Connect to the Oracle NoSQL Cloud Service with IAM configuration.
	// See https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm
	// for the format and contents of an IAM configuration file.
	//
	//   [DEFAULT]
	//   user=ocid1.user.oc1..aaaaaaaa65vwl75tewwm32rgqvm6i34unq
	//   fingerprint=20:3b:97:13:55:1c:5b:0d:d3:37:d8:50:4e:c5:3a:34
	//   key_file=~/.oci/oci_api_key.pem
	//   pass_phrase=examplephrase
	//   tenancy=ocid1.tenancy.oc1..aaaaaaaaba3pv6wuzr4h25vqstifsfdsq
	//   region=us-ashburn-1
	//
	// 2. Connect to the Oracle NoSQL Database Server on-premise (with security enabled).
	// Specify the username and password that used to authenticate with the server.
	//
	//   username=user1
	//   password=NoSql00__123456
	//
	configFile = flag.String("configFile", "", "Specify the path to the `configuration file`.\n"+
		"This is required with -config=cloud when using a configuration file "+
		"other than ~/.oci/config, or -config=onprem when the NoSQL "+
		"Database Server (on-premise) security configuration is enabled.")
)

// cmdArgs represents the command line arguments supplied to run the examples.
type cmdArgs struct {
	// NoSQL service endpoint or region id
	endpoint      string
	config        string
	configFile    string
	profileID     string // (optional) for IAM config files
	compartmentID string // (optional) for IAM
}

// parseArgs parses and validates command line arguments.
func parseArgs() *cmdArgs {
	flag.CommandLine.SetOutput(os.Stderr)
	flag.Usage = printExampleUsage
	flag.Parse()

	var arg0 string
	switch *config {
	case "onprem", "cloudsim":
		// A non-flag argument is required, which specifies the NoSQL service endpoint.
		if flag.NArg() < 1 {
			printExampleUsage()
			os.Exit(1)
		}
		arg0 = flag.Args()[0]

	case "cloud":
		// If config file is not specified, use the default file ~/.oci/config.
		if len(*configFile) == 0 {
			*configFile = "~/.oci/config"
		}

		// If there is a non-flag argument, it should specify a region id.
		if flag.NArg() > 0 {
			arg0 = flag.Args()[0]
		}

	default:
		printExampleUsage()
		os.Exit(1)
	}

	return &cmdArgs{
		endpoint:      arg0,
		config:        *config,
		configFile:    *configFile,
		profileID:     *profileID,
		compartmentID: *compartmentID,
	}
}

// CreateClient creates a client using the configurations from command line
// arguments supplied by user.
func CreateClient() (client *nosqldb.Client, err error) {
	args := parseArgs()

	var p nosqldb.AuthorizationProvider
	switch args.config {
	case "cloud":
		p, err = iam.NewSignatureProviderFromFile(args.configFile,
			args.profileID, "", args.compartmentID)
	case "cloudsim":
		p = &cloudsim.AccessTokenProvider{TenantID: "ExampleTenantId"}
	case "onprem":
		if len(args.configFile) > 0 {
			p, err = kvstore.NewAccessTokenProviderFromFile(args.configFile)
		}
	default:
		return nil, fmt.Errorf("invalid configuration %s", args.config)
	}

	if err != nil {
		return
	}

	cfg := nosqldb.Config{
		Mode:                  args.config,
		AuthorizationProvider: p,
	}

	if cfg.IsCloud() {
		region, err := common.StringToRegion(args.endpoint)
		if err != nil {
			cfg.Endpoint = args.endpoint
		} else {
			cfg.Region = region
		}
	} else {
		cfg.Endpoint = args.endpoint
	}

	// If the example client runs on a Windows system or runs against on-premise
	// secure server, skip server certificate verification.
	if _, ok := p.(*kvstore.AccessTokenProvider); ok || runtime.GOOS == "windows" {
		cfg.InsecureSkipVerify = true
	}

	return nosqldb.NewClient(cfg)
}

func printExampleUsage() {
	fmt.Fprintf(os.Stderr, "Usage:\n%s [OPTIONS] <NoSQL service endpoint or OCI region id>\n\n  OPTIONS:\n\n",
		os.Args[0])
	flag.PrintDefaults()
}

// RunQuery executes a query in a loop to be sure that all results have been
// returned. This function returns a single list of results, which is not
// recommended for queries that return a large number of results.
//
// This function mostly demonstrates the need to loop if a query result includes
// a continuation key. This is a common occurrence because each query request
// will only read a limited amount of data before returning a response. It is
// valid, and expected that a query result can have a continuation key but not
// contain any actual results. This occurs if the read limit is reached before
// any results match a predicate in the query. It is also expected for count(*)
// types of queries.
//
// NOTE: for better performance and less throughput consumption prepared queries
// should be used.
func RunQuery(client *nosqldb.Client, query string) (results []*types.MapValue, err error) {
	var prepareReq *nosqldb.PrepareRequest
	var prepareRes *nosqldb.PrepareResult
	var queryReq *nosqldb.QueryRequest
	var queryRes *nosqldb.QueryResult
	var res []*types.MapValue

	prepareReq = &nosqldb.PrepareRequest{
		Statement: query,
	}
	prepareRes, err = client.Prepare(prepareReq)
	if err != nil {
		return
	}

	queryReq = &nosqldb.QueryRequest{
		PreparedStatement: &prepareRes.PreparedStatement,
	}

	for {
		queryRes, err = client.Query(queryReq)
		if err != nil {
			return
		}

		res, err = queryRes.GetResults()
		if err != nil {
			return
		}

		results = append(results, res...)
		if queryReq.IsDone() {
			break
		}
	}

	return
}
