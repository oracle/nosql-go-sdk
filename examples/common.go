//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
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
// (1) Create an IAM configuration file (e.g. ~/iam_config).
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
// region, the corresponding region key, region identifier and service endpoint
// is "iad", "us-ashburn-1", https://nosql.us-ashburn-1.oci.oraclecloud.com,
// respectively, use the command:
//
//   cd nosql-go-sdk/bin/examples
//   # Run the basic example.
//   # To run other examples, replace "basic" with the desired example binary.
//   ./basic -config=iam -configFile=~/iam_config iad
//   or
//   ./basic -config=iam -configFile=~/iam_config us-ashburn-1
//   or
//   ./basic -config=iam -configFile=~/iam_config https://nosql.us-ashburn-1.oci.oraclecloud.com
//      Note: you may also include -iamProfileID=<profile> to use a profile other
//            than "DEFAULT", and/or include -iamCompartmentID=<compartmentOCID> to
//            use a compartment ID other than the "tenancy" OCID from the IAM config file.
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
//   ./basic -config=kvstore -configFile=~/kvstore_config https://localhost:8080
//
// Assume the HTTP Proxy is running at http://localhost:8080, use the command to
// run examples against the Oracle NoSQL Database Server that has security
// configuration disabled:
//
//   cd nosql-go-sdk/bin/examples
//   # Run the basic example.
//   # To run other examples, replace "basic" with the desired example binary.
//   ./basic -config=kvstore localhost:8080
//
package examples

import (
	"flag"
	"fmt"
	"os"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/kvstore"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/cloudsim"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// Command line flags.
var (
	config = flag.String("config", "cloudsim", "Specify a configuration for the Oracle NoSQL Database "+
		"that the examples are run against.\nThe available configurations are:\n"+
		"\tiam      : connect to the Oracle NoSQL Cloud Service with IAM configuration\n"+
		"\tkvstore  : connect to the Oracle NoSQL Database Server on-premise\n"+
		"\tcloudsim : connect to the Oracle NoSQL Cloud Simulator\n")

	compartmentID = flag.String("iamCompartmentID", "", "(optional) IAM `compartment ID` to use for requests (defaults to tenantID).")
	profileID     = flag.String("iamProfileID", "", "(optional) `profile ID` to find in IAM config file (defaults to \"DEFAULT\").")

	// configFile specifies the path to the configuration file.
	//
	// This flag is required if connect to the Oracle NoSQL Cloud Service with
	// IAM configuration, or the Oracle NoSQL Database Server on-premise with
	// security enabled.
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
		"This is required with -config=iam, or -config=kvstore when the NoSQL "+
		"Database Server (on-premise) security configuration is enabled.")
)

// Args represents the command line arguments supplied to run the examples.
type Args struct {
	// NoSQL service endpoint.
	Endpoint      string
	config        string
	configFile    string
	profileID     string // (optional) for IAM config files
	compartmentID string // (optional) for IAM
}

// ParseArgs parses and validates command line arguments.
func ParseArgs() *Args {
	flag.CommandLine.SetOutput(os.Stderr)
	flag.Usage = printExampleUsage
	flag.Parse()
	// At least one non-flag argument is required, which specifies the NoSQL service endpoint.
	if flag.NArg() < 1 {
		printExampleUsage()
		os.Exit(1)
	}

	arg0 := flag.Args()[0]
	switch *config {
	case "kvstore", "cloudsim":
	case "iam":
		if len(*configFile) == 0 {
			fmt.Fprintf(os.Stderr, "Please specify a configuration file for %s.\n\n", *config)
			printExampleUsage()
			os.Exit(1)
		}

		// Try to parse arg0 as a region key or region id, if succeeds,
		// get service endpoint for the region.
		region, err := nosqldb.StringToRegion(arg0)
		if err == nil {
			arg0, _ = region.Endpoint()
		}

	default:
		printExampleUsage()
		os.Exit(1)
	}

	return &Args{
		Endpoint:      arg0,
		config:        *config,
		configFile:    *configFile,
		compartmentID: *compartmentID,
	}
}

// CreateAuthorizationProvider creates an appropriate authorization provider
// according to user specified command line arguments.
func CreateAuthorizationProvider(args *Args) (authProvider nosqldb.AuthorizationProvider, err error) {
	switch args.config {
	case "iam":
		return iam.NewSignatureProvider(args.configFile, args.profileID, "", args.compartmentID)
	case "cloudsim":
		return &cloudsim.AccessTokenProvider{TenantID: "ExampleTenantId"}, nil
	case "kvstore":
		if len(args.configFile) == 0 {
			return &kvstore.AccessTokenProvider{}, nil
		}
		return kvstore.NewAccessTokenProviderWithFile(args.configFile)
	default:
		return nil, fmt.Errorf("invalid configuration %s", args.config)
	}
}

func printExampleUsage() {
	fmt.Fprintf(os.Stderr, "Usage:\n%s [OPTIONS] <NoSQL service endpoint or OCI region key/id>\n\n  OPTIONS:\n\n",
		os.Args[0])
	flag.PrintDefaults()
}

// ExitOnError checks the specified error and exits the program if error is not nil.
func ExitOnError(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
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
