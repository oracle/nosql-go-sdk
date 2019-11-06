//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// Package examples provides common functions used by the examples that are in sub folders.
package examples

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/idcs"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// Command line flags.
//
// iamConfigFile is required when running with Oracle NoSQL Cloud Service.
//
// idcsUrl and entitlementId (IDCS params) will be removed in future releases. Please
// use IAM instead.
//
// See https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm for the
// format and contents of an IAM configuration file.
//
// iamPassPhrase is required if the PEM key referenced in the IAM config file
// requires a passphrase.
//
var iamConfigFile = flag.String("iamConfigFile", "", "Specify the path to the Oracle IAM config file")
var iamPassPhrase = flag.String("PassPhrase", "", "(optional) passphrase for PEM key referenced in IAM config file")
var iamCompartmentId = flag.String("iamCompartmentId", "", "(optional) IAM compartmentId to use for requests")
var idcsUrl = flag.String("idcsUrl", "", "IDCS URL which is specific to your tenant.")
var entitlementId = flag.String("entitlementId", "", "IDCS entitlement id")

type Args struct {
	IAMConfigFile    string
	IAMPassPhrase    string
	IAMCompartmentId string
	Endpoint         string
	IDCSUrl          string
	EntitlementId    string
}

// ParseArgs parses command line arguments.
func ParseArgs() *Args {
	flag.CommandLine.SetOutput(os.Stderr)
	flag.Usage = printExampleUsage
	flag.Parse()
	if len(os.Args) < 2 {
		printExampleUsage()
		os.Exit(1)
	}

	return &Args{
		Endpoint:         os.Args[1],
		IAMConfigFile:    *iamConfigFile,
		IAMPassPhrase:    *iamPassPhrase,
		IAMCompartmentId: *iamCompartmentId,
		IDCSUrl:          *idcsUrl,
		EntitlementId:    *entitlementId,
	}
}

// CreateAuthorizationProvider creates an appropriate authorization provider
// according to user specified command line arguments.
func CreateAuthorizationProvider(args *Args) (authProvider nosqldb.AuthorizationProvider, err error) {
	var p nosqldb.AuthorizationProvider
	// Use IAM cloud service.
	if len(args.IAMConfigFile) > 0 { // Use IAM cloud service.
		p, err = iam.NewSignatureProvider(args.IAMConfigFile, args.IAMPassPhrase, args.IAMCompartmentId)
	} else if len(args.IDCSUrl) > 0 { // Use IDCS cloud service.
		if len(args.EntitlementId) > 0 {
			p, err = idcs.NewAccessTokenProviderWithEntitlementId(args.IDCSUrl, args.EntitlementId)
		} else {
			p, err = idcs.NewAccessTokenProviderWithURL(args.IDCSUrl)
		}
	} else { // Use Cloud Simulator.
		p = &ExampleAccessTokenProvider{TenantId: "ExampleTenantId"}
	}

	return p, err
}

func printExampleUsage() {
	fmt.Fprintf(os.Stderr, "Usage:\n\t%s <endpoint>\n"+
		"\t\t[-iamConfigFile <IAM_configfile_path>] [-iamPassPhrase <IAM PEM key passphrase>]\n"+
		"\t\t[-idcsUrl IDCS_URL] [-entitlementId entitlement_id]\n\n", os.Args[0])
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

// ExampleAccessTokenProvider implements the nosqldb.AccessTokenProvider
// interface. It supports running examples against the Oracle NoSQL Cloud
// Simulator by providing a dummy access token.
//
// This should only be used when running with the Cloud Simulator, it is not
// recommended for use in production.
type ExampleAccessTokenProvider struct {
	TenantId string
}

func (p *ExampleAccessTokenProvider) AuthorizationScheme() string {
	return auth.BearerToken
}

func (p *ExampleAccessTokenProvider) AuthorizationString(req auth.Request) (string, error) {
	return auth.BearerToken + " " + p.TenantId, nil
}

func (p *ExampleAccessTokenProvider) Close() error {
	return nil
}

func (p *ExampleAccessTokenProvider) SignHTTPRequest(req *http.Request) error {
	return nil
}
