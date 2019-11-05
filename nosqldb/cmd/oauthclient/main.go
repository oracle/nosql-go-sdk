//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// A utility to create a custom OAuth client.
//
// To connect to Oracle NoSQL Database Cloud Service and authenticate with it,
// a client needs to acquire an access token from Oracle Identity Cloud Service
// (IDCS). As a prerequisite, a custom OAuth client named "NoSQLClient" must be
// created first using this utility. The custom client only needs to be created
// once for a tenant.
//
// This utility requires a valid access token from a token file. The token file
// can be downloaded from IDCS admin console following the instructions:
//
//   1. Login to IDCS admin console using your Oracle cloud account.
//   2. In IDCS admin console, choose "Applications" from the button on the top
// left. Find the application named "ANDC", click the button "Generate Access Token",
//   3. In the popup window, check the "Invoke Identity Cloud Service APIs"
// button under "Customized Scopes". Then click on the "Download Token" button.
// If everything goes well, a token file will be generated and downloaded.
// Note that the token in the downloaded token file expires in one hour.
//
// After the token file has been downloaded, run this utility with the command
// below to create an OAuth client:
//
//   ./oauthclient -create -idcsUrl <tenant-specific IDCS URL> -token <token file>
//
// Where the tenant-specific IDCS URL is the IDCS host assigned to the tenant.
// It can be retrieved following the instructions:
//
//   1. Login to the IDCS admin console using your Oracle cloud account.
//   2. If login is successful, you will be navigated to IDCS admin console.
// The address bar of browser displays the admin console URL, which is usually
// in the form of "https://{tenantId}.identity.oraclecloud.com/ui/v1/adminconsole".
// The "https://{tenantId}.identity.oraclecloud.com" portion is the required
// IDCS URL.
//
// If the OAuth client has been created successfully, its client id and secret
// will be printed on the console. A credentials file template named
// "credentials.tmpl" that contains client id and secret will be generated
// in current working directory by default. If you want to save the credentials
// file template in a different directory, use the "-credsdir" flag to specify
// the directory.
//
// This utility can also be used to delete or verify the custom OAuth client.
// To delete the OAuth client, run the command:
//
//   ./oauthclient -delete -idcsUrl <tenant-specific IDCS URL> -token <token file>
//
// To verify the OAuth client, run the command:
//
//   ./oauthclient -verify -idcsUrl <tenant-specific IDCS URL> -token <token file>
//
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth/idcs"
)

var (
	idcsUrl   = flag.String("idcsUrl", "", "tenant specific `IDCS URL`")
	tokenFile = flag.String("token", "", "access token `file path`")
	name      = flag.String("name", "NoSQLClient", "OAuth `client name`.")
	credsdir  = flag.String("credsdir", "", "a `directory` to store credentials file template.")
	timeout   = flag.Duration("timeout", 12*time.Second, "`request timeout`, "+
		"must be an acceptable time.Duration value.")
	verbose = flag.Bool("verbose", false, "turn on verbose mode.")
	create  = flag.Bool("create", false, "create an OAuth client.")
	delete  = flag.Bool("delete", false, "delete an OAuth client.")
	verify  = flag.Bool("verify", false, "verify an OAuth client.")
)

const usage = "\t" +
	"-idcsUrl <tenant specific IDCS URL>\n\t" +
	"-token <access token file path>\n\t" +
	"-name <OAuth client name> (default: NoSQLClient)\n\t" +
	"-credsdir <a directory to store credentials file template> (default: current working directory)\n\t" +
	"-timeout <request timeout, must be an acceptable time.Duration value> (default: 12 seconds)\n\t" +
	"-verbose (if specified, turn on verbose mode. The default is false)\n\t" +
	"-create | -delete | -verify (create, delete or verify an OAuth client, " +
	"only one of the \"-create\", \"-delete\", \"-verify\" flags may be specified at a time)"

var out io.Writer

func main() {
	out = os.Stdout
	flag.CommandLine.SetOutput(out)
	flag.Parse()

	if *idcsUrl == "" || *tokenFile == "" {
		printUsage("")
		return
	}

	numAction := 0
	if *create {
		numAction++
	}
	if *delete {
		numAction++
	}
	if *verify {
		numAction++
	}

	if numAction == 0 {
		printUsage("missing required flags: -create | -delete | -verify, " +
			"please specify one of them.")
		return
	}

	// Only one of the "-create/-delete/-verify" flags can be specified at a time.
	if numAction > 1 {
		printUsage("only one of the \"-create, -delete, -verify\" flags can be specified.")
		return
	}

	c, err := idcs.NewOAuthClient(*name, *idcsUrl, *tokenFile, *credsdir, *timeout, *verbose)
	if err != nil {
		fmt.Fprintf(out, "idcs.NewOAuthClient() failed, got error: %v\n", err)
		return
	}

	switch {
	case *delete:
		c.Delete()
	case *verify:
		c.Verify()
	case *create:
		c.Create()
	}
}

// printUsage prints usage of this utility.
// If the specified message is non-empty, it will be printed before the usage.
func printUsage(message string) {
	if len(message) > 0 {
		fmt.Fprintf(out, message)
		fmt.Fprintln(out)
	}
	fmt.Fprintf(out, "Usage of %s:\n", os.Args[0])
	fmt.Fprintln(out, usage)
}
