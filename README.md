# Oracle NoSQL Database Go SDK

This is the Go SDK for Oracle NoSQL Database. The SDK provides APIs,
[documentation](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb) and
[examples](https://github.com/oracle/nosql-go-sdk/tree/master/examples) to help
developers write Go applications that connect to
[the Oracle NoSQL Database Cloud Service](https://www.oracle.com/database/nosql-cloud.html),
[the Oracle NoSQL Database](https://www.oracle.com/database/technologies/related/nosql.html)
and to [the Oracle NoSQL Cloud Simulator](https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html).

This project is open source and maintained by Oracle Corp.

## Prerequisites
- Go 1.12 or later
  - Download a [Go](https://golang.org/dl/) 1.12+ binary release suitable for your system.
  - Install on your system following the [installation instructions](https://golang.org/doc/install).
  - Go for Oracle Linux 7 can be installed via `yum`: [Go Packages for Oracle Linux](http://yum.oracle.com/oracle-linux-golang.html).
  - Add the directory that contains the `go` executable into your system PATH, for example:
    ```bash
    export PATH=/usr/local/go/bin:$PATH
    ```
- Oracle NoSQL Database. Use one of the options:
  - Subscribe to the [Oracle NoSQL Database Cloud Service](https://www.oracle.com/database/nosql-cloud.html).
  - Download the [Oracle NoSQL Cloud Simulator](https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html).
  - Download the Oracle NoSQL Database Server (on-premise) and the Oracle NoSQL
Database Proxy (aka HTTP Proxy) at [here](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html).
    >***NOTE***: The on-premise configuration requires a running instance of the HTTP Proxy,
    see the [Oracle NoSQL Database Proxy and Driver](https://docs.oracle.com/en/database/other-databases/nosql-database/19.3/admin/proxy-and-driver.html) for HTTP proxy configuration information.


## Installation
The Go SDK for Oracle NoSQL Database is published as a Go module. It is
recommended to use the Go modules to manage dependencies for your application.

### Configuring GOPROXY

Run `go env GOPROXY` to check if the `GOPROXY` is set correctly for your environment,
if not, run the commands:

* If using `go1.12`

```sh
export GO111MODULE="on"
export GOPROXY="https://proxy.golang.org"
```

* If using `go1.13+`:

```go
go env -w GO111MODULE=on
go env -w GOPROXY="https://proxy.golang.org,direct"
```

### Downloading the SDK

Using Go modules, you don't need to download the Go SDK explicitly. Your typical
workflow is:

* Add import statements for the SDK packages to your application code as needed. Such as:

```go
import "github.com/oracle/nosql-go-sdk/nosqldb"
```

* Run `go build` or `go test` commands to build or test your application, these
commands will automatically add new dependencies as needed to satisfy imports,
updating *go.mod* and downloading the new dependencies.

When needed, more specific versions of the SDK can be downloaded explicitly 
with commands such as:

```sh
# download a tagged version v1.2.3
go get github.com/oracle/nosql-go-sdk@v1.2.3

# download the latest version on the development branch
go get github.com/oracle/nosql-go-sdk@dev

# download a specific version
go get github.com/oracle/nosql-go-sdk@af6e224
```

## Configuring the SDK

This section describes configuring the SDK for the 3 environments supported:

- **NoSQL DB Cloud Service**
- **Cloud Simulator**
- **NoSQL DB On-Premise**

The areas where the environments and use differ are:

- **Authentication and authorization.** This is encapsulated in the AuthorizationProvider interface.
  - The **Cloud Service** is secure and requires a Cloud Service identity as well as authorization for desired operations.
  - The **Cloud Simulator** is not secure at all and requires no identity.
  - The **On-Premise** configuration can be either secure or not, and also requires an instance of the NoSQL DB Proxy service to access the on-premise database.
- **API differences.** Some types and methods are specific to an environment. For example, the on-premise configuration includes methods to create namespaces and users and these concepts don’t exist in the cloud service. Similarly, the cloud service includes interfaces to specify and acquire throughput information on tables that is not relevant on-premise. Such differences are noted in the API documentation.

Before using the Cloud Service, it is recommended that users start with the Cloud Simulator to become familiar with the interfaces supported by the SDK.

### Configure for the Cloud Service

The SDK requires an Oracle Cloud account and a subscription to the Oracle NoSQL Database Cloud Service. If you do not already have an Oracle Cloud account you can start [here](https://cloud.oracle.com/home).

#### Acquire Credentials for the Oracle NoSQL Database Cloud Service

Several pieces of information comprise your credentials used by the Oracle NoSQL Database Cloud Service:

- Tenancy ID
- User ID
- Fingerprint
- Private Key File
- Passphrase (optional)
- Region (optional)

Information about how to acquire this information is found in the [Required Keys and OCIDs](https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm) page. Specifically, these topics can be found on that page:

- [Where to Get the Tenancy’s OCID and User’s OCID](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#Other)
- [How to Generate an API Signing Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How)
- [How to Get the Key’s Fingerprint](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How3)
- [How to Upload the Public Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How2)

#### Supplying Cloud Credentials to an Application

Credentials are used to establish the initial connection from your application to the service. The way to supply the credentials is to use a credentials file, which by default is found in `$HOME/.oci/config` but the location can be specified in the API calls (see below).

The format of the file is that of a properties file with the format of `key=value`, with one property per line. The contents and format are:

```ini
[DEFAULT]
tenancy=<your-tenancy-id>
user=<your-user-id>
fingerprint=<fingerprint-of-your-public-key>
key_file=<path-to-your-private-key-file>
pass_phrase=<optional-passphrase>
region=<optional-region-identifier>
```

Details of the configuration file can be found on the [SDK and Configuration File](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) page. Note that multiple profiles can exist (using the `[PROFILENAME]` properties file convention) and can be selected using the API (see example below).

The **Tenancy ID**, **User ID** and **fingerprint** should be acquired using the instructions above. The path to your private key file is the absolute path of the RSA private key. The order of the properties does not matter.

The **region** is only required if [`nosqldb.Config.Region`](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Config) is not set. It should specify the region of the NoSQL cloud service you are connecting to (for example: `us-phoenix-1`). Defined regions are listed in [Region godocs](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Region). For more information on regions, see [Regions and Availability Domains](https://docs.cloud.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm).

The **pass_phrase** is only required if the RSA key file itself requires a passphrase. If not supplied in the file, it may also be supplied in the API calls directly (see below).

#### Connecting an Application to the Cloud Service

The first step in any Oracle NoSQL Database Cloud Service `go` application is to create a `nosqldb.Client` handle used to send requests to the service. Instances of the Client handle are safe for concurrent use by multiple goroutines and intended to be shared in a multi-goroutines application. The handle is configured using your credentials and other authentication information:

- **profile**: The name of the proerties section to use. If not specified, **"DEFAULT"** is used.
- **passphrase**: If the private key requires a passphrase, and the passphrase is not included in the config file, it can be supplied at runtime.
- **compartment**: The compartment name or OCID for NoSQL DB table operations. If not supplied, the Tenancy OCID is used as the default.

The following code example shows how to connect to the cloud service:

```go
import (
    "fmt"

    "github.com/oracle/nosql-go-sdk/nosqldb"
    "github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
)

...

provider, err := iam.NewSignatureProviderFromFile(cfgfile, profile, passphrase, compartment)
if err != nil {
    fmt.Printf("failed to create new SignatureProvider: %v\n", err)
    return
}
cfg := nosqldb.Config{
    Region:                "us-phoenix-1",
    AuthorizationProvider: provider,
}
client, err := nosqldb.NewClient(cfg)
if err != nil {
    fmt.Printf("failed to create a NoSQL client: %v\n", err)
    return
}
defer client.close()

// use client for all NoSQL DB operations
// ...

```

### Configure for the Cloud Simulator

The Oracle NoSQL Cloud Simulator is a useful way to use this SDK to connect to a local server that supports the same protocol.

See [Download the Oracle NoSQL Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/download-oracle-nosql-cloud-simulator.html) to download and start the Cloud Simulator.

The Cloud Simulator should not be used for deploying production applications or important data, but it does allow for rapid development and testing.

The Cloud Simulator does not require the credentials and authentication information required by the Oracle NoSQL Database Cloud Service, so connecting to it is simple:

```go
import (
    "fmt"

    "github.com/oracle/nosql-go-sdk/nosqldb"
)

...

cfg := nosqldb.Config{
    Endpoint: "http://localhost:8080",
    Mode:     "cloudsim",
}
client, err := nosqldb.NewClient(cfg)
if err != nil {
    fmt.Printf("failed to create a NoSQL client: %v\n", err)
    return
}
defer client.Close()
// Perform database operations using client APIs.
// ...
```

### Configure for the On-Premise Oracle NoSQL Database

The on-premise configuration requires a running instance of the Oracle NoSQL database. In addition a running proxy service is required. See [Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html) for downloads, and see [Information about the proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/19.5/admin/proxy-and-driver.html) for proxy configuration information.

In this case, the `Endpoint` config parameter should point to the NoSQL proxy host and port location.

If running a secure store, a user identity must be created in the store (separately) that has permission to perform the required operations of the application, such as manipulating tables and data. If the secure server has installed a certificate that is self-signed or is not trusted by the default system CA, specify *InsecureSkipVerify* to instruct the client to skip verifying server's certificate, or specify the *CertPath* and *ServerName* that used to verify server's certificate and hostname.

```go
import (
    "fmt"

    "github.com/oracle/nosql-go-sdk/nosqldb"
    "github.com/oracle/nosql-go-sdk/nosqldb/httputil"
)
...
cfg := nosqldb.Config{
    Endpoint: "https://nosql.mycompany.com:8080",
    Mode:     "onprem",
    Username: "testUser",
    Password: []byte("F;0s2M0;-Tdr"),
    // Specify InsecureSkipVerify
    HTTPConfig: httputil.HTTPConfig{
        InsecureSkipVerify: true,
    },
    // Alternatively, specify the CertPath and ServerName
    //
    // HTTPConfig: httputil.HTTPConfig{
    //     CertPath: "/path/to/certificate-used-by-server",
    //     ServerName: "nosql.mycompany.com", // set to the "CN" subject value from the certificate
    // },
}
client, err := nosqldb.NewClient(cfg)
if err != nil {
    fmt.Printf("failed to create a NoSQL client: %v\n", err)
    return
}
defer client.Close()
// Perform database operations using client APIs.
// ...
```

If the store is not secure then the username and password are not required:

```go
...
cfg := nosqldb.Config{
    Endpoint: "http://nosql.mycompany.com:8080",
    Mode:     "onprem",
}
client, err := nosqldb.NewClient(cfg)
if err != nil {
    fmt.Printf("failed to create a NoSQL client: %v\n", err)
    return
}
defer client.Close()
// Perform database operations using client APIs.
// ...
```

## Simple Example

Below is a complete simple example program that opens a `nosqldb.Client` handle, creates a simple table if it does not already exist, puts, gets, and deletes a row, then drops the table.

The example program can be used as a template, please modify the program according to your environment.

```go
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// createClient creates a client with the supplied configurations.
//
// This function encapsulates environmental differences and returns a
// client handle to use for data operations.
func createClient() (*nosqldb.Client, error) {

	// EDIT: set the configuration mode for your target environment.
	//
	//   Use "cloud" for the Oracle NoSQL Database Cloud Service
	//   Use "cloudsim" for the Oracle NoSQL Cloud Simulator
	//   Use "onprem" for the Oracle NoSQL Database on-premise
	//
	mode := "cloudsim"

	var cfg nosqldb.Config
	switch mode {
	default:
		return nil, fmt.Errorf("the specified configuration mode %q is not supported", mode)

	case "cloudsim":
		// EDIT: set desired endpoint for the Cloud Simulator accordingly
		// in your environment.
		cfg = nosqldb.Config{
			Endpoint: "http://localhost:8080",
			Mode:     "cloudsim",
		}

	case "onprem":
		// EDIT: set desired endpoint for the Proxy server accordingly
		// in your environment.
		cfg = nosqldb.Config{
			Endpoint: "http://localhost:8080",
			Mode:     "onprem",
		}

	case "cloud":
		// EDIT:
		// This program demonstrates two approaches for supplying configurations
		// for cloud service.
		//
		//   1. Directly in this program.
		//   2. Use a configuration file.
		//
		// If you use the second approach, set "useConfigFile" to true.
		useConfigFile := false

		// EDIT: set desired region id for NoSQL cloud service. e.g. us-ashburn-1
		region := "<nosql-service-region-identifier>"

		// EDIT: set desired compartment id.
		// Set to an empty string to use the default compartment, that is
		// the root compartment of the tenancy.
		// If using a nested compartment, specify the full compartment path
		// relative to the root compartment as compartmentID.
		// For example, if using rootCompartment.compartmentA.compartmentB, the
		// compartmentID should be set to compartmentA.compartmentB.
		compartmentID := "<optional-compartmentID>"

		if !useConfigFile {
			// EDIT: set the following information accordingly:
			//
			//   tenancy OCID
			//   user OCID
			//   fingerprint of your public key
			//   your private key file or private key content
			//   passphrase of your private key
			//
			tenancy := "<your-tenancy-OCID>"
			user := "<your-user-OCID>"
			fingerprint := "<fingerprint-of-your-public-key>"
			privateKey := "<path-to-your-private-key-file-or-private-key-content>"
			// If passphrase is not required, use an empty string
			privateKeyPassphrase := "<optional-passphrase>"

			sp, err := iam.NewRawSignatureProvider(tenancy, user, region, fingerprint,
				compartmentID, privateKey, &privateKeyPassphrase)
			exitOnError(err, "Cannot create a Signature Provider")

			cfg = nosqldb.Config{
				Mode:                  "cloud",
				Region:                nosqldb.Region(region),
				AuthorizationProvider: sp,
			}

		} else {
			// EDIT: modify and save the following content into the
			// configuration file $HOME/.oci/config.
			//
			// [DEFAULT]
			// tenancy=<your-tenancy-id>
			// user=<your-user-id>
			// fingerprint=<fingerprint-of-your-public-key>
			// key_file=<path-to-your-private-key-file>
			// pass_phrase=<optional-passphrase>
			//
			sp, err := iam.NewSignatureProviderFromFile("~/.oci/config", "", "", compartmentID)
			exitOnError(err, "Cannot create a Signature Provider")
			cfg = nosqldb.Config{
				Mode:                  "cloud",
				Region:                nosqldb.Region(region),
				AuthorizationProvider: sp,
			}
		}
	}

	client, err := nosqldb.NewClient(cfg)
	return client, err
}

func exitOnError(err error, msg string) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, msg+": ", err)
	os.Exit(1)
}

func main() {
	client, err := createClient()
	exitOnError(err, "Can't create NoSQL DB client")
	defer client.Close()

	// Creates a simple table with a LONG key and a single STRING field.
	tableName := "go_quick_start"
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id LONG, "+
		"test_data STRING, "+
		"PRIMARY KEY(id))",
		tableName)
	tableReq := &nosqldb.TableRequest{
		Statement: stmt,
		TableLimits: &nosqldb.TableLimits{
			ReadUnits:  50,
			WriteUnits: 50,
			StorageGB:  2,
		},
	}
	tableRes, err := client.DoTableRequest(tableReq)
	exitOnError(err, "Can't initiate CREATE TABLE request")

	// The create table request is asynchronous, wait for table creation to complete.
	_, err = tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	exitOnError(err, "Error finishing CREATE TABLE request")
	fmt.Println("Created table ", tableName)

	// put a simple set of string data
	mapVals := types.ToMapValue("id", 12345)
	mapVals.Put("test_data", "This is a sample string of test data")
	putReq := &nosqldb.PutRequest{
		TableName: tableName,
		Value:     mapVals,
	}
	putRes, err := client.Put(putReq)
	exitOnError(err, "Can't put single row")
	fmt.Printf("Put row: %v\nresult: %v\n", putReq.Value.Map(), putRes)

	// get data back
	key := &types.MapValue{}
	key.Put("id", 12345)
	getReq := &nosqldb.GetRequest{
		TableName: tableName,
		Key:       key,
	}
	getRes, err := client.Get(getReq)
	exitOnError(err, "Can't get single row")
	if getRes.RowExists() {
		fmt.Printf("Got row: %v\n", getRes.ValueAsJSON())
	} else {
		fmt.Printf("The row does not exist.\n")
	}

	// Delete the row
	delReq := &nosqldb.DeleteRequest{
		TableName: tableName,
		Key:       key,
	}
	delRes, err := client.Delete(delReq)
	exitOnError(err, "Can't delete single row")
	if delRes.Success {
		fmt.Printf("Deleted key: %v\nresult: %v\n", jsonutil.AsJSON(delReq.Key.Map()), delRes)
	}

	// Drop the table
	dropReq := &nosqldb.TableRequest{
		Statement: "DROP TABLE IF EXISTS " + tableName,
	}
	tableRes, err = client.DoTableRequestAndWait(dropReq, 60*time.Second, time.Second)
	exitOnError(err, "Can't drop created table")
	fmt.Println("Dropped table ", tableName)
}
```

Create a directory `quickstart`, save the example program as `quickstart.go` in the directory.
Run the example program with the commands:

```sh
cd quickstart

# initialize a new module for the example
go mod init example.com/quickstart

# build the example
go build -o quickstart

# run the example
./quickstart
```

For more details on how to configure applications to connect to different supported
environments, see [How to Connect Tutorial](https://github.com/oracle/nosql-go-sdk/blob/master/doc/connect.md).

## API Reference

The online [godoc](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb) has
information on using the Go SDK as well as an API reference describing the
packages, types and methods.

In the documentation, the Oracle NoSQL Database Cloud Service and the Oracle
NoSQL Cloud Simulator are referred to as the **cloud service** while the Oracle
NoSQL Database is referred to as **on-premise**. Packages, types and methods are
noted if they are only relevant to a specific environment.

## Examples

Examples can be found at the [**examples**](https://github.com/oracle/nosql-go-sdk/blob/master/examples)
directory. Examples include simple, standalone programs that show the Go API usages.
They include comments about how they can be configured and run in the different
supported environments. See the example source code and the
[example documentation](https://godoc.org/github.com/oracle/nosql-go-sdk/examples)
for details on how to build and run the examples.

## Help

There are a few ways to get help or report issues:
- Open an issue in the [Issues](https://github.com/oracle/nosql-go-sdk/issues) page.
- Post your question on the [Oracle NoSQL Database Community](https://community.oracle.com/community/groundbreakers/database/nosql_database).
- [Email to nosql\_sdk\_help\_grp@oracle.com](mailto:nosql_sdk_help_grp@oracle.com)

When requesting help please be sure to include as much detail as possible,
including version of the SDK and **simple**, standalone example code as needed.

## Changes

See the [Changelog](https://github.com/oracle/nosql-go-sdk/blob/master/CHANGELOG.md).

## Contributing

The nosql-go-sdk is an open source project. See
[Contributing](https://github.com/oracle/nosql-go-sdk/blob/master/CONTRIBUTING.md)
for information on how to contribute to the project.

## License

Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.

This SDK is licensed under the Universal Permissive License 1.0. See
[LICENSE](https://github.com/oracle/nosql-go-sdk/blob/master/LICENSE.txt) for
details.
