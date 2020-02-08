# Oracle NoSQL Database Go SDK

This is the Go SDK for Oracle NoSQL Database. The SDK provides APIs,
[documentation](https://godoc.org/github.com/oracle/nosql-go-sdk/) and
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
recommended to use the Go module to manage dependencies for your application.

With Go module support, it is easy to add the Go SDK as a dependency of your
application: `import "github.com/oracle/nosql-go-sdk/nosqldb"` or any sub
packages as needed into your application code, and the `go [build|run|test]`
commands will automatically download the necessary dependencies.

## Configuring the SDK

This section describes configuring the SDK for the 3 environments supported:
- NoSQL DB Cloud Service
- Cloud Simulator
- NoSQL DB On-Premise

The areas where the environments and use differ are:

1. **Authentication and authorization.** This is encapsulated in the AuthorizationProvider interface. The Cloud Service is secure and requires a Cloud Service identity as well as authorization for desired operations. The Cloud Simulator is not secure at all and requires no identity. The on-premise configuration can be either secure or not and it also requires an instance of the proxy service to access the database.
2. **API differences.** Some classes and methods are specific to an environment. For example, the on-premise configuration includes methods to create namespaces and users and these concepts don’t exist in the cloud service. Similarly, the cloud service includes interfaces to specify and acquire throughput information on tables that is not relevant on-premise.

Before using the Cloud Service it is recommended that users start with the Cloud Simulator to become familiar with the interfaces supported by the SDK.

### Configure for the Cloud Service

The SDK requires an Oracle Cloud account and a subscription to the Oracle NoSQL Database Cloud Service. If you do not already have an Oracle Cloud account you can start [here](https://cloud.oracle.com/home).

Acquire Credentials for the Oracle NoSQL Database Cloud Service

Several pieces of information comprise your credentials used by the Oracle NoSQL Database Cloud Service:

- Tenancy ID
- User ID
- Fingerprint
- Private Key File
- Region

Information about how to acquire this information is found in the [Required Keys and OCIDs](https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm) page. Specifically, these topics can be found on that page:

- [Where to Get the Tenancy’s OCID and User’s OCID](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#Other)
- [How to Generate an API Signing Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How)
- [How to Get the Key’s Fingerprint](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How3)
- [How to Upload the Public Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How2)

### Supplying Cloud Credentials to an Application

Credentials are used to establish the initial connection from your application to the service. The way to supply the credentials is to use a credentials file, which by default is found in `$HOME/.oci/config` but the location can be specified in the API calls (see below).

The format of the file is that of a properties file with the format of `key=value`, with one property per line. The contents and format are:

```
[DEFAULT]
tenancy=<your-tenancy-id>
user=<your-user-id>
fingerprint=<fingerprint-of-your-public-key>
key_file=<path-to-your-private-key-file>
region=<region-identifier>
```

Details of the configuration file can be found on the [SDK and Configuration File](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) page. Note that multiple profiles can exist (using the `[PROFILENAME]` properties file convention) and can be selected using the api (see example below).

The **Tenancy ID**, **User ID** and **fingerprint** should be acquired using the instructions above. The path to your private key file is the absolute path of the RSA private key. The order of the properties does not matter.

The **region** should specify the region of the NoSQL cloud service you are connecting to (for example: `us-phoenix-1`). Defined regions are listed in [Region godocs](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Region). For more information on regions, see [Regions and Availability Domains](https://docs.cloud.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm).

### Connecting an Application to the Cloud Service

The first step in any Oracle NoSQL Database Cloud Service `go` application is to create a `nosqldb.Client` handle used to send requests to the service. Instances of the Client handle are safe for concurrent use by multiple goroutines and intended to be shared in a multi-goroutines application. The handle is configured using your credentials and other authentication information:

- **profile**: The name of the proerties section to use. If not specified, **"DEFAULT"** is used.
- **passphrase**: If the private key requires a passphrase, and the passphrase is not included in the config file, it can be supplied at runtime.
- **compartment**: The compartment name or OCID for NoSQL DB table operations. If not supplied, the Tenancy OCID is used as the default.

The following code example shows how to connect to the cloud service:

```
import (
    "github.com/oracle/nosql-go-sdk/nosqldb"
    "github.com/oracle/nosql-go-sdk/nosqldb/auth/iam"
)

...

provider, err := iam.NewSignatureProvider(cfgfile, profile, passphrase, compartment)
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

## Configure for the Cloud Simulator

The Oracle NoSQL Cloud Simulator is a useful way to use this SDK to connect to a local server that supports the same protocol. The Cloud Simulator requires Java 8 or higher.

See [Download the Oracle NoSQL Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/download-oracle-nosql-cloud-simulator.html) to download and start the Cloud Simulator.

The Cloud Simulator should not be used for deploying production applications or important data, but it does allow for rapid development and testing.

The Cloud Simulator does not require the credentials and authentication information required by the Oracle NoSQL Database Cloud Service, so connecting to it is simple:

```
import (
    "github.com/oracle/nosql-go-sdk/nosqldb"
)

...

cfg := nosqldb.Config{
    Endpoint: "https://localhost:8080",
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


## Configure for the On-Premise Oracle NoSQL Database

The on-premise configuration requires a running instance of the Oracle NoSQL database. In addition a running proxy service is required. See [Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html) for downloads, and see [Information about the proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/19.5/admin/proxy-and-driver.html) for proxy configuration information.

In this case, the `Endpoint` config paramter should point to the NoSQL proxy host and port location.

If running a secure store, a user identity must be created in the store (separately) that has permission to perform the required operations of the application, such as manipulating tables and data. If the store is not secure then the username and password are not required:

```
import (
    "github.com/oracle/nosql-go-sdk/nosqldb"
)

...

cfg := nosqldb.Config{
    Endpoint: "https://nosql.mycompany.com:8080",
    Mode:     "onprem",
    Username: "testUser",
    Password: []byte("F;0s2M0;-Tdr"),
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

below is a complete simple example program that opens a cloudsim Client handle, creates a simple table if it does not already exist, puts, gets, and deletes a row, then drops the table.

```
package main

import (
    "fmt"
    "os"
    "time"
    "github.com/oracle/nosql-go-sdk/nosqldb"
    "github.com/oracle/nosql-go-sdk/nosqldb/types"
    "github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
)

func ExitOnError(err error, msg string) {
    if err == nil {
        return
    }
    fmt.Fprintln(os.Stderr, msg, err)
    os.Exit(1)
}

func main() {

    cfg := nosqldb.Config{
        Endpoint: "http://localhost:8080",
        Mode:     "cloudsim",
    }
    client, err := nosqldb.NewClient(cfg)
    ExitOnError(err, "Can't create NoSQL DB client")
    defer client.Close()

    // Creates a simple table with a LONG key and a single STRING field.
    tableName := "exampleData"
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
            StorageGB:  20,
        },
    }
    tableRes, err := client.DoTableRequest(tableReq)
    ExitOnError(err, "Can't initiate CREATE TABLE request")

    // The create table request is asynchronous, wait for table creation to complete.
    _, err = tableRes.WaitForState(client, types.Active, 60*time.Second, time.Second)
    ExitOnError(err, "Error finishing CREATE TABLE request")
    fmt.Println("Created table ", tableName)

    // put a simple set of string data
    mapVals := types.ToMapValue("id", "12345")
    mapVals.Put("test_data", "This is a sample string of test data")
    putReq := &nosqldb.PutRequest{
        TableName: tableName,
        Value:       mapVals,
    }
    putRes, err := client.Put(putReq)
    ExitOnError(err, "Can't put single row")
    fmt.Printf("Put row: %v\nresult: %v\n", putReq.Value.Map(), putRes)

    // get data back
    key := &types.MapValue{}
    key.Put("id", "12345");
    getReq := &nosqldb.GetRequest{
        TableName: tableName,
        Key:       key,
    }
    getRes, err := client.Get(getReq)
    ExitOnError(err, "Can't get single row")
    fmt.Printf("Got row: %v\n", getRes.Value)

    // Delete the row
    delReq := &nosqldb.DeleteRequest{
        TableName: tableName,
        Key:       key,
    }
    delRes, err := client.Delete(delReq)
    ExitOnError(err, "Can't delete single row")
    fmt.Printf("Deleted key: %v\nresult: %v\n", jsonutil.AsJSON(delReq.Key.Map()), delRes)

    // Drop the table
    dropReq := &nosqldb.TableRequest{
        Statement: "DROP TABLE IF EXISTS " + tableName,
    }
    tableRes, err = client.DoTableRequestAndWait(dropReq, 60*time.Second, time.Second)
    ExitOnError(err, "Can't drop created table")
    fmt.Println("Dropped table ", tableName)
}
```

## API Reference

The online [godoc](https://godoc.org/github.com/oracle/nosql-go-sdk/) has
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
- Open a ticket on [My Oracle Support](https://support.oracle.com/).

## Changes

See the [Changelog](https://github.com/oracle/nosql-go-sdk/blob/master/CHANGELOG.md).

## Development

The [development readme](https://github.com/oracle/nosql-go-sdk/blob/master/README-DEV.md)
has information about running tests and other development activities.

## Contributing

The nosql-go-sdk is an open source project. See
[Contributing](https://github.com/oracle/nosql-go-sdk/blob/master/CONTRIBUTING.md)
for information on how to contribute to the project.

## License

Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.

This SDK is licensed under the Universal Permissive License 1.0. See
[LICENSE](https://github.com/oracle/nosql-go-sdk/blob/master/LICENSE.txt) for
details.
