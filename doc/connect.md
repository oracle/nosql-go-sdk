This guide describes how to install, configure, and use the Oracle NoSQL
Database Go SDK. There are several supported environments:

1. Oracle NoSQL Database Cloud Service
2. Oracle NoSQL Database Cloud Simulator
3. Oracle NoSQL Database on-premise

## Prerequisites

Th Go SDK requires:
* Go 1.12 or later
* For the Oracle NoSQL Cloud Service
  - An Oracle Cloud Infrastructure account
  - A user created in that account, in a group with a policy that grants the desired permissions.
* For the Oracle NoSQL Database Cloud Simulator:
  - See [Download Oracle NoSQL Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/downloading-oracle-nosql-cloud-simulator.html) to download and start the Cloud Simulator.
* For the on-premise Oracle NoSQL Database:
  - An instance of the database, see [Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html).
  - A running proxy server, see the [Oracle NoSQL Database Proxy and Driver](https://docs.oracle.com/en/database/other-databases/nosql-database/19.3/admin/proxy-and-driver.html) for proxy configuration information.

## Downloading and Installing the SDK

The Go SDK for Oracle NoSQL Database is published as a Go module. It is
recommended to use the Go modules to manage dependencies for your application.

### Configuring GOPROXY

Run `go env GOPROXY` to check if the `GOPROXY` is set correctly for your environment, if not, run the commands:

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

This section describes configuring the SDK for the 3 environments supported.
The areas where the environments and use differ are

1. Authentication and authorization. This is encapsulated in the
   `AuthorizationProvider` interface. The Cloud Service is secure and requires a
   Cloud Service identity as well as authorization for desired operations. The
   Cloud Simulator is not secure at all and requires no identity. The on-premise
   configuration can be either secure or not and it also requires an instance of
   the proxy service to access the database.
2. API differences. Some types and methods are specific to an environment. For
   example, the on-premise configuration includes methods to create namespaces
   and users and these concepts don't exist in the cloud service. Similarly, the
   cloud service includes interfaces to specify and acquire throughput
   information on tables that is not relevant on-premise. Such differences are
   noted in the API documentation.

Skip to the section or sections of interest:

- [Configure for the Cloud Service](#configure-for-the-cloud-service)
- [Configure for the Cloud Simulator](#configure-for-the-cloud-simulator)
- [Configure for the On-Premise Oracle NoSQL Database](#configure-for-the-on-premise-oracle-nosql-database)

### Configure for the Cloud Service

The SDK requires an Oracle Cloud account and a subscription to the Oracle NoSQL
Cloud Service. If you do not already have an Oracle Cloud account you
can start [here](https://www.oracle.com/cloud). Credentials used for
connecting an application are associated with a specific user. If needed, create
a user for the person or system using the API.
See [Adding Users](https://docs.cloud.oracle.com/en-us/iaas/Content/GSG/Tasks/addingusers.htm).

#### Acquire Credentials for the Oracle NoSQL Cloud Service

See [Acquiring Credentials](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/acquiring-credentials.html) for details of credentials you will need to configure an application.

These steps only need to be performed one time for a user. If they have already
been done they can be skipped. You need to obtain the following credentials:

 * Tenancy ID
 * User ID
 * API signing key (private key in PEM format)
 * Private key pass phrase, only needed if the private key is encrypted
 * Fingerprint for the public key uploaded to the user's account

See [Required Keys and OCIDs](https://docs.cloud.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm) for detailed descriptions of the above credentials and the steps you need to perform to obtain them. Specifically:

- [Where to Get the Tenancy’s OCID and User’s OCID](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#Other)
- [How to Generate an API Signing Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How)
- [How to Get the Key’s Fingerprint](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How3)
- [How to Upload the Public Key](https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#How2)


#### Supply Credentials to the Application

Credentials are used to establish the initial connection from your application
to the service. There are 2 ways to supply credentials to the application:

1. Using a configuration file
2. Directly, via API

Both mechanisms use [`iam.SignatureProvider`](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/auth/iam/#SignatureProvider) to handle credentials.
If using a configuration file it's default location is *$HOME/.oci/config*, but
the location can be changed using the API [`iam.NewSignatureProviderFromFile`](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/auth/iam#NewSignatureProviderFromFile).

The format of the configuration file is that of a properties file with the
format of *key=value*, with one property per line. The contents and format are::

```ini
[DEFAULT]
tenancy=<your-tenancy-id>
user=<your-user-id>
fingerprint=<fingerprint-of-your-public-key>
key_file=<path-to-your-private-key-file>
pass_phrase=<optional-pass-phrase-for-key-file>
region=<optional-cloud-service-region>
```

The *Tenancy ID*, *User ID* and *fingerprint* should be acquired using the
instructions above. The path to your private key file is the absolute path of
the RSA private key. The order of the properties does not matter. The
*[DEFAULT]* portion is the *profile*. A configuration file may contain multiple
profiles, you can specify the target profile in the `ociProfile` parameter of
the `iam.NewSignatureProviderFromFile` function.

The `iam.NewSignatureProviderFromFile` function requires a `compartmentID`
parameter. If *compartmentID* is set to an empty string, the default compartment
which is the root compartment of the tenancy is used. If you would like to use
a nested compartment, specify the full compartment path relative to the root
compartment as *compartmentID*. For example, if the nested compartment 
`rootCompartment.compartmentA.compartmentB` is used, the *compartmentID* should 
be set to `compartmentA.compartmentB`.

There are several ways to supply credentials and create a `nosqldb.Config` for cloud service:

1. Provide credentials using a configuration file in the default location, using
the default profile and default compartment:
```go
cfg := nosqldb.Config{
    // This is only required if the "region" property is not specified in ~/.oci/config.
    // This takes precedence over the "region" property when both are specified.
    Region: "us-ashburn-1", 
}
client, err := nosqldb.NewClient(cfg)
...
```

2. Provide credentials using a configuration file in a non-default location and
non-default profile:
```go
sp, err := iam.NewSignatureProviderFromFile("your_config_file_path", "your_profile_name", "", "compartment_id")
if err != nil {
    return
}
cfg := nosqldb.Config{
    AuthorizationProvider: sp,
    // This is only required if the "region" property is not specified in the config file.
    Region: "us-ashburn-1",
}
client, err := nosqldb.NewClient(cfg)
...
```

3. Provide credentials without a configuration file:
```go
privateKeyFile := "/path/to/privateKeyFile"
passphrase := "examplepassphrase"
sp, err := iam.NewRawSignatureProvider("ocid1.tenancy.oc1..tenancy", /* Tenancy */
                                       "ocid1.user.oc1..user", /* User */
                                       "us-ashburn-1", /* service region */,
                                       "fingerprint", /* fingerprint of public key */
                                       "compartmentID", /* compartment ID or name/path */
                                       privateKeyFile , /* private key file */ 
                                       &passphrase /* pass phrase for the private key*/)
if err != nil {
    return
}
cfg := nosqldb.Config{
    AuthorizationProvider: sp,
    // This is only required if the "region" property is not specified in the config file.
    Region: "us-ashburn-1",
}
...
```

### Configure for the Cloud Simulator

The Oracle NoSQL Database Cloud Simulator simulates the cloud service and lets
you write and test applications locally without accessing Oracle NoSQL Database
Cloud Service. Before using the Cloud Service it is recommended that users start
with the Cloud Simulator to become familiar with the interfaces supported by
the SDK.

See [Download Oracle NoSQL Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/downloading-oracle-nosql-cloud-simulator.html) to download and start the Cloud Simulator.

The Cloud Simulator does not require the credentials and authentication
information required by the Oracle NoSQL Cloud Service. The Cloud
Simulator should not be used for deploying applications or important data.

To connect an application to a cloud simulator, specify the endpoint at which
the cloud simulator is running, and specify *cloudsim* as configuration mode.

```go
cfg := nosqldb.Config{
    Mode:     "cloudsim",
    Endpoint: "localhost:8080",
}
client, err := nosqldb.NewClient(cfg)
...
```

### Configure for the On-Premise Oracle NoSQL Database

The on-premise configuration requires a running instance of the Oracle NoSQL
database. In addition a running proxy service is required. See [Oracle NoSQL
Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html) for downloads,
and see [Information about the proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/19.3/admin/proxy-and-driver.html) for proxy configuration information.

#### Configure for non-secure on-premise Oracle NoSQL Database

To connect an application to a non-secure NoSQL database, specify the endpoint at
which the Proxy server is running, and specify *onprem* as configuration mode.

```go
cfg := nosqldb.Config{
    Mode:     "onprem",
    Endpoint: "http://exampleHostServer:8080",
}
client, err := nosqldb.NewClient(cfg)
...
```

#### Configure for secure on-premise Oracle NoSQL Database

To connect an application to a secure NoSQL database, you need to provide user
credentials used to authenticate with the server. If the Proxy server is configured
with a self-signed certificate or a certificate that is not trusted by the
the default system CA, you also need to specifiy *CertPath* and *ServerName* for
the certificate path and server name used to verify server's certificate.

```go
cfg := nosqldb.Client{
    Mode:     "onprem",
    Endpoint: "https://exampleHostServer",
    Username: "driverUser",
    Password: "ExamplePassword__123",
    HTTPConfig: httputil.HTTPConfig{
        CertPath: "/path/to/server-certificate",
        ServerName: "exampleHostServer", // should match the CN subject value from the certificate
    },
}
client, err := nosqldb.NewClient(cfg)
```

Or if you run applications with the secure NoSQL database in a test environment, 
you may not want to verify the certificate presented by server, you can specify the
*InsecureSkipVerify* paramater.

```go
cfg := nosqldb.Client{
    Mode:     "onprem",
    Endpoint: "https://exampleHostServer",
    Username: "driverUser",
    Password: "ExamplePassword__123",
    HTTPConfig: httputil.HTTPConfig{
        InsecureSkipVerify: true,
    },
}
client, err := nosqldb.NewClient(cfg)
```
