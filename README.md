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
recommended to use Go module to manage dependencies for your application.

With Go module support, it is easy to add the Go SDK as a dependency of your
application, `import "github.com/oracle/nosql-go-sdk/nosqldb"` or any sub
packages as needed into your application code and the `go [build|run|test]`
commands will automatically download the necessary dependencies.

## Documentation

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