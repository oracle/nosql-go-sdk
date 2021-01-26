# Contributing to the Oracle NoSQL Database Cloud Service Go SDK

*Copyright (c) 2019, 2021 Oracle and/or its affiliates. All rights reserved.*

The target readers of this document are those who want to contribute to the
Oracle NoSQL Database Go SDK project, including but not limited to contributing
to the source code, examples, tests and documents.

## Before contributing

### Sign the OCA

Before you become a contributor, please read and sign
[The Oracle Contributor Agreement](https://www.oracle.com/technetwork/community/oca-486395.html)
(OCA), see [Contributing](https://github.com/oracle/nosql-go-sdk/blob/master/CONTRIBUTING.md)
for more details.

After you signed the OCA, make sure that your Git tool is configured to create
commits using your chosen name and e-mail address as they appear in the
[OCA Signatories list](https://www.oracle.com/technetwork/community/oca-486395.html#list).
You can configure Git globally (or locally as you prefer) with the commands:
```bash
git config --global user.email you@example.com
git config --global user.name YourName
```

### Check the issue tracker

When you find any issues with the Go SDK or want to propose a change, please
check the [Issues](https://github.com/oracle/nosql-go-sdk/issues) page
first, this helps prevent duplication of effort. If the issue is already being
tracked, feel free to participate in the discussion.

### Open a new issue

If you find an issue that is not tracked in the [Issues](https://github.com/oracle/nosql-go-sdk/issues)
page, feel free to open a new one, describe the issue, discuss your plans or
proposed changes.
All contributions should be connected to an issue except for the trivial changes.

## Contributing

Follow the [Github Flow](https://guides.github.com/introduction/flow/) when you
work on a change for Go SDK.

Before you open a pull request, make sure:
- Add unit tests for the code changes you made.
- Use gofmt to format the code.
- Run golint and govet, fix the warnings.
- Run all tests.
  - It is important to run all tests and make sure they pass with both the
Oracle NoSQL Cloud Simulator and the Oracle NoSQL Database (on-premise).
If you have a subscription to the Oracle NoSQL Database Cloud Service, it would
be great if you can run the tests with the Cloud Service as well.

## Run Tests

The Go SDK project contains an internal package `github.com/oracle/nosql-go-sdk/internal/test`
that provides test utility functions and configurations, you can use or improve
that package when you add unit tests for your code change.

There are two JSON configuration files `cloudsim_config.json` and `onprem_config.json`
in that package that are used to configure the test with the Oracle NoSQL Cloud
Simulator and the Oracle NoSQL Database on-premise respectively.
The test configuration parameters are described as follows:

- General configuration parameters

| Parameter    | Required?   | Notes   |
| ------------ | ----------- | ------- |
| clientConfig.mode     | Yes | cloudsim: test with the Oracle NoSQL Cloud Simulator. |
|                       |     | onprem: test with the Oracle NoSQL Database on-premise. |
| clientConfig.endpoint | Yes | Specify the NoSQL service endpoint. |
| version | Yes | Specify the Oracle NoSQL Database on-premise release version or the Oracle NoSQL Cloud Simulator release version. |
|         |     | This is used to determine the tests that only apply to specific release versions. |
| tablePrefix | No | Specify the prefix for table names. |
|             |    | You can use a prefix to discriminate between tables created by different test users. |
| reCreateTables | No | A bool flag that indicates if tables should be drop-and-recreate before tests. |
| dropTablesOnTearDown | No | A bool flag that indicates if tables should be dropped after tests. |

- Configuration parameters used when test against on-premise Oracle NoSQL Database Server that has security configuration enabled

| Parameter    | Required?   | Notes   |
| ------------ | ----------- | ------- |
| clientConfig.username | Yes  | Specify the username used to authenticate with the Oracle NoSQL Database Server. |
| clientConfig.password | Yes  | Specify a base64 encoded string of the password used to authenticate with the Oracle NoSQL Database Server. |
|                       |      | For example, the output of \`echo -n "Password" \| base64` could be used as the value. |
| clientConfig.httpConfig.certPath   | No | Specify the certificate used by the Oracle NoSQL Database Server. |
| clientConfig.httpConfig.serverName | No | Specify the server name of the Oracle NoSQL Database Server. |
|                                    |    | This should match the "CN" subject value from the certificate specified by "certPath". |
| clientConfig.httpConfig.insecureSkipVerify | No | A bool flag that indicates whether to skip server certificate verification. |
|                                            |    | If this is set to true, the "certPath" and "serverName" are ignored. |

>**Note**: The test configurations include NoSQL client configurations represented by the `clientConfig` parameter.  For a complete list of
configuration parameters for `clientConfig`, please see [nosqldb.Config](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Config).

### Run tests with the Oracle NoSQL Cloud Simulator
- Start a Cloud Simulator instance.
- Modify *nosql-go-sdk/internal/test/cloudsim_config.json*, set the *endpoint*
based on how the Cloud Simulator was started.
- Run tests with the command:
```bash
cd nosql-go-sdk

# Run all tests
make cloudsim-test

# Run all tests in the "TestDataOperations" test suite
make cloudsim-test testcases=TestDataOperations

# Run a specific test in the "TestDataOperations" test suite
make cloudsim-test testcases=TestDataOperations/TestPutGetDelete
```

### Run tests with Oracle NoSQL Database on-premise
- Start an instance of the Oracle NoSQL Database proxy running against an Oracle NoSQL Database on-premise.
- Modify *nosql-go-sdk/internal/test/onprem_config.json*, set the *endpoint*
based on how the database proxy was started.
If running against a secure Database Server and Database proxy, set the
*username* and *password* parameters. The user must be created separately using
Oracle NoSQL Database Admin CLI and must have permission to create and use tables.
See [User Management](https://docs.oracle.com/en/database/other-databases/nosql-database/19.3/security/user-management.html#GUID-3336FBBB-C9C1-433C-9B32-9D02C8C739F7) for more details.
- Run tests with the command:
```bash
cd nosql-go-sdk

# Run all tests
make onprem-test

# Run all tests in the "TestDataOperations" test suite
make onprem-test testcases=TestDataOperations

# Run a specific test in the "TestDataOperations" test suite
make onprem-test testcases=TestDataOperations/TestPutGetDelete
```

## Run Examples

See the [Examples](https://github.com/oracle/nosql-go-sdk#examples) section.

## Pull Requests

Pull requests can be made under
[The Oracle Contributor Agreement](https://www.oracle.com/technetwork/community/oca-486395.html)
(OCA).

For pull requests to be accepted, the bottom of
your commit message must have the following line using your name and
e-mail address as it appears in the OCA Signatories list.

```
Signed-off-by: Your Name <you@example.org>
```

This can be automatically added to pull requests by committing with:

```
git commit --signoff
```

Only pull requests from committers that can be verified as having
signed the OCA can be accepted.
