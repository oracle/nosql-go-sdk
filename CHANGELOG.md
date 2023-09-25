# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## Unreleased

### Added

- Added capability to use Session Token based authentication:
     * NewSessionTokenSignatureProvider
     * NewSessionTokenSignatureProviderFromFile
- Cloud only: added support to read region from system environment variable
  OCI_REGION if using user principal or session token authentication
- Latest Oracle Cloud Infrastructure regions and region codes: OZZ, DRS
- Updated realm domain for OC22/NAP

## 1.4.1 - 2023-08-21

### Added
- Latest Oracle Cloud Infrastructure regions and region codes: MTY, STR, BEG, VLL, YUM,
  VAP, BOG, AGA, NAP, AVZ
- Added new signature provider methods to allow for delegation (OBO) tokens to be used
  with Instance Principals:
     * NewSignatureProviderWithInstancePrincipalDelegation
     * NewSignatureProviderWithInstancePrincipalDelegationFromFile
- On-premise only: added support for default namespace in config and
  namespace in requests.

### Changed
- Updated yaml.v2 to 2.4.0
- Updated copyrights to 2023
- Changed Version tests to be tolerant of serial versions

## 1.4.0 - 2022-12-15

### Added
Support for new, flexible wire protocol (V4):

The previous protocol is still supported for communication with servers that do not yet support V4. The
version negotation is internal and automatic; however, use of V4 features will fail
at runtime when attempted with an older server. Failure may be an empty or
undefined result or an exception if the request cannot be serviced at all. The following
new features or interfaces depend on the new protocol version:
 - added Durability to QueryRequest for queries that modify data
 - added pagination information to TableUsageResult and TableUsageRequest
 - added shard percent usage information to TableUsageResult
 - added IndexInfo.FieldTypes to return the type information on an index on a JSON field
 - added the ability to ask for and receive the schema of a query using
     * PrepareRequest.GetQuerySchema
     * PreparedStatement.GetQuerySchema
 - Cloud only: added use of ETags, DefinedTags and FreeFormTags in TableRequest and TableResult

- Latest Oracle Cloud Infrastructure regions and region codes: SGU, IFP, GCN

## 1.3.2 - 2022-10-18

### Added
- New GetBinary() convenience function
- Latest Oracle Cloud Infrastructure regions and region codes: TIW, ORD, BGY, MXP, DUS, DTM, ORK, SNN

### Fixed
- Fix for ARM64 compatibility issue #8

## 1.3.1 - 2022-06-13

### Added
- Support for session persistence. If a Set-Cookie HTTP header is present the SDK will now set a Cookie header using the requested session value.
- Support for PKCS8 format private keys.
- Added latest Oracle Cloud Infrastructure regions and region codes: CDG, MAD, QRO.

## 1.3.0 - 2022-02-24

### Added
- Added latest Oracle Cloud Infrastructure regions and region codes: LIN, MTZ, VCP, BRS, UKB, JNB, SIN, MRS, ARN, AUH, MCT, WGA.
- Cloud only: Added support for creating on-demand tables
- On-Prem only: Added support for setting Durability in put/delete operations
- Added support for returning row modification time in get operations

### Changed
- TableLimits now includes a CapacityMode field, to allow for specifying OnDemand. The default is Provisioned. This may affect existing code if the TableLimits struct was created without using named fields.
- Internal logic now detects differences in server protocol version, and decrements its internal serial version to match. This is to maintain compatibility when a new driver is used with an old server.

## 1.2.2 - 2021-06-08

### Added
- Added latest Oracle Cloud Infrastructure regions and region codes.
- Added support for ISO 8601 date strings with "Z" appended
- Added lint option to Makefile
- Rate Limiting (cloud only):
  - New boolean nosqldb.Config.RateLimitingEnabled can enable automatic internal rate limiting based on table read and write throughput limits.
  - If rate limiting is enabled:
    - nosqldb.Config.RateLimiterPercentage can control how much of a table's full limits this client handle can consume (default = 100%).
    - Result classes now have a GetRateLimitDelayed() method to return the amount of time an operation was delayed due to internal rate limiting.

### Changed
- Now uses SHA256 for OCI IAM instance principal fingerprint

## 1.2.1 - 2020-08-14

### Added
- Added generic GROUP BY and SELECT DISTINCT. These features will only work
with servers that also support generic GROUP BY.
- Added support for creating a signature provider with Instance Principal and
Resource Principal. These can be used to access NoSQL cloud service from within
Oracle Compute Instances or Oracle Functions. Cloud only.

### Changed
- Changed **nosqldb.Region** type to **common.Region**. Cloud applications need
to specify a value of `common.Region` type for the `Region` field of
`nosqldb.Config` when initialize client configurations. _**This is a breaking
change**_.

### Fixed
- Fixed a problem where the HTTP Host header was not being added in all request
cases. This prevented use of an intermediate proxy such as Nginx, which
validates headers. On-premise only.

## 1.2.0 - 2020-04-09
This is the initial release of the Go SDK for the Oracle NoSQL Database.
It supports both the on-premise product and the Oracle NoSQL
Database Cloud Service.
