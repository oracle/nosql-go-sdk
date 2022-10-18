# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## Unreleased

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
