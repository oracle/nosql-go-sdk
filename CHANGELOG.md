# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## [Unreleased]

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
