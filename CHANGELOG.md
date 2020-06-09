# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## 1.2.1 - 2020-??-??

### Added
- Added generic GROUP BY and SELECT DISTINCT. These features will only work
  with servers that also support generic GROUP BY.

### Fixed
- Fixed a problem where the HTTP Host header was not being added in all request
cases. This prevented use of an intermediate proxy such as Nginx, which
validates headers. On-premise only.

## 1.2.0 - 2020-04-09
This is the initial release of the Go SDK for the Oracle NoSQL Database.
It supports both the on-premise product and the Oracle NoSQL
Database Cloud Service.
