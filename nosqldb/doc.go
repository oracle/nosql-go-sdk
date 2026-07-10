//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

/*
Package nosqldb provides the public APIs for Go applications to use the Oracle NoSQL Database.

This package also provides configuration and common operational structs and interfaces,
such as request and result types used for NoSQL database operations.

Client-side statistics can be enabled with Config.StatsProfile. The profile
names and emitted JSON follow the Oracle NoSQL Java SDK contract: NONE disables
statistics, REGULAR emits aggregate request statistics, MORE adds latency
percentiles, and ALL adds query-level statistics. Use Client.GetStatsControl to
inspect or update runtime statistics settings and Config.StatsHandler to receive
emitted snapshots directly.

More detailed information can be viewed at: https://github.com/oracle/nosql-go-sdk/blob/master/README.md
*/
package nosqldb
