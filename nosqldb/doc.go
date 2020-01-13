//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

/*
Package nosqldb provides the public APIs for Go applications to use the Oracle NoSQL Database.

This package also provides configuration and common operational structs and interfaces,
such as request and result types used for NoSQL database operations.
Errors that may return by the NoSQL client are in the "nosqlerr" package.
Values that used for data are in the "types" package.

This and other packages in the system support both users of the Oracle NoSQL
Database Cloud Service and the on-premise Oracle NoSQL Database. Some structs,
methods, and parameters are specific to each environment. The documentation for
affected structs and methods notes whether there are environment-specific
considerations. Unless otherwise noted they are applicable to both environments.
The differences mostly related to authentication models, encapsulated in
AuthorizationProvider and resource constraints and limits in the Cloud Service
that are not present on-premise.

The overall flow of a driver application is:

1. Create a configuration used for NoSQL client, including the endpoint for the
server to use. The Config struct defined in this package provides various configuration
options for applications.

2. Use the configuration object to create a Client instance.

3. All data operations are methods on the Client struct. They all have the same
pattern of:

   Create and configure Request instance for the operations
   Call the appropriate method on Client for the request
   Process results in the Result object

If there are any errors during the operation, they are returned to applications,
usually as a nosqlerr.Error value.

Instances of the Client are safe for concurrent use by multiple goroutines and
intended to be shared in a multi-goroutines application.
They are associated 1:1 with an identity so they cannot be shared by different
users.
*/
package nosqldb
