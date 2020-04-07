# Working With Tables

Oracle NoSQL Database applications use tables to manipulate data. The Go SDK
supports creation, modification and removal of tables and indexes, inserting, 
reading, updating and deleting of data on tables, as well as quering data from 
tables. This guide provides an overview of these capabilities. For complete 
description of the APIs and available options, see the Go SDK
[documentation](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb).

## Overview

The Go SDK provides several packages such as [nosqldb](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb),
[types](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/types),
[nosqlerr](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr),
etc.

The **nosqldb** package provides APIs for applications to send operation
requests and receive results from the Oracle NoSQL Database. It also provides
configuration and common operational structs and interfaces, such as request and
result types used for NoSQL database operations.

The **nosqlerr** package defines errors that may be reported for NoSQL client
configurations and database operations.

The **types** package provides types and values used to manipulate data on the
Oracle NoSQL Database.

>***Note***: These and other packages provided by the Go SDK support both users
of the Oracle NoSQL Database Cloud Service and on-premise Oracle NoSQL Database.
Some structs, methods, and parameters are specific to each environment. The 
documentation for affected structs and methods notes whether there are
environment-specific considerations. Unless otherwise noted they are applicable
to both environments.

The overall flow of an application is:

1. Create a configuration used for NoSQL client, including the service region or
service endpoint to use. The [nosqldb.Config](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Config) 
struct provides various configuration options for applications.
2. Use the configuration object to create a [nosqldb.Client](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client) instance.
3. Use one or more methods on the *Client* instance to execute desired 
operations on the Oracle NoSQL Database. All data operations are methods on the 
*nosqldb.Client* type. They all have the same pattern of:

   - Create and configure *Request* instance for the operations
   - Call the appropriate method on *Client* for the request
   - Process results in the *Result* object

4. Use [Client.Close](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.Close) 
to close the connection and clean up resources.

If there are any errors during the operation, they are reported to applications,
usually as a *nosqlerr.Error* value.

## Create a Client

The first step when you write an application is create a NoSQL client using
appropriate configurations. A [nosqldb.Client](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client)
represents a connection handle to the Oracle NoSQL Database server. Once created
it must be closed using the [Client.Close()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.Close) 
method in order to clean up resources. A client is safe for use by multiple
goroutines and intended to be shared. When create a client, a
[nosqldb.Config](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Config) 
instance must be provided that specifies the communication endpoint,
authorization information, and any other configurations required.

Client configuration requires a [nosqldb.AuthorizationProvider](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#AuthorizationProvider) 
to provide identity and authorization information to the client. There are
different implementations of this interface for the different environments:

1. [iam.SignatureProvider](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/auth/iam#SignatureProvider)
for Oracle NoSQL Cloud Service
2. [cloudsim.AccessTokenProvider](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/auth/cloudsim#AccessTokenProvider)
for Oracle NoSQL Cloud Simulator
3. [kvstore.AccessTokenProvider](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/auth/kvstore#AccessTokenProvider)
for Oracle NoSQL Database on-premise

See the [Connect Tutorial](https://github.com/oracle/nosql-go-sdk/nosqldb/doc/connect.md)
for details on how to provide client configurations and authorization
information for different environments.

## Create Tables and Indexes

Learn how to create tables and indexes in Oracle NoSQL Database.

You must create a table before you can manipulate data on the table. Before 
creating a table, learn about:

* Design considerations for tables. See [Table Design](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/table-design.html#GUID-7A409201-F240-4DE5-A0C1-545ADFCBFB77).
* Supported data types for the Oracle NoSQL Database. See
[Supported Data Types](https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-833B2B2A-1A32-48AB-A19E-413EAFB964B8).
* For the Oracle NoSQL Database Cloud Service limits. See [Oracle NoSQL Database Cloud Limits](https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-30129AB3-906B-4E71-8EFB-8E0BBCD67144).
These limits are not relevant on-premise.

Use the [nosqldb.TableRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#TableRequest) 
to specify information for creating tables or indexes:

* A Data Definition Language (DDL) statement that defines the table or index.
* Table limits that specify read/write throughput and storage size for the table.
  This is required when create a table on the Oracle NoSQL Cloud Service or 
  Cloud Simulator. Table limits are ignored when work with the on-premise Oracle
  NoSQL database, if provided.
* An optional timeout value for the table request.

Examples of DDL statements that create tables or indexes are:

```sql
/* Create a new table called users */
CREATE IF NOT EXISTS users (id INTEGER, name STRING, PRIMARY KEY (id));

/* Create a new table called users and set the TTl value to 4 days */
CREATE IF NOT EXISTS users (id INTEGER, name STRING, PRIMARY KEY (id)) USING TTL 4 days;

/* Create a new index called nameIdx on the name field in the users table */
CREATE INDEX IF NOT EXISTS nameIdx ON users(name);
```

Encapsulate the DDL statement and other information as a *TableRequest*, then
execute the request using [nosqldb.DoTableRequest()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.DoTableRequest) 
function. All calls to the *DoTableRequest()* are asynchronous so it is 
necessary to check the returned [nosqldb.TableResult](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#TableResult)  and call the 
[TableResult.WaitForCompletion()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#TableResult.WaitForCompletion)
to wait for operation to complete. The [nosqldb.DoTableRequestAndWait()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.DoTableRequestAndWait) method provides a convenience way to 
execute the operation and wait until the operation completes.

```go
req := &nosqldb.TableRequest{
    Statement: "CREATE IF NOT EXISTS users (id INTEGER, name STRING, PRIMARY KEY (id))",
    TableLimits: &nosqldb.TableLimits{
        ReadUnits:  200,
        WriteUnits: 200,
        StorageGB:  5,
    },
}
res, err := client.DoTableRequestAndWait(req, 5*time.Second, time.Second)
...

idxReq := &nosqldb.TableRequest{
    Statement: "CREATE INDEX IF NOT EXISTS nameIdx ON users(name)",
}
idxRes, err := client.DoTableRequestAndWait(idxReq, 5*time.Second, time.Second)
```

See examples of creating a table using [DoTableRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-DoTableRequest) and [DoTableRequestAndWait](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-DoTableRequestAndWait-CreateTable) for more details.

## Add Data

Learn how to add rows to your table.

When you store data in table rows, your application can easily retrieve, add to,
or delete information from the table.

The [nosqldb.PutRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#PutRequest) 
represents an input to the [nosqldb.Put()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Put)
function used to insert single rows. This function can be used for unconditional
and conditional puts to:

* Overwrite any existing row. This is the default. See an example of [unconditional put](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-Put).
* Succeed only if the row does not exist. Specify *types.PutIfAbsent* for the
*PutRequest.PutOption* field for this case. See an example of [PutIfAbsent](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-Put-PutIfAbsent).
* Succeed only if the row exists. Specify *types.PutIfPresent* for the
*PutRequest.PutOption* field for this case. See an example of [PutIfPresent](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-Put-PutIfPresent).
* Succeed only if the row exists and its version matches a specific version.
Specify *types.PutIfVersion* for the *PutRequest.PutOption* field and a desired
version for the *PutRequest.MatchVersion* field for this case.
See an example of [PutIfVersion](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-Put-PutIfVersion).

When adding data the values supplied must accurately correspond to the schema
for the table. If they do not, *nosqlerr.IllegalArgumentError* is reported.
Columns with default or nullable values can be left out without error, but it is
recommended that values be provided for all columns to avoid unexpected defaults.
By default, unexpected columns are ignored silently, and the value is put using
the expected columns. If the *PutRequest.ExactMatch* field is set to true, any
unexepcted columns would result in *nosqlerr.IllegalArgumentError*.

The data value provided for a row (in *PutRequest*) or key (in *GetRequest* and
*DeleteRequest* described in the following sections) is a [*types.MapValue](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/types#MapValue).
The *key* portion of each entry in the *MapValue* must match the column name of 
target table, and the *value* portion must be a valid value for the column.
There are several ways to create a *MapValue* that supplied for the row to put
into a table:

1. Create an empty *MapValue* and put values for each column

```go
value := &types.MapValue{}
value.Put("id", 1).Put("name", "Jack")
req := &nosqldb.PutRequest{
    TableName: "users",
    Value: value,
}
res, err := client.Put(req)
...
```

2. Create a *MapValue* from a *map[string]interface{}*

```go
m := map[string]interface{}{
    "id": 1,
    "name": "Jack",
}
value := types.NewMapValue(m)
req := &nosqldb.PutRequest{
    TableName: "users",
    Value: value,
}
res, err := client.Put(req)
...
```

3. Create a *MapValue* from JSON. This is convenient for setting values for
a row in the case of a fixed-schema table where the JSON is converted to the 
target schema. For example:

```go
value, err := types.NewMapValueFromJSON(`{"id": 1, "name": "Jack"}`)
if err != nil {
    return
}
req := &nosqldb.PutRequest{
    TableName: "users",
    Value: value,
}
res, err := client.Put(req)
...
```

JSON data can also be directly inserted into a column of type *JSON*. The use 
of the JSON data type allows you to create table data without a fixed schema, 
allowing more flexible use of the data.

If you have multiple rows that share the same shard key they can be put in a
single request using [nosqldb.WriteMultipleRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#WriteMultipleRequest) which can be created using a number of 
*PutRequest* or *DeleteRequest* objects.

## Read Data

Learn how to read data from your table.

You can read single rows using the [Client.Get](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.Get) 
function. This function allows you to retrieve a record based on its primary key
value. In order to read multiple rows in a single request see *Use Queries* below.

The [nosqldb.GetRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#GetRequest) 
is used for simple get operations. It contains the primary key value for the 
target row and returns an instance of [nosqldb.GetResult](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#GetResult). If the get operation succeeds, a non-nil *GetResult.Version*
is returned.

```go
key := &types.MapValue{}
key.Put("id", 1)
req := &nosqldb.GetRequest{
    TableName: "users",
    Key: key,
}
res, err := client.Get(req)
...
```

By default all read operations are eventually consistent, using 
*types.Eventual*. This type of read is less costly than those using absolute 
consistency, *types.Absolute*. This default can be changed in 
[nosqldb.RequestConfig](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#RequestConfig) 
using *RequestConfig.Consistency* before creating the client. It
can be changed for a single request using *GetRequest.Consistency* field.

1. Change default consistency for all read operations.

```go
cfg := nosqldb.Config{
    ...
    RequestConfig: nosqldb.RequestConfig{
        Consistency: types.Absolute,
        ...
    },
    ...
}
client, err := nosqldb.NewClient(cfg)
...
```

2. Change consistency for a single read operation.

```go
req := &nosqldb.GetRequest{
    TableName: "users",
    Key: key,
    Consistency: types.Absolute,
}
```

## Use Queries

Learn about using queries in your application.

Oracle NoSQL Database provides a rich query language to read and update data.
See the [SQL For NoSQL Specification](http://www.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=sql_nosql) 
for a full description of the query language.

To execute a query use the [Client.Query](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.Query) 
function. For example, to execute a *SELECT* query to read data from your table:

```go
prepReq := &nosqldb.PrepareRequest{
    Statement: "select * from users",
}

prepRes, err := client.Prepare(prepReq)
if err != nil {
    fmt.Printf("Prepare failed: %v\n", err)
    return
}

queryReq := &nosqldb.QueryRequest{
    PreparedStatement: &prepRes.PreparedStatement,
}
var results []*types.MapValue
for {
    queryRes, err := client.Query(queryReq)
    if err != nil {
        fmt.Printf("Query failed: %v\n", err)
        return
    }

    res, err := queryRes.GetResults()
    if err != nil {
        fmt.Printf("GetResults() failed: %v\n", err)
        return
    }

    results = append(results, res...)

    if queryReq.IsDone() {
        break
    }
}
```

Queries should generally be run in a loop and check the *QueryRequest.IsDone()*
to determine if the query completes. It is possible for single request to return
no results but still have *QueryRequest.IsDone()* evaluated to false, indicating
that the query loop should continue, as show in the example above.

When using queries it is important to be aware of the following considerations:

* Oracle NoSQL Database provides the ability to prepare queries for execution
and reuse. It is recommended that you use prepared queries when you run the
same query for multiple times. When you use prepared queries, the execution
is much more efficient than starting with a query string every time. The query
language and API support query variables to assist with query reuse.
See [Cilent.Prepare()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.Prepare) 
and [nosqldb.PrepareRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#PrepareRequest) 
for more information.
* The *nosqldb.QueryRequest* allows you to set the read consistency for a query
(via the *QueryRequest.Consistency* field), as well as modifying the maximum 
amount of resource (read and write, via the *QueryRequest.MaxReadKB* and
*QueryRequest.MaxWriteKB* fields) to be used by a single request. This can be 
important to prevent a query from getting throttled because it uses too much 
resource too quickly.

## Delete Data

Learn how to delete rows from your table.

Single rows are deleted using [nosqldb.DeleteRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#DeleteRequest) 
using a primary key value:

```go
key := &types.MapValue{}
key.Put("id", 1)
req := &nosqldb.DeleteRequest{
    TableName: "users",
    Key: key,
}
res, err := client.Delete(req)
...
```

Delete operations can be conditional based on a *types.Version* returned
from a get operation.  See an example of [DeleteIfVersion](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-Delete-DeleteIfVersion).

You can perform multiple deletes in a single operation using a value range using
[nosqldb.MultiDeleteRequest](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#MultipleDeleteRequest) 
and [Client.MultiDelete()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#MultiDelete) function.

## Modify Tables

Learn how to modify tables. You modify a table to:

* Add or remove fields to an existing table
* Change the default TimeToLive (TTL) value for the table
* Modify table limits

Examples of DDL statements to modify a table are:

```sql
/* Add a new field to the table */
ALTER TABLE users (ADD age INTEGER)

/* Drop an existing field from the table */
ALTER TABLE users (DROP age)

/* Modify the default TTl value */
ALTER TABLE users USING TTL 4 days
```

Specify the DDL statement and other information in a *TableRequest*, and execute
the request using the [nosqldb.DoTableRequest()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.DoTableRequest) 
or [nosqldb.DoTableRequestAndWait()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#Client.DoTableRequestAndWait)  function. 

```go
req := &nosqldb.TableRequest{
    Statement: "ALTER TABLE users (ADD age INTEGER)",
}
res, err := client.DoTableRequestAndWait(req, 5*time.Second, time.Second)
...
```

If using the Oracle NoSQL Database Cloud Service table limits can be modified
using *TableRequest.TableLimits*, for example:

```go
req := &nosqldb.TableRequest{
    TableName: "users",
    TableLimits: &nosqldb.TableLimits{
        ReadUnits: 100,
        WriteUnits: 100,
        StorageGB: 5,
    },
}
res, err := client.DoTableRequestAndWait(req, 5*time.Second, time.Second)
...
```

See an example of modifying table limits using [DoTableRequestAndWait](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-DoTableRequestAndWait-ChangeTableLimits) for more details.

## Drop Tables and Indexes

Learn how to drop a table or index.

To drop a table or index, use the *drop table* or *drop index* DDL statement,
```sql
/* drop the table named users (implicitly drops any indexes on that table) */
DROP TABLE users

/* drop the index called nameIndex on the table users. Don't fail if the index doesn't exist */
DROP INDEX IF EXISTS nameIndex ON users
```

```go
req := &nosqldb.TableRequest{
    Statement: "drop table users",
}
res, err := client.DoTableRequestAndWait(req, 5*time.Second, time.Second)
...
```

See an example of dropping tables using [DoTableRequestAndWait](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#example-Client-DoTableRequestAndWait-DropTable) for more details.

## Configure Logging

By default the Go SDK logs *WARNING* and *ERROR* messages for client
operations to standard error output. You can configure logging as needed by
creating a logger and provide to the *LoggingConfig.Logger*.

```go
myLogger := New(os.Stdout, logger.Info, true)
cfg := nosqldb.Config{
    ...
    LoggingConfig: nosqldb.LoggingConfig{
        Logger: myLogger,
    },
    ...
}
client, err := nosqldb.NewClient(cfg)
...
```

## Handle Errors

Go SDK errors are reported as *nosqlerr.Error* values defined as part of the API.

Errors are split into 2 broad categories:

* Errors that may be retried with the expectation that they may succeed on
  retry. These are retryable errors on which the [Error.Retryable()](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr#Error.Retryable) 
  method call returns *true*. Examples of these include *nosqlerr.OperationLimitExceeded*,
  *nosqlerr.ReadLimitExceeded*, *nosqlerr.WriteLimitExceeded*, which are
  raised when resource consumption limits are exceeded.
* Errors that should not be retried, as they will fail again. Examples of these 
  include *nosqlerr.IllegalArgumentError*, *nosqlerr.TableNotFoundError*, etc.

Throttling errors such as *nosqlerr.ReadLimitExceeded*,
*nosqlerr.WriteLimitExceeded* will never be thrown in an on-premise 
configuration as there are no relevant limits.

## Handle Resource Limits

This section is relevant only to the Cloud Service and Cloud Simulator.

Programming in a resource-limited environment can be unfamiliar and can lead to
unexpected errors. Tables have user-specified throughput limits and if an
application exceeds those limits it may be throttled, which means requests will
raise throttling errors such as *nosqlerr.ReadLimitExceeded*, 
*nosqlerr.WriteLimitExceeded*.

There is some support for built-in retries and users can create their own
*retry handler* by implementing the [nosqldb.RetryHandler](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#RetryHandler) 
interface and supplying to the client using *Config.RetryHandler*, this allows 
more direct control over retries as well as tracing of throttling events.
An application should not rely on retries to handle throttling errors 
as that will result in poor performance and an inability to use all of the 
throughput available for the table. This happens because the default retry 
handler will do exponential backoff, starting with a one-second delay.

While handling throttling error is necessary it is best to avoid
throttling entirely by rate-limiting your application. In this context
*rate-limiting* means keeping request rates under the limits for the table. This
is most common using queries, which can read a lot of data, using up capacity
very quickly. It can also happen for get and put operations that run in a tight
loop. Some tools to control your request rate include:

* use the methods available in all *Result* objects that indicate how much read
and write throughput was used by that request. For example, see
[GetResult.ReadKB](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#GetResult) 
or [PutResult.WriteKB](https://godoc.org/github.com/oracle/nosql-go-sdk/nosqldb#PutResult).
* reduce the default amount of data read for a single query request by using
*QueryRequest.MaxReadKB*. Remember to perform query operations in a loop, 
checking *QueryRequest.IsDone()* in each loop. Be aware that a single query 
request can return no results but still have *QueryRequest.IsDone()* returning 
*false* that means you need to keep looping.
* add rate-limiting code in your request loop. This may be as simple as a delay
between requests or intelligent code that considers how much data has been
read (see *QueryResult.ReadKB*) as well as the capacity of the table to either 
delay a request or reduce the amount of data to be read.
