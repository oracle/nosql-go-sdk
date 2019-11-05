# Go SDK Examples for Oracle NoSQL Database

# Introduction

The `examples` folder contains examples for using the Oracle NoSQL Go SDK. There are three examples:

- The `basic` sub folder contains a sample program that demonstrates how to
perform basic operations on a table. The operations include but not limited to:
  - Create a table in Oracle NoSQL database
  - Put a row into the table
  - Get a row from the table
  - Use SQL query to retrieve rows from the table
  - Delete a row from the table
  - Drop the table

- The `delete` sub folder contains a sample program that demonstrates how to delete a row
or multiple rows satisfying certain criteria from a table. The operations include but not limited to:
  - Create a table in Oracle NoSQL database
  - Put a row into the table
  - Delete a row from the table
  - Delete multiple rows from the table
  - Drop the table

- The `index` sub folder contains a sample program that demonstrates how to create and
use the index on a table. The operations include but not limited to:
  - Create a table in Oracle NoSQL database
  - Create an index on the table
  - Put rows into the table
  - Get the rows using the index
  - Drop the table

## Build examples

- Build the examples using the command:
```bash
cd nosql-go-sdk
make build-examples
```
- Once build succeeds, the example binaries *basic*, *delete* and *index* will be
located in the *nosql-go-sdk/bin/examples* directory.

# Run examples
All examples can run with the Oracle NoSQL Cloud Service or the Oracle NoSQL Cloud Simulator.

### Run with the cloud service:
- Set up your ~/.andc/credentials file following the [instructions](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/required-credentials.html)
- Run the examples with the specified Oracle NoSQL cloud service endpoint and IDCS URL specific to your tenant as follows:
```bash
cd nosql-go-sdk/bin/examples

# Run the basic example
./basic ndcs.uscom-east-1.oraclecloud.com -idcsUrl https://idcs-xxxxxx.identity.oraclecloud.com

# Run the delete example
./delete ndcs.uscom-east-1.oraclecloud.com -idcsUrl https://idcs-xxxxxx.identity.oraclecloud.com

# Run the index example
./index ndcs.uscom-east-1.oraclecloud.com -idcsUrl https://idcs-xxxxxx.identity.oraclecloud.com
```

### Run with the cloud simulator:
- Start an instance of cloud simulator
- Run the examples with the specified endpoint for cloud simulator as follows:
```bash
cd nosql-go-sdk/bin/examples

# Run the basic example
./basic localhost:8080

# Run the delete example
./delete localhost:8080

# Run the index example
./index localhost:8080
```
