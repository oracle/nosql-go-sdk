//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// The index example shows how to create and use the index on a table.
// For example:
//
//   Create a table in Oracle NoSQL database
//   Create an index on the table
//   Put rows into the table
//   Get the rows using the index
//   Drop the table
//
// See https://godoc.org/github.com/oracle/nosql-go-sdk/examples for more details
// on how to build and run the examples.
//
package main

import (
	"fmt"
	"time"

	"github.com/oracle/nosql-go-sdk/examples"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func main() {
	args := examples.ParseArgs()
	runIndexExample(args)
}

func runIndexExample(args *examples.Args) {

	authProvider, err := examples.CreateAuthorizationProvider(args)
	examples.ExitOnError(err)

	cfg := nosqldb.Config{
		Endpoint:              args.Endpoint,
		AuthorizationProvider: authProvider,
	}
	client, err := nosqldb.NewClient(cfg)
	examples.ExitOnError(err)

	// Creates a table.
	tableName := "exampleProfiles"
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s "+
		"(id INTEGER, userInfo JSON, primary key(id))", tableName)
	tableReq := &nosqldb.TableRequest{
		Statement: stmt,
		TableLimits: &nosqldb.TableLimits{
			ReadUnits:  50,
			WriteUnits: 50,
			StorageGB:  5,
		},
	}
	tableRes, err := client.DoTableRequest(tableReq)
	fmt.Println("Creating table", tableName)
	examples.ExitOnError(err)

	// The create table request is asynchronous, wait for table creation to complete.
	_, err = tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	examples.ExitOnError(err)
	fmt.Println("Created table", tableName)

	// Create an index on the firstName field inside the userInfo JSON object.
	idxName := "idx1"
	stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s "+
		"(userInfo.firstName AS STRING)", idxName, tableName)
	idxReq := &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err = client.DoTableRequest(idxReq)
	fmt.Println("Creating index", idxName)
	examples.ExitOnError(err)

	_, err = tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	examples.ExitOnError(err)
	fmt.Println("Created index", idxName)

	// Put with json
	jsonStrings := []string{
		`{"firstName": "Taylor", "lastName": "Smith", "age": 33}`,
		`{"firstName": "Xiao", "lastName": "Zhu", "age": 17}`,
		`{"firstName": "Supriya", "lastName": "Jain", "age": 65}`,
		`{"firstName": "Taylor", "lastName": "Rodriguez", "age": 43}`,
	}
	for i, s := range jsonStrings {
		v, err := types.NewMapValueFromJSON(s)
		examples.ExitOnError(err)

		value := &types.MapValue{}
		value.Put("id", i+1).Put("userInfo", v)
		putReq := &nosqldb.PutRequest{
			TableName: tableName,
			Value:     value,
		}
		putRes, err := client.Put(putReq)
		examples.ExitOnError(err)
		fmt.Printf("Put row:%v,\nresult: %s\n", jsonutil.AsPrettyJSON(putReq.Value.Map()),
			jsonutil.AsPrettyJSON(putRes))
	}

	//  Query from the indexed field
	query := "SELECT * FROM " + tableName + " p WHERE p.userInfo.firstName=\"Taylor\""
	results, err := examples.RunQuery(client, query)
	examples.ExitOnError(err)
	fmt.Printf("Number of query results for %q: %d\n", query, len(results))
	for i, r := range results {
		fmt.Printf("\t%d: %s\n", i+1, jsonutil.AsJSON(r.Map()))
	}

	// Drop the table
	dropReq := &nosqldb.TableRequest{
		Statement: "DROP TABLE IF EXISTS " + tableName,
	}
	tableRes, err = client.DoTableRequestAndWait(dropReq, 60*time.Second, time.Second)
	examples.ExitOnError(err)
	fmt.Println("Dropped table", tableName)
}
