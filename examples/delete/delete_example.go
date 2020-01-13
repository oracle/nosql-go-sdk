//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

// The delete example shows how to delete a row or multiple rows satisfying certain criteria from a table.
// For example:
//
//   Create a table in Oracle NoSQL database
//   Put a row into the table
//   Delete a row from the table
//   Delete multiple rows from the table
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
	runDeleteExample(args)
}

func runDeleteExample(args *examples.Args) {

	authProvider, err := examples.CreateAuthorizationProvider(args)
	examples.ExitOnError(err)

	cfg := nosqldb.Config{
		Endpoint:              args.Endpoint,
		AuthorizationProvider: authProvider,
	}
	client, err := nosqldb.NewClient(cfg)
	examples.ExitOnError(err)

	// Creates a table
	tableName := "examplesAddress"
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s "+
		"(id INTEGER, address_line1 STRING, address_line2 STRING, "+
		"pin INTEGER, PRIMARY KEY(SHARD(pin), id))", tableName)
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

	// Put a row
	val := map[string]interface{}{
		"id":            1,
		"pin":           1234567,
		"address_line1": "10 Red Street",
		"address_line2": "Apt 3",
	}
	putReq := &nosqldb.PutRequest{
		TableName: tableName,
		Value:     types.NewMapValue(val),
	}
	putRes, err := client.Put(putReq)
	examples.ExitOnError(err)
	fmt.Printf("Put row: %v,\nresult: %v\n", jsonutil.AsPrettyJSON(putReq.Value.Map()),
		jsonutil.AsPrettyJSON(putRes))

	// Put with json
	jsonStrings := []string{
		`{"id": 2, "pin": 1234567, "address_line1": "2 Green Street", "address_line2": "Suite 9"}`,
		`{"id": 3, "pin": 1234567, "address_line1": "5 Blue Ave", "address_line2": "Floor 2"}`,
		`{"id": 4, "pin": 87654321, "address_line1": "9 Yellow Boulevard", "address_line2": "Apt 3"}`,
	}
	for _, s := range jsonStrings {
		v, err := types.NewMapValueFromJSON(s)
		examples.ExitOnError(err)

		putReq = &nosqldb.PutRequest{
			TableName: tableName,
			Value:     v,
		}
		putRes, err = client.Put(putReq)
		examples.ExitOnError(err)
		fmt.Printf("Put row:%v,\nresult: %v\n", jsonutil.AsPrettyJSON(putReq.Value.Map()),
			jsonutil.AsPrettyJSON(putRes))
	}

	// select all records
	query := "select * from " + tableName
	results, err := examples.RunQuery(client, query)
	examples.ExitOnError(err)
	fmt.Printf("Number of query results for %q: %d\n", query, len(results))
	for i, r := range results {
		fmt.Printf("\t%d: %s\n", i+1, jsonutil.AsJSON(r.Map()))
	}

	// Delete a single row - pass the entire key for the row to be deleted
	key := &types.MapValue{}
	key.Put("id", 2).Put("pin", 1234567)
	delReq := &nosqldb.DeleteRequest{
		TableName: tableName,
		Key:       key,
	}
	delRes, err := client.Delete(delReq)
	examples.ExitOnError(err)
	if delRes.Success {
		fmt.Println("Delete succeed")
	}

	// Delete multiple rows.
	// examplesAddress table's primary key is <pin, id> where the shard key is
	// the pin. To delete a range of id's that share the same shard key on can
	// pass the shard key to a MultiDeleteRequest
	shardKey := &types.MapValue{}
	shardKey.Put("pin", 1234567)
	multiDelReq := &nosqldb.MultiDeleteRequest{
		TableName: tableName,
		Key:       shardKey,
	}
	multiDelRes, err := client.MultiDelete(multiDelReq)
	examples.ExitOnError(err)
	fmt.Printf("MultiDelete result=%v\n", multiDelRes)

	// Query to verify that all related ids for a shard are deleted.
	results, err = examples.RunQuery(client, query)
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
