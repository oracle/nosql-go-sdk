//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
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

	client, err := examples.CreateClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	err = runExample(client)
	if err != nil {
		fmt.Println(err)
	}
}

func runExample(client *nosqldb.Client) error {

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
	if err != nil {
		return fmt.Errorf("failed to create table %s: %v", tableName, err)
	}
	fmt.Printf("Creating table %s ...\n", tableName)

	// The create table request is asynchronous, wait for table creation to complete.
	_, err = tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %v", tableName, err)
	}
	fmt.Println("Created table", tableName)

	// Create an index on the firstName field inside the userInfo JSON object.
	idxName := "idx1"
	stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s "+
		"(userInfo.firstName AS STRING)", idxName, tableName)
	idxReq := &nosqldb.TableRequest{
		Statement: stmt,
	}
	tableRes, err = client.DoTableRequest(idxReq)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %v", idxName, err)
	}
	fmt.Printf("Creating index %s ...\n", idxName)

	_, err = tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %v", idxName, err)
	}
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
		if err != nil {
			return fmt.Errorf("failed to create value from JSON: %v", err)
		}

		value := &types.MapValue{}
		value.Put("id", i+1).Put("userInfo", v)
		putReq := &nosqldb.PutRequest{
			TableName: tableName,
			Value:     value,
		}
		putRes, err := client.Put(putReq)
		if err != nil {
			return fmt.Errorf("failed to put a row: %v", err)
		}
		fmt.Printf("Put row: %v,\nresult: %s\n", jsonutil.AsPrettyJSON(putReq.Value.Map()),
			jsonutil.AsPrettyJSON(putRes))
	}

	//  Query from the indexed field
	query := "SELECT * FROM " + tableName + " p WHERE p.userInfo.firstName=\"Taylor\""
	results, err := examples.RunQuery(client, query)
	if err != nil {
		return fmt.Errorf("failed to execute statement %q: %v", query, err)
	}
	fmt.Printf("Number of query results for %q: %d\n", query, len(results))
	for i, r := range results {
		fmt.Printf("\t%d: %s\n", i+1, jsonutil.AsJSON(r.Map()))
	}

	// Drop the table
	dropReq := &nosqldb.TableRequest{
		Statement: "DROP TABLE IF EXISTS " + tableName,
	}
	tableRes, err = client.DoTableRequestAndWait(dropReq, 60*time.Second, time.Second)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %v", tableName, err)
	}
	fmt.Println("Dropped table", tableName)

	return nil
}
