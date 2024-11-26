//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/oracle/nosql-go-sdk/examples"
	"github.com/oracle/nosql-go-sdk/nosqldb"
)

func main() {

	client, err := examples.CreateClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	err = runCDCRestartable(client)
	if err != nil {
		fmt.Println(err)
	}
}

// Example program that runs consumer goroutines for all partitions of
// a change data capture stream for a single table. Each partition is
// handled by a consumer goroutine.
// The CDC streams are monitored for changes in partitioning, and new
// consumer goroutines are created for any new partitions.
// This returns only when all partitions for a table have been completely
// consumed - either because the table was dropped, or its CDC streaming
// was disabled elsewahere.

func runCDCRestartable(client *nosqldb.Client) error {

	// Assume table "customer_data" exists already and is CDC enabled
	// Assume table "insight_data" exists already and is CDC enabled
	var cursor *nosqldb.ChangeCursorGroup
	// if a previous run of this program wrote a checkpoint file,
	// read that and use its data as a group cursor for our CDC consumer.
	fileName := "/tmp/cdc_cursor.json"
	infile, err := os.Open(fileName)
	if err == nil {
		var cg nosqldb.ChangeCursorGroup
		decoder := json.NewDecoder(infile)
		err = decoder.Decode(&cg)
		infile.Close()
		if err != nil {
			return fmt.Errorf("error decoding cursor from %s: %v", fileName, err)
		}
		cursor = &cg
	} else {
		// Create a new cursor group for two tables, starting with the most current entry in
		// each partition of each table.
		cursor = &nosqldb.ChangeCursorGroup{
			Cursors: []nosqldb.ChangeCursor{
				{TableName: "client_info", CursorType: nosqldb.Latest},
				{TableName: "location_data", CursorType: nosqldb.Latest},
			},
		}
	}

	// Read 10 messages from the CDC stream. Write a checkpoint after every message received.
	for i := 0; i < 10; i++ {
		// wait up to one second to read up to 10 events
		message, err := client.GetCDCMessages(cursor, 10, time.Duration(1*time.Second))
		if err != nil {
			return fmt.Errorf("error getting CDC messages: %v", err)
		}
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.

		fmt.Printf("Received message: %v", message)

		processCDCMessage(message)

		// The message contains a cursor pointing to the next messages in the CDC stream
		cursor = &message.CursorGroup

		// write a file to enable starting this program again from a checkpoint
		jsonData, err := json.Marshal(*cursor)
		if err != nil {
			return fmt.Errorf("can't marshal cursor to JSON: %v", err)
		}
		outfile, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("can't open checkpoint file %s: %v", fileName, err)
		}
		_, err = outfile.Write(jsonData)
		outfile.Close()
		if err != nil {
			return fmt.Errorf("can't write checkpoint data to file %s: %v", fileName, err)
		}
	}

	// The JSON representation of the cursor group may now be something like the
	// following (assuming in this case the two tables have 2 partititons each):
	// {
	//   "cursors": [
	//     {
	//       "tableName": "client_info",
	//       "tableOCID": "aaagfjdfjddfk758485",
	//       "partitionID": "2443hxdF",
	//       "offset": 128548,
	//       "cursorType": "AtOffset"
	//     },
	//     {
	//       "tableName": "client_info",
	//       "tableOCID": "aaagfjdfjddfk758485",
	//       "partitionID": "54664hdF",
	//       "offset": 129024,
	//       "cursorType": "AtOffset"
	//     },
	//     {
	//       "tableName": "location_data",
	//       "tableOCID": "aaagfjgdsgds943fkjs",
	//       "partitionID": "hf73kjdsiX",
	//       "offset": 3404,
	//       "cursorType": "AtOffset"
	//     },
	//     {
	//       "tableName": "location_data",
	//       "tableOCID": "aaagfjgdsgds943fkjs",
	//       "partitionID": "84537hf",
	//       "offset": 17808,
	//       "cursorType": "AtOffset"
	//     }
	//   ]
	// }

	// This function can now be run again, picking up where it left off from the
	// latest checkpoint file.

	return nil

}

func processCDCMessage(message *nosqldb.ChangeMessage) {
	fmt.Printf("Message: events remaining = %v", message.EventsRemaining)

	// A StartEvent will only appear once in teh stream, at the very start.
	// If this stream was started in the middle, this event will not appear.
	if message.StartEvent != nil {
		se := *message.StartEvent
		fmt.Printf("Start event: \n")
		fmt.Printf(" parent partititons: %v", se.ParentIDs)
		fmt.Printf(" table name: %s", se.Cursor.TableName)
		fmt.Printf(" table compartment: %s", se.Cursor.CompartmentOCID)
		// Note: partition ID will be blank if the table is not partitioned
		fmt.Printf(" partition ID: %s", se.Cursor.PartitionID)
		fmt.Printf(" stream start time: %v", se.Cursor.StartTime)
	}

	// An EndEvent will appear when the CDC stream has ended, either because the
	// table has been dropped, CDC streaming for the table has been disabled,
	// or the table is partitioned and this partition has no more data.
	if message.EndEvent != nil {
		ee := *message.EndEvent
		fmt.Printf("End event: \n")
		// If a partition has ended, it may have been split into new partitions
		fmt.Printf(" child partitions: %v", ee.ChildIDs)
		fmt.Printf(" table name: %s", ee.Cursor.TableName)
		fmt.Printf(" table compartment: %s", ee.Cursor.CompartmentOCID)
		// Note: partition ID will be blank if the table is not partitioned
		fmt.Printf(" partition ID: %s", ee.Cursor.PartitionID)
	}

	// ChangeEvents is an array of changes for the message
	for i := 0; i < len(message.ChangeEvents); i++ {
		event := message.ChangeEvents[i]
		fmt.Printf("Change event: \n")
		fmt.Printf(" table name: %s", event.Cursor.TableName)
		fmt.Printf(" table compartment: %s", event.Cursor.CompartmentOCID)
		// Note: partition ID will be blank if the table is not partitioned
		fmt.Printf(" partition ID: %s", event.Cursor.PartitionID)
		// type will be Put or Delete
		fmt.Printf(" change type: %v", event.ChangeType)
		fmt.Printf(" modification time: %v", event.ModificationTime)
		fmt.Printf(" expiration time: %v", event.ExpirationTime)
		fmt.Printf(" current image:\n")
		fmt.Printf("   key: %v", event.CurrentImage.RecordKey)
		fmt.Printf("   value: %v", event.CurrentImage.RecordValue)
		fmt.Printf("   metadata: %v", event.CurrentImage.RecordMetadata)
		if event.BeforeImage != nil {
			fmt.Printf(" before image:\n")
			fmt.Printf("   key: %v", event.BeforeImage.RecordKey)
			fmt.Printf("   value: %v", event.BeforeImage.RecordValue)
			fmt.Printf("   metadata: %v", event.BeforeImage.RecordMetadata)
		}
	}
}
