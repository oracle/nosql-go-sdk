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
	"sync"
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

	err = runCDCMultiConsumer(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCAddTable(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCRestartable(client)
	if err != nil {
		fmt.Println(err)
	}
}

// Example function that runs consumer goroutines for all partitions of
// a change data capture stream for a single table. Each partition is
// handled by a consumer goroutine.
// The CDC streams are monitored for changes in partitioning, and new
// consumer goroutines are created for any new partitions.
// This returns only when all partitions for a table have been completely
// consumed - either because the table was dropped, or its CDC streaming
// was disabled elsewhere.
func runCDCMultiConsumer(client *nosqldb.Client) error {

	// Assume table "customer_data" exists already

	// Make an API call to enable CDC for this table. If it is already enabled,
	// no error is returned.
	// Note this is typically executed elsewhere, through a control plane mechanism
	// like terraform, the OCI control panel, or some other "coordinator" program.
	// In this example, we start with 4 partitions in the CDC stream for the table.
	tableName := "customer_data"
	tableReq := &nosqldb.TableRequest{
		TableName: tableName,
		CDCConfig: &nosqldb.CDCConfig{
			Enabled:       true,
			NumPartitions: 4,
		},
	}
	tableRes, err := client.DoTableRequest(tableReq)
	if err != nil {
		return fmt.Errorf("failed to enable CDC streaming on table %s: %v", tableName, err)
	}

	// The CDC enable table request is asynchronous. Wait for enablement to complete.
	completeRes, err := tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	if err != nil {
		return fmt.Errorf("failed to enable CDC streaming on table %s: %v", tableName, err)
	}

	// The list of partition IDs is contained in the returned table result
	if completeRes.CDCInfo == nil || len(completeRes.CDCInfo.Partitions) == 0 {
		return fmt.Errorf("no partitions returned from CDC enablement request for table %s", tableName)
	}

	// Keep a set of partitionIDs that need consumers running for them.
	pcMap := make(map[string]bool)

	// Set a flag for each partition that needs to have a consumer started.
	// Actual consumer creation is done in the loop below.
	for i := 0; i < len(completeRes.CDCInfo.Partitions); i++ {
		pID := completeRes.CDCInfo.Partitions[i].ID
		// false == need a consumer, but one isn't running yet
		pcMap[pID] = false
	}

	// keep a mutex lock for the map
	var pcMapLock sync.Mutex

	// Loop, periodically checking if there are any new partitions to start consumers for.
	// The partition map is updated by the consumers themselves, based on END messages they
	// may receive in their streams.
	// If a consumer needs to be started, it will exist in the map with a false value.
	// Note: this could also be managed by periodically polling the list of partitions for
	// a table and comparing that to the current map. But it has the caveat that new consumers
	// might be started for new partitions before their current parent partitions have
	// been completely consumed.
	for {
		pcMapLock.Lock()
		// walk map looking for 'false' values indicating a new consumer needs to be started
		for pID, value := range pcMap {
			if !value {
				pcMap[pID] = true
				// Start a goroutine to consume this partition
				go consumePartition(client, tableName, pID, pcMap, &pcMapLock)
			}
		}
		pcMapLock.Unlock()

		// If the map is totally empty, call NoSQL to see if there are any partitions
		// active for this table. If there are, add consumers for them. Note this should not
		// happen, since the consumers themselves will get notified when new partitions
		// are available.
		if len(pcMap) == 0 {
			tableReq := &nosqldb.GetTableRequest{
				TableName: tableName,
			}
			tableRes, err := client.GetTable(tableReq)
			if err != nil || tableRes.CDCInfo == nil || len(tableRes.CDCInfo.Partitions) == 0 {
				// If there are no partitions available (or the table has been dropped), we're done.
				break
			}
			for i := 0; i < len(tableRes.CDCInfo.Partitions); i++ {
				pID := tableRes.CDCInfo.Partitions[i].ID
				// Set a flag that a consumer needs to be started for this partition.
				// Actual consumer creation is done in the loop above.
				pcMap[pID] = false
			}
		}

		// Check for new partitions once per second
		time.Sleep(time.Duration(1) * time.Second)
	}

	return nil
}

// Consume message for a partition of a table. Update the given map if
// the stream indicates changes to the partitioning.
func consumePartition(client *nosqldb.Client,
	tableName string,
	pID string,
	pcMap map[string]bool,
	pcMapLock *sync.Mutex) error {

	// Start a CDC channel for the table and partition.
	// We can stop the CDC stream by closing the write channel.
	cursor := &nosqldb.ChangeCursorGroup{Cursors: []nosqldb.ChangeCursor{
		{TableName: tableName, PartitionID: pID, CursorType: nosqldb.TrimHorizon},
	}}

	// Loop reading messages from the CDC stream.
	for {
		// wait up to one second to read up to 10 events
		message, err := client.GetCDCMessages(cursor, 10, time.Duration(1*time.Second))
		if err != nil {
			// This would only happen if the table is dropped and all messages
			// have been consumed
			return fmt.Errorf("error getting CDC messages: %v", err)
		}
		fmt.Printf("Received message: %v\n", message)

		printCDCMessage(message)

		// The message will typically contain one or more events. But it may also
		// contain other metadata for the stream, such as start/end events and errors.
		if message.EndEvent != nil {
			// This partition is ended. It may have been replaced with new partitions.
			pcMapLock.Lock()
			for i := 0; i < len(message.EndEvent.ChildIDs); i++ {
				newPID := message.EndEvent.ChildIDs[i]
				// Set a flag that a consumer needs to be started for this partition.
				// Actual consumer execution is done in the parent loop.
				// Check to see that this partition is not already in the map before
				// blindly setting it to needing a new consumer
				if _, ok := pcMap[newPID]; !ok {
					pcMap[newPID] = false
				}
			}
			pcMapLock.Unlock()
			break
		}
	}

	// Remove this partition from the map of active consumers
	pcMapLock.Lock()
	delete(pcMap, pID)
	pcMapLock.Unlock()

	return nil
}

// Simple example to show how to add a table to an existing cursor group.
func runCDCAddTable(client *nosqldb.Client) error {

	// start with a cursor for one table
	cursors := &nosqldb.ChangeCursorGroup{Cursors: []nosqldb.ChangeCursor{
		{TableName: "table1", CursorType: nosqldb.TrimHorizon},
	}}

	// Read one message. This could be in a loop reading, etc...
	// wait up to one second to read up to 10 events
	message, err := client.GetCDCMessages(cursors, 10, time.Duration(1*time.Second))
	if err != nil {
		// This will happen if the table doesn't exist or isn't enabled for CDC
		return fmt.Errorf("error getting CDC messages: %v", err)
	}
	printCDCMessage(message)

	// The message contains a group cursor with the current values set (offset(s) are
	// set to the next event in the CDC stream)
	cursors = &message.CursorGroup

	// We can add a table to the cursor group by appending it to the Cursors array
	// The type can be any of the valid types (horizon, latest, date, etc)
	cursor := nosqldb.ChangeCursor{TableName: "table2", CursorType: nosqldb.TrimHorizon}
	cursors.Cursors = append(cursors.Cursors, cursor)

	// The next GetCDCMessages call may now contain events for both tables
	message, err = client.GetCDCMessages(cursors, 10, time.Duration(1*time.Second))
	if err != nil {
		// This will happen if the second table doesn't exist or isn't enabled for CDC
		return fmt.Errorf("error getting CDC messages: %v", err)
	}
	printCDCMessage(message)

	return nil
}


// Example function that runs consumer goroutines for all partitions of
// a change data capture stream for a single table. Each partition is
// handled by a consumer goroutine.
// The CDC streams are monitored for changes in partitioning, and new
// consumer goroutines are created for any new partitions.
// This returns only when all partitions for a table have been completely
// consumed - either because the table was dropped, or its CDC streaming
// was disabled elsewhere.
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

		printCDCMessage(message)

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

// Function to print all contents of a CDC stream message
func printCDCMessage(message *nosqldb.ChangeMessage) {
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
