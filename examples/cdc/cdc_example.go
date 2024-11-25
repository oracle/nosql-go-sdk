//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package main

import (
	"fmt"
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

	err = runCDCExample(client)
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
// was disabled elsewhere.

func runCDCExample(client *nosqldb.Client) error {

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

		// TODO: show details of message

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
