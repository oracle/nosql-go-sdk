//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package main

import (
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

	err = runCDCSimple(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCGroupConsumer(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCAddTable(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCCheckpoint(client)
	if err != nil {
		fmt.Println(err)
	}
}

// Simple example showing how to create a single consumer for
// a single table.
// In this example, it is assumed that the table already exists and
// has already been enabled for CDC streaming.
func runCDCSimple(client *nosqldb.Client) error {

	// Assume table "customer_data" exists already and is CDC enabled

	// Create a consumer config that will be used to create the consumer.
	// Start with the earliest message available in the change stream.
	// Automatically commit polled messages on 10 second intervals.
	config := client.CreateChangeConsumerConfig().
		TableName("customer_data").
		// Compartment(string)
		// AddTable(tablename, compartment, startat, startdata)
		Earliest().
		// Latest()
		// FirstUncommitted()
		// AtTime(time.Time)
		CommitInterval(time.Duration(10 * time.Second))
		// CommitOnPoll()
		// CommitManual()
		// GroupID()

	// Create the consumer. This will make a server side call to validate
	// and establish the consumer information.
	consumer, err := client.CreateChangeConsumer(config)
	if err != nil {
		return fmt.Errorf("error creating group consumer: %v", err)
	}

	// Loop reading messages from the consumer
	for {
		// wait up to one second to read up to 10 events
		message, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			// This would only happen if the table is dropped and all messages
			// have been consumed
			return fmt.Errorf("error polling for CDC messages: %v", err)
		}
		// Do something with message
		printCDCMessage(message)

		//TODO: example of manual commit
	}
}

// Example function that runs many consumers in goroutines for
// a change data capture stream for a single table.
// This returns only when all data for a table has been completely
// consumed - either because the table was dropped, or its CDC streaming
// was disabled elsewhere.
func runCDCGroupConsumer(client *nosqldb.Client) error {

	// Assume table "customer_data" exists already

	// Make an API call to enable CDC for this table. If it is already enabled,
	// no error is returned.
	// Note this is typically executed elsewhere, through a control plane mechanism
	// like the OCI control panel or some other "coordinator" program.
	tableName := "customer_data"
	tableReq := &nosqldb.TableRequest{
		TableName: tableName,
		CDCConfig: &nosqldb.CDCConfig{
			Enabled: true,
		},
	}
	tableRes, err := client.DoTableRequest(tableReq)
	if err != nil {
		return fmt.Errorf("failed to enable CDC streaming on table %s: %v", tableName, err)
	}

	// The CDC enable table request is asynchronous. Wait for enablement to complete.
	_, err = tableRes.WaitForCompletion(client, 60*time.Second, time.Second)
	if err != nil {
		return fmt.Errorf("failed to enable CDC streaming on table %s: %v", tableName, err)
	}

	// Create a consumer config that will be used to create the consumers.
	config := client.CreateChangeConsumerConfig().
		TableName(tableName).
		Earliest().
		Group("test_group")

	// Create 5 consumer goroutines to consume data from the table. The consumers will
	// be created with a common Group ID, so change data will be evenly distributed
	// across them.
	//
	// Note that typically, multiple consumers for a table would be created in different
	// processes, likely on different machines or containers. This example is really just to
	// show how the group ID concept works.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		consumer, err := client.CreateChangeConsumer(config)
		if err != nil {
			return fmt.Errorf("error creating group consumer: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runGroupConsumer(consumer)
		}()
	}
	// Wait for the consumers to complete... which will only happen if the table is
	// dropped, or change streaming has been disabled for the table.
	wg.Wait()

	return nil
}

// Consume messages for one consumer in a group.
func runGroupConsumer(consumer *nosqldb.ChangeConsumer) {

	// Loop reading messages from the consumer
	for {
		// wait up to one second to read up to 10 events
		message, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			// This would only happen if the table is dropped and all messages
			// have been consumed
			fmt.Printf("error polling for CDC messages: %v", err)
			return
		}
		// Do something with message
		printCDCMessage(message)
	}
}

// Simple example to show how to add a table to an existing consumer
func runCDCAddTable(client *nosqldb.Client) error {

	// start with a consumer for one table
	consumer, err := client.CreateChangeConsumer(
		client.CreateChangeConsumerConfig().TableName("table1"))
	if err != nil {
		return fmt.Errorf("error creating consumer: %v", err)
	}

	// Read one message. This could be in a loop reading, etc...
	// wait up to one second to read up to 10 events
	message, err := consumer.Poll(10, time.Duration(1*time.Second))
	if err != nil {
		// This should only happen if the table has since been dropped or
		// CDC has been disabled (not likely in the few ms after consumer creation)
		return fmt.Errorf("error getting CDC messages: %v", err)
	}
	printCDCMessage(message)

	// Add a table to the consumer.
	err = consumer.AddTable("table_2", "", nosqldb.FirstUncommitted, nil)
	if err != nil {
		return fmt.Errorf("error adding table to consumer: %v", err)
	}

	// The next Poll call may now contain events for both tables
	message, err = consumer.Poll(10, time.Duration(1*time.Second))
	if err != nil {
		// This will happen if the second table doesn't exist or isn't enabled for CDC
		return fmt.Errorf("error getting CDC messages: %v", err)
	}
	printCDCMessage(message)

	return nil
}

// Example function that reads CDC checkpoint info from a file on
// startup and writes the checkpoint info to the file as it reads
// messages from the CDC stream.
//
// NOTE: this is not the recommended way to manage consumer
// positions. Instead, applications should either use the auto-commit
// feature, or call the manual commit methods to have the NoSQL
// system manage the positions of committed data internally.
func runCDCCheckpoint(client *nosqldb.Client) error {

	// Assume table "customer_data" exists already and is CDC enabled
	// Assume table "insight_data" exists already and is CDC enabled
	//
	var consumer *nosqldb.ChangeConsumer
	// if a previous run of this program wrote a checkpoint file,
	// read that and use its data as a checkpoint for our CDC consumer.
	fileName := "/tmp/cdc_cursor.json"
	bytes, err := os.ReadFile(fileName)
	if err == nil {
		consumer, err = client.CreateChangeConsumerAtCheckpoint(bytes)
		if err != nil {
			return fmt.Errorf("error creating consumer from %s: %v", fileName, err)
		}
	} else {
		// Create a new consumer for two tables, starting with the most current entry in
		// each table.
		config := client.CreateChangeConsumerConfig().
			AddTable("client_info", "", nosqldb.Latest, nil).
			AddTable("location_data", "", nosqldb.Latest, nil)
		consumer, err = client.CreateChangeConsumer(config)
		if err != nil {
			return fmt.Errorf("error creating consumer for two tables: %v", err)
		}
	}

	// Read 10 messages from the CDC stream. Write a checkpoint after every message received.
	for i := 0; i < 10; i++ {
		// wait up to one second to read up to 10 events
		message, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			return fmt.Errorf("error getting CDC messages: %v", err)
		}
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.

		fmt.Printf("Received message: %v", message)

		printCDCMessage(message)

		// write a file to enable starting this program again from a checkpoint
		// Note: the checkpoint data is a byte representation of a JSON string.
		// The data contained in the checkpoint is intended to be opaque to the
		// application; however, it may be used for tracing or debugging purposes.
		checkpoint := consumer.GetCheckpoint()
		outfile, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("can't open checkpoint file %s: %v", fileName, err)
		}
		_, err = outfile.Write(checkpoint)
		outfile.Close()
		if err != nil {
			return fmt.Errorf("can't write checkpoint data to file %s: %v", fileName, err)
		}
	}

	// This function can now be run again, picking up where it left off from the
	// latest checkpoint file.

	return nil

}

// Function to print all contents of a CDC stream message
func printCDCMessage(message *nosqldb.ChangeMessage) {
	fmt.Printf("Message: events remaining = %v", message.EventsRemaining)

	// ChangeEvents is an array of changes for the message
	for i := 0; i < len(message.ChangeEvents); i++ {
		event := message.ChangeEvents[i]
		fmt.Printf("Change event: \n")
		fmt.Printf(" table name: %s", event.TableName)
		fmt.Printf(" table compartment: %s", event.CompartmentOCID)
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
