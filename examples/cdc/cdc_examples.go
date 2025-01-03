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

	err = runCDCSimple(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCGroupConsumerRoutines(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCMultipleTablesManualCommit(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCManualCommitRoutines(client)
	if err != nil {
		fmt.Println(err)
	}

	err = runCDCAddTable(client)
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

	// Create a simple change consumer that will read change events for a
	// single table, starting at the first uncommitted message for the
	// given group ID.
	//
	// If no other consumer has ever been created for this group, the
	// consumer will start with the first messages available (i.e. "Earliest").
	consumer, err := client.CreateSimpleChangeConsumer("customer_data", "my_group")
	if err != nil {
		return fmt.Errorf("error creating simple consumer: %v", err)
	}

	// Loop reading messages from the consumer
	for {
		// wait up to one second to read up to 10 messages
		bundle, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			// This will typically only happen if the table is dropped and all messages
			// have been consumed, or if the group is manually deleted elsewhere
			return fmt.Errorf("error polling for CDC messages: %v", err)
		}
		// Do something with messages
		printCDCMessageBundle(bundle)
	}
}

// Example of running a consumer across multiple tables, doing manual
// commits.
// In this example, it is assumed that the tables already exist and
// have already been enabled for CDC streaming.
func runCDCMultipleTablesManualCommit(client *nosqldb.Client) error {

	// Create a consumer config that will be used to create the consumer.
	// The start locations will be used only if there are no existing
	// active consumers already in the group.
	config := client.CreateChangeConsumerConfig().
		AddTable("customer_data", "", nosqldb.Earliest, nil).
		AddTable("internal_metadata", "", nosqldb.FirstUncommitted, nil).
		AddTable("item_locations", "", nosqldb.AtTime, nil).
		GroupID("test_group").
		CommitManual()

	// Create a consumer to read change events from the above tables
	consumer, err := client.CreateChangeConsumer(config)
	if err != nil {
		return fmt.Errorf("error creating consumer: %v", err)
	}

	// Loop reading messages from the consumer
	for {
		// wait up to one second to read up to 10 messages
		bundle, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			// This will typically only happen if all tables are dropped and all messages
			// have been consumed, or if the group is manually deleted elsewhere
			return fmt.Errorf("error polling for CDC messages: %v", err)
		}
		// Do something with messages
		printCDCMessageBundle(bundle)
		// Commit after all change events are processed
		consumer.Commit(time.Duration(1 * time.Second))

		// A real application may do some other operations after processing
		// the messages, which may consume some time or resources. The above Commit()
		// call ensures that if the additional processing causes the program
		// to exit for some reason, the change events read in the last Poll() call
		// will not be read again by any other existing or restarted consumers.
	}
}

// Example that runs a single consumer, passing messages to goroutines.
// The goroutines are responsible for committing the messages when they
// are finished processing the change data.
// This returns only when all data for a table has been completely
// consumed - either because the table was dropped, or its CDC streaming
// was disabled elsewhere.
func runCDCManualCommitRoutines(client *nosqldb.Client) error {

	// Assume that there is a mechanism to set this flag elsewhere
	shouldExit := false

	// Assume table "test_table" exists already and is CDC enabled

	// Create a simple consumer for one table, setting commit mode to manual.
	// Start at the first uncommitted message in the stream. This setting allows
	// for this program to be restarted after failure, and consuming will pick up
	// where the failed process left off.
	config := client.CreateChangeConsumerConfig().
		AddTable("test_table", "", nosqldb.FirstUncommitted, nil).
		GroupID("test_group").
		CommitManual()

	// Create a channel to pass messages to the goroutines. Use buffering so there
	// can be many messages waiting in the channel.
	// Pass pointers in the channel to avoid large memory copies of change data.
	messageChan := make(chan *nosqldb.ChangeMessageBundle, 10)

	// create a single consumer to read change data from
	consumer, err := client.CreateChangeConsumer(config)
	if err != nil {
		return fmt.Errorf("error creating consumer: %v", err)
	}

	// Start 4 goroutines that will each read MessageBundles from the channel, and
	// commit them when they are done processing them
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			// Note we don't have to pass the consumer itself to the goroutines. A pointer to
			// it is maintained internally in MessageBundles.
			runChannelConsumer(messageChan)
		}()
	}

	// Loop reading messages from the consumer, passing them to the
	// goroutines via the channel.
	for {
		// wait up to one second to read up to 10 messages
		bundle, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			// This will typically only happen if all tables are dropped and all messages
			// have been consumed, or if the group is manually deleted elsewhere
			return fmt.Errorf("error polling for CDC messages: %v", err)
		}

		// The bundle may be empty, if no change data was available in the given timeframe.
		// no need to send empty bundles to routines. Committing an empty bundle has no effect.
		if bundle.IsEmpty() {
			continue
		}

		// Send message bundle to the channel. This will block if the channel buffer is full.
		// the readers of the channel are responsible for calling commit.
		messageChan <- bundle

		// Typically, a loop like this would check for signals/flags to tell it to quit.
		if shouldExit {
			close(messageChan)
			// call close on the consumer to tell the system that it will
			// no longer be pollng for messages. Note that commits that happen after
			// the consumer is closed will still succeed.
			consumer.Close()
			break
		}
	}

	// Wait for the consumers to complete
	wg.Wait()

	return nil
}

// Consume messages for by reading them from a channel. Commit each message
// after processing is done.
func runChannelConsumer(messageChan chan *nosqldb.ChangeMessageBundle) {
	// Loop reading messages from the channel
	for {
		bundle, ok := <-messageChan
		if !ok {
			// channel closed by parent
			break
		}

		// Do something with change data in message bundle
		printCDCMessageBundle(bundle)

		// After completely using the change data, commit the messages in the bundle
		bundle.Commit(time.Duration(1 * time.Second))
	}
}

// Example that runs many consumers in goroutines for
// a change data capture stream for a single table.
// This returns only when all data for a table has been completely
// consumed - either because the table was dropped, or its CDC streaming
// was disabled elsewhere.
func runCDCGroupConsumerRoutines(client *nosqldb.Client) error {

	// Assume table "customer_data" exists already

	// Make an API call to enable CDC for this table. If it is already enabled,
	// no error is returned.
	// Note this could be executed elsewhere, through a control plane mechanism
	// like the OCI control panel, or REST API call.
	tableName := "customer_data"
	tableReq := &nosqldb.TableRequest{
		TableName:  tableName,
		CDCEnabled: true,
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
		AddTable(tableName, "", nosqldb.Earliest, nil).
		GroupID("test_group")

	// Create 4 consumer goroutines to consume data from the table. The consumers will
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
		// wait up to one second to read up to 10 messages
		bundle, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			// This would only happen if the table is dropped and all messages
			// have been consumed
			fmt.Printf("error polling for CDC messages: %v", err)
			return
		}
		// Do something with messages
		printCDCMessageBundle(bundle)
	}
}

// Simple example to show how to add a table to an existing consumer
func runCDCAddTable(client *nosqldb.Client) error {

	// start with a consumer for one table
	consumer, err := client.CreateSimpleChangeConsumer("table1", "group1")
	if err != nil {
		return fmt.Errorf("error creating consumer: %v", err)
	}

	// Read one message. This could be in a loop reading, etc...
	// wait up to one second to read up to 10 messages
	bundle, err := consumer.Poll(10, time.Duration(1*time.Second))
	if err != nil {
		// This should only happen if the table has since been dropped or
		// CDC has been disabled (not likely in the few ms after consumer creation)
		return fmt.Errorf("error getting CDC messages: %v", err)
	}
	printCDCMessageBundle(bundle)

	// Add a table to the consumer.
	err = consumer.AddTable("table_2", "", nosqldb.FirstUncommitted, nil)
	if err != nil {
		return fmt.Errorf("error adding table to consumer: %v", err)
	}

	// The next Poll call may now contain messages for both tables
	bundle, err = consumer.Poll(10, time.Duration(1*time.Second))
	if err != nil {
		// This will happen if the second table doesn't exist or isn't enabled for CDC
		return fmt.Errorf("error getting CDC messages: %v", err)
	}
	printCDCMessageBundle(bundle)

	return nil
}

// Function to print all contents of a CDC stream message bundle
func printCDCMessageBundle(bundle *nosqldb.ChangeMessageBundle) {
	fmt.Printf("MessageBundle: messages remaining = %v", bundle.MessagesRemaining)

	// ChangeMessages is an array of messages for the bundle
	for m := 0; m < len(bundle.ChangeMessages); m++ {
		message := bundle.ChangeMessages[m]
		// Each message will typically have one event, unless group-by-transaction
		// is enabled on the stream and this message represents multiple events in a
		// single transaction
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
}
