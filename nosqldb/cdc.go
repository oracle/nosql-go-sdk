//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// ChangeType represents the type of change in a change event.
type ChangeType int

const (
	// Put specifies a row that was put into the table.
	Put = iota + 1

	// Delete specifies a row that was removed from the table.
	Delete
)

type ChangeImage struct {
	// RecordValue specifies all of the fields of the record.
	RecordValue *types.MapValue

	// RecordMetadata represents additional data for the record. Its value is TBD.
	RecordMetadata *types.MapValue
}

// ChangeEvent represents a single Change Data Capture event.
type ChangeRecord struct {

	// ID of the event
	EventID string

	// RecordKey specifies the fields in the record that make up the primary key.
	RecordKey *types.MapValue

	// The current image of the table record. If this is nil, this event represents
	// a Delete operation.
	CurrentImage *ChangeImage

	// The previous image of the table record, if it existed before this event
	BeforeImage *ChangeImage

	// ModificationTime specifies a timestamp indicating when the change event occurred.
	ModificationTime time.Time

	// ExpirationTime specifies a timestamp indicating when the record will expire. This will
	// be zero for Delete events.
	ExpirationTime time.Time

	// PartitionID specifies the CDC partition this event originated from.
	PartitionID int

	// RegionID specifies the NoSQL cloud region this event originated from.
// TODO: do we need this in CDC???
	RegionID int
}

type ChangeEvent struct {
	// The set of records in the change event. This will typically
	// be only one record, unless the stream has group-by-transaction
	// mode enabled, in which case there may be many records.
	Records []*ChangeRecord
}

// ChangeMessage represents a single message from the change stream
type ChangeMessage struct {
	// The table name for this set of events
	TableName string

	// The compartment OCID for this set of events. If this is empty,
	// the compartment is assumed to be the default compartment
	// for the tenancy.
	CompartmentOCID string

	// The table OCID for this set of events
	TableOCID string

	// Version of the event format
	Version string

	// Events is an array of events.
	Events []*ChangeEvent
}

// ChangeMessageBundle represents one or more ChangeMessages returned from a call to Poll().
type ChangeMessageBundle struct {

	// Internal: pointer to the consumer that generated this bundle
	consumer *ChangeConsumer

	// EventsRemaining specifies an estimate of the number of change events that are still remaining to
	// be consumed, not counting the events in this struct. This can be used to monitor if a reader of
	// the events consumer is keeping up with change messages for the table.
	// This value applies to only the table data that this specific consumer can receive in Poll() calls,
	// which may be less than the overall total if this consumer is one in a group of many active consumers.
	EventsRemaining int64

	// Messages is an array of messages containing change event data
	Messages []*ChangeMessage
}

// Mark the messages in the bundle as committed: all messages have been
// fully read and consumed, and the messages should not be read again by any
// current or future consumer in the group.
//
// Note that this commit implies commits on all previously polled messages from the
// same consumer (that is, messages that were returned from calls to Poll() before
// this one). Calling Commit() on a previous MessageBundle will have no effect.
func (mb *ChangeMessageBundle) Commit(time.Duration) error {
	// TODO
	return fmt.Errorf("function not implemented yet")
}

// Return true if the bundle is empty. This may happen if there was no
// change data to read in the given timeframe of a Poll().
func (mb *ChangeMessageBundle) IsEmpty() bool {
	return len(mb.Messages) == 0
}

type ChangeConsumerTableMetrics struct {
	// The table name for this table
	TableName string

	// The compartment OCID for this table.
	CompartmentOCID string

	// The table OCID for this table.
	TableOCID string

	// The number of messages remaining to be consumed. This may be an
	// estimate, and is based on the number of messages between the last
	// committed message and the most recent message produced.
	RemainingMessages uint64

	// The number of bytes remaining to be consumed. This may be an
	// estimate, and is based on the number of bytes between the last
	// committed message and the most recent message produced.
	RemainingBytes uint64

	// The timestamp of the oldest uncommitted message. This maybe an
	// estimate, and it represents the time that the message was placed
	// into the stream.
	OldestMessageTimestamp time.Time
}

// ChangeConsumerMetrics encapsulates metrics for all tables in a consumer group.
type ChangeConsumerMetrics struct {
	TableMetrics []ChangeConsumerTableMetrics
}

// Get the metrics for this specific consumer. The metrics data returned is
// relevant only to the change data that this consumer can read (as opposed
// to the metrics for the group as a whole).
func (cc *ChangeConsumer) GetMetrics() (*ChangeConsumerMetrics, error) {
	// TODO
	return nil, fmt.Errorf("function not implemented yet")
}

// Get metrics for the specified consumer group. The metrics are for the group
// as a whole (as opposed to metrics for each individual consumer in a group).
func (c *Client) GetChangeConsumerMetrics(groupID string) (*ChangeConsumerMetrics, error) {
	// TODO
	return nil, fmt.Errorf("function not implemented yet")
}

type ChangeLocationType string

const (
	// Start consuming at the first uncommitted message in the stream. This is
	// the default.
	FirstUncommitted ChangeLocationType = "firstUncommitted"

	// Start consuming from the earliest (oldest) available message in the stream.
	Earliest ChangeLocationType = "earliest"

	// Start consuming messages that were published after the start of the consumer.
	Latest ChangeLocationType = "latest"

	// Start consuming from a given time.
	AtTime ChangeLocationType = "atTime"
)

type ChangeStartLocation struct {
	// The location type (earliest, latest, etc). If this is empty,
	// FirstUncommitted is used as the default.
	Location ChangeLocationType `json:"locationType,omitempty"`

	// used for AtTime type
	StartTime time.Time `json:"startTime,omitempty"`
}

// ChangeConsumerTableConfig represents the details for a single table in
// a change data capture configuration. It is typically created using API
// calls:
//
//	config := c.CreateChangeConsumerConfig().
//	    AddTable("client_info", "", Latest, nil).
type ChangeConsumerTableConfig struct {
	// Name of the table. One of tableName or tableOCID are required.
	tableName string

	// Optional compartment ID for the table. If empty, the default compartment
	// for the tenancy is used.
	compartmentOCID string

	// Optional start location. If empty, FirstUncommitted is used as the default.
	startLocation ChangeStartLocation `json:"startLocation,omitempty"`

	// Table OCID. One of tableName or tableOCID are required.
	tableOCID string
}

// ChangeConsumerConfig represents the configuration to use when creating a ChangeConsumer.
// Typically this struct is created and populated with API calls rather than creating it
// directly:
//
//	config := c.CreateChangeConsumerConfig().
//	    AddTable("client_info", "", Latest, nil).
//	    AddTable("location_data", "", Latest, nil).
//	    GroupID("test_group").
//	    CommitAutomatic()
//	consumer, err := c.CreateChangeConsumer(config)
type ChangeConsumerConfig struct {
	// Tables to consume from. This array must have at least one table config defined.
	Tables []ChangeConsumerTableConfig `json:"tables"`

	// The group ID. In NoSQL Change Data Capture, every consumer is part of a "group".
	// The group may be a single consumer, or may have multiple consumers.
	//
	// When multiple consumers use the same group ID, the NoSQL system will attempt
	// to evenly distribute data for the specified tables evenly across all consumers in
	// the group. Data to any one consumer will be consistently ordered for records using
	// the same shard key. That is, different consumers will not get data for the same shard key.
	// Any group ID used should be sufficiently unique to avoid unintended alterations to
	// existing groups.
	//
	// If there is only one consumer in a group, this consumer will
	// always get all of the data for all of the tables specified in the group.
	//
	// NOTE: The Tables in this config will override any previous tables that other
	// existing consumers in this group have specified. Any tables that are being
	// currently consumed by this group that are not in the specified Tables list will
	// have their consumption stopped.
	//
	// If a table is already being consumed by other consumers in this group, this
	// consumer's start location for the table will be FirstUncommitted (the start location
	// specified in the config is ignored). If a table is not in the existing group (or if this the
	// first consumer in this group), the StartLocation in the table config will be used.
	// This behavior can be changed by setting ForceReset to true in the config.
	GroupId string `json:"groupId,omitempty"`

	// The compartment ID to use for the consumer group. If this is empty, the
	// default tenancy ID will be used.
	CompartmentOCID string `json:"compartmentOcid,omitempty"`

	// Specify the commit mode for the consumer. If this value is true, the system will not
	// automatically commit messages consumed by Poll(). It is the responsibility of the
	// application to call Commit() on a timely basis after consumed data has been processed.
	// If this value is false (the default), commits will be done automatically: every call
	// to Poll() will automatically mark the data returned by the previous Poll() as committed.
	ManualCommit bool `json:"manualCommit"`

	// Specify the maximum interval between calls to Poll() before the system will
	// consider this consumer as failed, which will trigger a rebalance operation to
	// redirect its change event data to other active consumers.
	MaxPollingInterval time.Duration

	// Force resetting the start location for the consumer(s) in the group. This is typically
	// only used when a consumer group is completely stopped and a new group with the same
	// group ID is to be started at a given start location (Earliest, Latest, etc).
	//
	// This setting will remove any existing consumers' committed locations.
	//
	// NOTE: Usage of this setting is dangerous, as any currently running consumers in the group
	// will have their current and committed locations reset unexpectedly.
	ForceReset bool `json:"-"`
}

// AddTable adds a table to the consumer config. The table must have already been
// CDC enabled via the OCI console or a NoSQL SDK TableRequest call.
//
// tableName: required. This may be the OCID of the table, if available.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
//
// startLocation: Specify the position of the first element to read in the change stream.
// If a table is already being consumed by other consumers in this group, this
// consumer's start location for the table will be FirstUncommitted (the start location
// specified in the config is ignored). If a table is not in the existing group (or if this the
// first consumer in this group), the startLocation in the table config will be used.
//
// startTime: If start location specifies AtTime, the startTime field is required to be non-nil.
func (cc *ChangeConsumerConfig) AddTable(tableName string, compartmentOCID string, startLocation ChangeLocationType, startTime *time.Time) *ChangeConsumerConfig {
	sl := ChangeStartLocation{
		Location:  startLocation,
	}
	if startTime != nil {
		sl.StartTime = *startTime
	}
	tc := ChangeConsumerTableConfig{
		tableName:       tableName,
		compartmentOCID: compartmentOCID,
		startLocation:   sl,
	}
	cc.Tables = append(cc.Tables, tc)
	return cc
}

// Specify the group ID. In NoSQL Change Data Capture, every consumer is part of a "group".
// The group may have a single consumer, or may have multiple consumers.
//
// When there is only a single consumer in a group, this consumer will
// always get all of the data for all of the tables specified in the group.
//
// When multiple consumers use the same group ID, the NoSQL system will attempt
// to evenly distribute data for the specified tables evenly across all consumers in
// the group. Data to any one consumer will be consistently ordered for records using
// the same shard key. That is, different consumers will not get data for the same shard key.
// Any group ID used should be sufficiently unique to avoid unintended alterations to
// existing groups.
//
// NOTE: The Tables in this config will override any previous tables that other
// existing consumers in this group have specified. Any tables that are being
// currently consumed by this group that are not in the specified Tables list will
// have their consumption stopped: calls to [ChangeConsumer.Poll] from existing consumers will
// no longer contain data for tables not specified in this config.
//
// If a table is already being consumed by other consumers in this group, this
// consumer's start location for the table will be FirstUncommitted (the start location
// specified in the config is ignored). If a table is not in the existing group (or if this the
// first consumer in this group), the StartLocation in the table config will be used.
// This behavior can be changed by specifying ResetStartLocation() in the config.
func (cc *ChangeConsumerConfig) GroupID(groupID string) *ChangeConsumerConfig {
	cc.GroupId = groupID
	return cc
}

// Specify automatic commit mode for the consumer. This is the default if not specified.
// In this mode, commits will be done automatically: every call
// to [ChangeConsumer.Poll] will automatically mark the data returned by the previous
// call to [ChangeConsumer.Poll] as committed.
func (cc *ChangeConsumerConfig) CommitAutomatic() *ChangeConsumerConfig {
	cc.ManualCommit = false
	return cc
}

// Specify manual commit mode for the consumer. The system will not
// automatically commit messages consumed by [ChangeConsumer.Poll]. It is the responsibility
// of the application to call [ChangeConsumer.Commit] on a timely basis after consumed data has been processed.
func (cc *ChangeConsumerConfig) CommitManual() *ChangeConsumerConfig {
	cc.ManualCommit = true
	return cc
}

// Specify the maximum interval between calls to Poll() before the system will
// consider this consumer as failed, which will trigger a rebalance operation to
// redirect its change event data to other active consumers.
//
// If not specified, the default value for MaxPollInterval is 30 seconds.
//
// Note: if a consumer process dies, the data that it would be consuming
// will not be consumed by any other consumers in this consumer's group until
// this interval expires.
func (cc *ChangeConsumerConfig) MaxPollInterval(interval time.Duration) *ChangeConsumerConfig {
	cc.MaxPollingInterval = interval
	return cc
}

// Force resetting the start location for the consumer(s) in the group. This is typically
// only used when a consumer group is completely stopped and a new group with the same
// group ID is to be started at a given start location (Earliest, Latest, etc).
//
//	NOTE: Usage of this setting is dangerous, as any currently running consumers in the group
//	      will have their current and committed locations reset unexpectedly.
//
// This setting will remove any existing consumers' committed locations.
func (cc *ChangeConsumerConfig) ForceResetStartLocation() *ChangeConsumerConfig {
	cc.ForceReset = true
	return cc
}

func (c *Client) CreateChangeConsumerConfig() *ChangeConsumerConfig {
	return &ChangeConsumerConfig{
		GroupId:      "",
		ManualCommit: false,
	}
}

// The main struct used for Change Data Capture.
//
// NOTE: this struct in not thread-safe, with the exception of calling
// Commit(), which can be done in other threads/goroutines. Calling
// Poll() from multiple routines/threads will result in undefined behavior.
type ChangeConsumer struct {
	cursor []byte
	config *ChangeConsumerConfig
	client *Client
}

// Create a Change Data Capture consumer based on configuration.
//
// This will make a server-side call to validate all configuration and
// establish server-side state for the consumer.
//
// Any table changes (added tables, removed tables, start locations) for
// the consumer group will be applied immediately after this call succeeds.
// It is not necessary to call Poll() to trigger table changes.
//
// Note that rebalancing operations will not take effect after this call.
// Rebalancing does not happen until the first call to Poll().
func (c *Client) CreateChangeConsumer(config *ChangeConsumerConfig) (*ChangeConsumer, error) {
	req := &cdcConsumerRequest{config: config, mode: CreateConsumer}

	// Validate all tables in the config. This will also populate the tableOCID
	// for each table, which is used internally for all accesses.
	for i, table := range config.Tables {
		if table.tableOCID != "" {
			// TODO: verify in OCID format
			continue
		}
		if table.tableName == "" {
			return nil, fmt.Errorf("missing table name in consumer configuration")
		}
		if table.compartmentOCID != "" {
// TODO: config allows different compartments, but user is in one compartment, and GetTable()
// uses the single user's compartment.... hmmm.
		}
		getTableReq := &GetTableRequest{TableName: table.tableName}
		res, err := c.GetTable(getTableReq)
		if err != nil {
			return nil, fmt.Errorf("can't get table '%s' information: %v\n",
							table.tableName, err)
		}
		config.Tables[i].tableOCID = res.TableOcid
fmt.Printf("Using ocid=%s for table=%s\n", res.TableOcid, table.tableName)
	}

	res, err := c.execute(req)
	if err != nil {
		if strings.Contains(err.Error(), "unknown opcode") {
			return nil, nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
		}
		return nil, err
	}
	if resp, ok := res.(*cdcConsumerResult); ok {
		if resp.cursor == nil {
			return nil, nosqlerr.New(nosqlerr.BadProtocolMessage, "Response missing cursor")
		}
		cc := &ChangeConsumer{config: config, cursor: resp.cursor, client: c}
		return cc, nil
	}
	return nil, errUnexpectedResult
}

// Create a single-table consumer with all default parameters.
//
// This is a convenience method for crating a consumer for a single table.
// It is equivalent to:
//
//	config := c.CreateChangeConsumerConfig().
//	    AddTable(tableName, "", FirstUncommitted, nil).
//	    GroupID(groupID)
//	consumer, err := c.CreateChangeConsumer(config)
func (c *Client) CreateSimpleChangeConsumer(tableName, groupID string) (*ChangeConsumer, error) {
	config := c.CreateChangeConsumerConfig().
		AddTable(tableName, "", FirstUncommitted, nil).
		GroupID(groupID)
	return c.CreateChangeConsumer(config)
}

// Get Change Data Capture messages for a consumer.
//
// limit: max number of change messages to return in the bundle. This value can be set to
// zero to specify that this consumer is alive and active in the group without actually
// returning any change events.
//
// waitTime: max amount of time to wait for messages
//
// If this is the first call to Poll() for a consumer, this call may trigger
// a rebalance operation to redistribute change data across this and all other active consumers.
// Note that the rebalance may not happen immediately; in the NoSQL system,
// rebalanace operations are rate limitied to avoid excessive resource
// usage when many consumers are being added to or removed from a group.
//
// This method in not thread-safe. Calling Poll() on the same consumer instance
// from multiple routines/threads will result in undefined behavior.
func (cc *ChangeConsumer) Poll(limit int, waitTime time.Duration) (bundle *ChangeMessageBundle, err error) {
	pollInterval := 100 * time.Millisecond // TODO: config?
	start := time.Now();
	for {
		bundle, err = cc.pollOnce(limit)
		if err != nil {
			return
		}
		if len(bundle.Messages) > 0 {
			return
		}
		// if no messages, sleep for a short period and retry
		// TODO: backoff algorithm?
		// if nearing end of waitTime, bail out
		if time.Since(start) + pollInterval > waitTime {
			return
		}

		time.Sleep(pollInterval)
	}
}

func (cc *ChangeConsumer) pollOnce(limit int) (*ChangeMessageBundle, error) {
	req := &cdcPollRequest{consumer: cc, maxEvents: limit}
	res, err := cc.client.execute(req)
	if err != nil {
		if strings.Contains(err.Error(), "unknown opcode") {
			return nil, nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
		}
		return nil, err
	}
	if res, ok := res.(*cdcPollResult); ok {
		cc.cursor = res.cursor
		res.bundle.consumer = cc
		return res.bundle, nil
	}
	return nil, errUnexpectedResult
}

// Mark the data from the most recent call to [ChangeConsumer.Poll] as committed: the consumer has
// completely processed the data and it should be considered "consumed".
//
// Note that this commit implies commits on all previously polled messages from the
// same consumer (that is, messages that were returned from calls to Poll() before
// this one).
//
// This method is only necessary when using manual commit mode. Otherwise, in auto commit mode, the
// commit is implied for all previous data every time [ChangeConsumer.Poll] is called.
func (cc *ChangeConsumer) Commit(timeout time.Duration) error {
	return fmt.Errorf("function not implemented yet")
}

// Mark the data from the given MessageBundle as committed: the consumer has
// completely processed the data and it should be considered "consumed".
//
// Note that this commit implies commits on all previously polled messages from the
// same consumer (that is, messages that were returned from calls to Poll() before
// this one). Calling CommitBundle() on a previous MessageBundle will have no effect.
//
// This method is only necessary when using manual commit mode. Otherwise, in auto commit mode, the
// commit is implied for all previous data every time [ChangeConsumer.Poll] is called.
func (cc *ChangeConsumer) CommitBundle(bundle *ChangeMessageBundle, timeout time.Duration) error {
	return fmt.Errorf("function not implemented yet")
}

// AddTable adds a table to an existing consumer. The table must have already been
// CDC enabled via the OCI console or a NoSQL SDK [Client.DoTableRequest] call.
// If the given table already exists in the group, this call is ignored and will return no error.
//
// Note this will affect all active consumers using the same group ID.
//
// tableName: required. This may be the OCID of the table, if available.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
//
// startLocation: Specify the position of the first element to read in the change stream.
//
// startTime: If start location specifies AtTime, the startTime field is required to be non-nil.
func (cc *ChangeConsumer) AddTable(tableName string, compartmentOCID string, startLocation ChangeLocationType, startTime *time.Time) error {
	// TODO
	return fmt.Errorf("function not implemented yet")
}

// RemoveTable removes a table from an existing change consumer group.
// If the given table does not exist in the group, this call is ignored and
// will return no error.
//
// Note this will affect all active consumers using the same group ID.
//
// tablename: required.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
func (cc *ChangeConsumer) RemoveTable(tableName string, compartmentOCID string) error {
	// TODO
	return fmt.Errorf("function not implemented yet")
}

// Close and release all resources for this consumer instance.
//
// Call this method if the application does not intend to continue using
// this consumer. If this consumer was part of a group and has called Poll(),
// this call will trigger a rebalance such that data that was being directed
// to this consumer will now be redistributed to other active consumers.
//
// If the consumer is in auto-commit mode, calling Close() will implicitly call
// Commit() on the most recent events returned from Poll().
//
// It is not required to call this method. If a consumer has not called [ChangeConsumer.Poll]
// within the maximum poll period, it will be considered closed by the system and a
// rebalance may be triggered at that point.
func (cc *ChangeConsumer) Close() error {
	req := &cdcConsumerRequest{cursor: cc.cursor, mode: CloseConsumer}
	res, err := cc.client.execute(req)
	if err != nil {
		if strings.Contains(err.Error(), "unknown opcode") {
			return nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
		}
		return err
	}
	if res, ok := res.(*cdcConsumerResult); ok {
		if res.cursor != nil {
			return nosqlerr.New(nosqlerr.UnknownError, "Consumer not closed on server side")
		}
		return nil
	}
	return errUnexpectedResult
}

// Get the minimum number of consumers needed in order to process all change
// data given a set of tables and start location, and a desired amount of time to process
// the data.
//
// config: use this field to specify the tables and their start locations.
// all other data in the config is ignored, with the exception of GroupID (see below).
//
// timeToProcess: specify the desired time to finish processing of the change
// data in the given tables from their respective start points.
//
// This function returns an estimate based only on the amount of data that exists
// in the change streams for the given tables, and the fastest rate that the system can
// deliver that data to each consumer. It does not take into account any additional
// processing time used by the consumer applications.
//
// Note: if GroupID is specified in the config, and the start location in the config is
// NextUncommitted, and this is an existing group, this call will return the count based
// on the remaining data that has yet to be read and committed for the existing group.
func (c *Client) GetMinimumChangeConsumerCount(config *ChangeConsumerConfig, processTime time.Duration) (int, error) {
	// TODO
	return 0, fmt.Errorf("function not implemented yet")
}

// AddTableToChangeConsumerGroup adds a table to an existing change consumer group
// based on the groupID. The table must have already been
// CDC enabled via the OCI console or a NoSQL SDK TableRequest call.
// If the given group ID does not exist in the system, this call will return an error.
// If the given table already exists in the group, this call is ignored and
// will return no error.
//
// groupID: required.
//
// tablename: required.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
//
// startLocation: Specify the position of the first element to read in the change stream.
//
// startTime: If start location specifies AtTime, the startTime field is required to be non-nil.
func (c *Client) AddTableToChangeConsumerGroup(groupID string, tableName string, compartmentOCID string, startLocation ChangeLocationType, startTime *time.Time) error {
	// TODO
	return fmt.Errorf("function not implemented yet")
}

// RemoveTableFromChangeConsumerGroup removes a table from an existing change consumer group
// based on the groupID.
// If the given group ID does not exist in the system, this call will return an error.
// If the given table does not exist in the group, this call is ignored and
// will return no error.
//
// groupID: required.
//
// tablename: required.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
func (c *Client) RemoveTableFromChangeConsumerGroup(groupID string, tableName string, compartmentOCID string) error {
	// TODO
	return fmt.Errorf("function not implemented yet")
}

// DeleteChangeConsumerGroup deletes all metadata (current poll positions,
// committed message locations, etc) from a consumer group, effectively
// removing it from the system.
//
// Any active consumers currently polling this group will get errors on
// all successive calls to Poll() after this call completes.
//
//	Note: This method is dangerous, as it will affect any currently
//	      running consumers for this group.
//
// This method is typically used during testing, where it may be desirable
// to immediately stop and clean up all resources used by a group.
func (c *Client) DeleteChangeConsumerGroup(groupID string) error {
	// TODO
	return fmt.Errorf("function not implemented yet")
}

func (c *Client) simpleCDCTest() error {
	// Create a single (non-grouped) consumer for a single table.
	consumer, err := c.CreateSimpleChangeConsumer("test_table", "group1")
	if err != nil {
		return fmt.Errorf("error creating change consumer: %v", err)
	}

	// read data from the stream, returning only if the stream has ended.
	for {
		// wait up to one second to read up to 10 events
		message, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			return fmt.Errorf("error getting CDC messages: %v", err)
		}
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.
		fmt.Printf("Received message: %v", message)
	}
}

func (c *Client) multiTableCDCTest() error {

	// Create a new consumer for two tables, starting with the most current entry in
	// each table's CDC stream.
	config := c.CreateChangeConsumerConfig().
		AddTable("client_info", "", Latest, nil).
		AddTable("location_data", "", Latest, nil).
		GroupID("test_group").
		CommitAutomatic()
	consumer, err := c.CreateChangeConsumer(config)
	if err != nil {
		return err
	}

	// Read 10 messages from the CDC stream.
	for i := 0; i < 10; i++ {
		// wait up to one second to read up to 10 events
		message, err := consumer.Poll(10, time.Duration(1*time.Second))
		if err != nil {
			return fmt.Errorf("error getting CDC messages: %v", err)
		}
		// If the time elapsed but there were no messages to read, the returned message
		// will have an empty array of events.
		fmt.Printf("Received message: %v", message)
	}

	return nil
}
