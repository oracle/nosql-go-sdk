//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"context"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

var errNilClient = nosqlerr.NewIllegalArgument("client must be non-nil")

// Result is an interface that represents the operation result for a request.
//
// All operation results should satisfy this interface.
type Result interface {
	// ConsumedCapacity is a function used to return the read, write throughput
	// consumed by an operation.
	ConsumedCapacity() (Capacity, error)

	Delayed() DelayInfo
}

// DelayInfo contains information about the amount of time a request was delayed.
type DelayInfo struct {
	// RateLimitTime represents the time delayed due to internal rate limiting.
	RateLimitTime time.Duration
	// RetryTime represents the time delayed due to internal request retries.
	RetryTime time.Duration
}

// Delayed returns the time delay information for a completed request.
func (d DelayInfo) Delayed() DelayInfo {
	return d
}

func (d DelayInfo) setRateLimitTime(t time.Duration) {
	d.RateLimitTime = t
}

func (d DelayInfo) setRetryTime(t time.Duration) {
	d.RetryTime = t
}

// Capacity represents the read/write throughput consumed by an operation.
type Capacity struct {
	// ReadKB represents the number of kilo bytes consumed for reads.
	ReadKB int `json:"readKB"`

	// WriteKB represents the number of kilo bytes consumed for writes.
	WriteKB int `json:"writeKB"`

	// ReadUnits represents the number of read units consumed for reads.
	//
	// A read unit represents 1 eventually consistent read per second for data
	// up to 1 KB in size. A read that is absolutely consistent is double that,
	// consuming 2 read units for a read of up to 1 KB in size.
	ReadUnits int `json:"readUnits"`
}

// String returns a JSON string representation of the Capacity.
func (r Capacity) String() string {
	return jsonutil.AsJSON(r)
}

// ConsumedCapacity returns the read/write throughput consumed by an operation.
func (r Capacity) ConsumedCapacity() (Capacity, error) {
	return r, nil
}

// noCapacity represents an empty capacity.
//
// It implements the Result interface and is used for operations that do not
// care about consumed capacity.
type noCapacity struct{}

func (r noCapacity) ConsumedCapacity() (Capacity, error) {
	return Capacity{}, nil
}

// GetResult represents the result of a Client.Get() operation.
//
// On a successful operation the value of the row is available in
// GetResult.Value and the other state available in this struct is valid.
//
// On failure that value is nil and other state, other than consumed capacity,
// is undefined.
type GetResult struct {
	Capacity

	// Value represents the value of the returned row, or nil if the row does not exist.
	Value *types.MapValue `json:"value"`

	// Version represents the version of the row if the operation was
	// successful, or nil if the row does not exist.
	Version types.Version `json:"version"`

	// ExpirationTime represents the expiration time of the row.
	// A zero value of time.Time indicates that the row does not expire.
	// This value is valid only if the operation successfully returned a row,
	// which means the returned Value is non-nil.
	ExpirationTime time.Time `json:"expirationTime"`

	DelayInfo
}

// String returns a JSON string representation of the GetResult.
func (r GetResult) String() string {
	return jsonutil.AsJSON(r)
}

// ValueAsJSON returns a JSON string representation of the GetResult.Value.
func (r GetResult) ValueAsJSON() string {
	if r.Value == nil {
		return ""
	}
	return jsonutil.AsJSON(r.Value.Map())
}

// RowExists checks if the desired row exists.
// It returns true if the get operation successfully finds the row with
// specified key, returns false otherwise.
func (r GetResult) RowExists() bool {
	return len(r.Version) > 0
}

// SystemResult represents a result returned from Client.GetSystemStatus() and
// Client.DoSystemRequest() operations. It encapsulates the state of the
// operation requested.
//
// Some operations performed by DoSystemRequest() are asynchronous. When such an
// operation has been performed it is necessary to call GetSystemStatus() until
// the status of the operation is known. The method SystemResult.WaitForCompletion()
// exists to perform this task and should be used whenever possible.
//
// Asynchronous operations (e.g. create namespace) can be distinguished from
// synchronous System operations in this way:
//
//   a. Asynchronous operations may return a non-nil OperationID.
//   b. Asynchronous operations modify state, while synchronous operations are read-only.
//   c. Synchronous operations return a state of types.Complete and have a non-nil ResultString.
//
// Client.GetSystemStatus() is synchronous, returning the known state of the
// operation. It should only be called if the operation was asynchronous and
// returned a non-null OperationID.
//
// SystemResult is used for on-premise only.
type SystemResult struct {
	noCapacity
	DelayInfo

	// State represents the current state of the operation.
	State types.OperationState `json:"state"`

	// OperationID represents the id for the operation if it was asynchronous.
	// This is an empty string if the request did not generate a new operation.
	// The value can be used in SystemStatusRequest.OperationID to find
	// potential errors resulting from the operation.
	//
	// This is only useful for the result of asynchronous operations.
	OperationID string `json:"operationID"`

	// Statement represents the statement used for the operation.
	Statement string `json:"statement"`

	// ResultString represents the result string for the operation.
	// This may be empty if the operation did not return a result, or may be
	// a JSON string that contains the result for the operation.
	ResultString string `json:"resultString"`
}

// String returns a JSON string representation of the SystemResult.
func (r SystemResult) String() string {
	return jsonutil.AsJSON(r)
}

// WaitForCompletion waits for the operation to be complete.
//
// This is a blocking, polling style wait that pauses the current goroutine for
// the specified duration between each polling operation.
//
// This SystemResult is modified with any changes in state.
func (r *SystemResult) WaitForCompletion(client *Client, timeout, pollInterval time.Duration) (*SystemResult, error) {
	if r == nil {
		return nil, nosqlerr.NewIllegalArgument("SystemResult must be non-nil")
	}

	if r.State == types.Complete {
		return r, nil
	}

	if pollInterval == 0 {
		pollInterval = 500 * time.Millisecond
	}

	var err error
	if err = validateWaitTimeout(timeout, pollInterval); err != nil {
		return nil, err
	}

	var req *SystemStatusRequest
	var res *SystemResult
	req = &SystemStatusRequest{
		OperationID: r.OperationID,
		Statement:   r.Statement,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		res, err = client.getSystemStatusWithContext(ctx, req)
		if err != nil {
			return nil, err
		}

		if res != nil && res.State == types.Complete {
			// Do partial copy of new state.
			// Statement and OperationId are not changed.
			r.ResultString = res.ResultString
			r.State = res.State
			return r, nil
		}

		// The operation has not completed, continue to check its status after
		// the specified duration if timeout has not elapsed.
		if shouldRetryAfter(ctx, pollInterval) {
			continue
		}

		return nil, nosqlerr.NewRequestTimeout("operation did not complete "+
			"within specified time %v", timeout)
	}
}

// TableResult is returned from Client.GetTable() and Client.DoTableRequest()
// operations. It encapsulates the state of the table specified in the request.
//
// Operations available in Client.DoTableRequest() such as table creation,
// modification and drop are asynchronous operations. When such an operation has
// been performend, it is necessary to call Client.GetTable() until the status
// of the table is Active or there is an error condition. TableResult provides
// a convenience method WaitForCompletion() to perform such tasks and should be
// used whenever possible.
//
// Client.GetTable() is synchronous, it returns static information about the
// table as well as its current state.
type TableResult struct {
	noCapacity
	DelayInfo

	// TableName represents the name of target table.
	TableName string `json:"tableName"`

	// State represents current state of the table.
	// A table in Active state or Updating state is usable for normal operation.
	// It is not permitted to perform table modification operations while the
	// table is in Updating state.
	State types.TableState `json:"state"`

	// Limits represents read/write throughput and storage limits for the table.
	Limits TableLimits `json:"limits"`

	// Schema represents table schema and any other metadata available for the table.
	// The returned schema may subject to change in future releases.
	Schema string `json:"schema"`

	// OperationID represents the operation id for an asynchronous operation.
	// This is empty if the request did not generate a new operation. The value
	// can be used in GetTableRequest.OperationId to find potential errors
	// resulting from the operation.
	OperationID string `json:"operationID"`
}

// String returns a JSON string representation of the TableResult.
func (r TableResult) String() string {
	return jsonutil.AsJSON(r)
}

// WaitForCompletion waits for a table operation to complete.
//
// Table operations are asynchronous. The method blocks checking for the table
// state until the specified timeout elapses or the table reaches a terminal
// state, which is either Active or Dropped. It is a polling style wait that
// pauses the current goroutine for a specified duration between each polling
// attempts.
//
// This instance must be the return value of a previous Client.DoTtableRequest()
// and contain a non-nil operation id representing the in-progress operation
// unless the operation has already completed.
//
// The timeout parameter specifies the total amount of time to wait. It must be
// greater than the specified pollInterval.
//
// The pollInterval parameter specifies the amount of time to wait between
// polling attempts. It must be greater than or equal to 1 millisecond. If it
// is set to zero, the default of 500 milliseconds will be used.
//
// If the table has reached the terminal state before specified timeout elapses,
// the method returns a TableResult that contains the current table state, and a
// nil error. Otherwise, it returns a nil TableResult and the error ocurred.
//
// This instance is modified with any change in table state or metadata.
func (r *TableResult) WaitForCompletion(client *Client, timeout, pollInterval time.Duration) (*TableResult, error) {
	if r == nil {
		return nil, nosqlerr.NewIllegalArgument("TableResult must be non-nil")
	}

	if r.State.IsTerminal() {
		return r, nil
	}

	if r.OperationID == "" {
		return nil, nosqlerr.NewIllegalArgument("OperationID must not be empty")
	}

	if client == nil {
		return nil, errNilClient
	}

	if pollInterval == 0 {
		pollInterval = 500 * time.Millisecond
	}

	var err error
	if err = validateWaitTimeout(timeout, pollInterval); err != nil {
		return nil, err
	}

	var req *GetTableRequest
	var res *TableResult
	// Creates a GetTableRequest with the table name and operation id.
	req = &GetTableRequest{
		TableName:   r.TableName,
		OperationID: r.OperationID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		res, err = client.getTableWithContext(ctx, req)
		if err != nil {
			return nil, err
		}

		if res != nil && res.State.IsTerminal() {
			// Do partial "copy" of possibly modified state. Do not modify
			// OperationID as that is what we are waiting to complete.
			r.State = res.State
			r.Limits = res.Limits
			r.Schema = res.Schema
			return r, nil
		}

		// Target table has not reached the desired state, continue to check
		// its status after the specified delay if specified timeout has not elapsed.
		if shouldRetryAfter(ctx, pollInterval) {
			continue
		}

		return nil, nosqlerr.NewRequestTimeout("table %q does not reach a terminal state "+
			"within specified time %v", r.TableName, timeout)
	}
}

// ListTablesResult represents the result of a Client.ListTables() operation.
//
// On a successful operation the table names are available as well as the
// index of the last returned table. Table names are returned in a slice of
// string, sorted in alphabetical order.
type ListTablesResult struct {
	noCapacity
	DelayInfo

	// Tables represents a slice of string that contains table names returned
	// by the operation, in alphabetical order.
	Tables []string `json:"tables"`

	// LastIndexReturnedthe represents index of the last table name returned.
	// This can be provided to ListTablesRequest.StartIndex to be used as a
	// starting point for listing tables.
	LastIndexReturned uint `json:"lastIndexReturned"`
}

// String returns a JSON string representation of the ListTablesResult.
func (r ListTablesResult) String() string {
	return jsonutil.AsJSON(r)
}

// GetIndexesResult represents the result of a Client.GetIndexes() operation.
//
// On a successful operation the index information is returned in a slice of IndexInfo.
type GetIndexesResult struct {
	noCapacity
	DelayInfo

	// Indexes represents a slice of IndexInfo that contains index information
	// returned by the operation.
	Indexes []IndexInfo `json:"indexes"`
}

// String returns a JSON string representation of the GetIndexesResult.
func (r GetIndexesResult) String() string {
	return jsonutil.AsJSON(r)
}

// IndexInfo represents the information about a single index including its name and field names.
type IndexInfo struct {
	// IndexName represents the name of index.
	IndexName string `json:"indexName"`

	// FieldNames represents a slice of string that contains names of fields
	// constitute the index.
	FieldNames []string `json:"fieldNames"`
}

// String returns a JSON string representation of the IndexInfo.
func (r IndexInfo) String() string {
	return jsonutil.AsJSON(r)
}

// WriteResult represents the result of single row for write operations such as put and delete.
type WriteResult struct {
	// ExistingVersion represents the Version of an existing row.
	ExistingVersion types.Version `json:"existingVersion"`

	// ExistingValue represents the Value of an existing row.
	ExistingValue *types.MapValue `json:"existingValue"`
}

// String returns a JSON string representation of the WriteResult.
func (r WriteResult) String() string {
	return jsonutil.AsJSON(r)
}

// ExistingValueAsJSON returns a JSON string representation of the ExistingValue.
func (r WriteResult) ExistingValueAsJSON() string {
	return jsonutil.AsJSON(r.ExistingValue)
}

// DeleteResult represents the result of a Client.Delete() operation.
//
// If the delete succeeded DeleteResult.Success returns true. Information about
// the existing row on failure may be available using DeleteResult.ExistingValue
// and DeleteResult.ExistingVersion, depending on the use of DeleteRequest.ReturnRow.
type DeleteResult struct {
	Capacity
	DelayInfo

	// WriteResult is used to get the information about the existing row such as
	// ExistingValue and ExistingVersion on operation failure.
	WriteResult

	// Success represents if the delete operation succeeded or not.
	Success bool `json:"success"`
}

// String returns a JSON string representation of the DeleteResult.
func (r DeleteResult) String() string {
	return jsonutil.AsJSON(r)
}

// PutResult represents the result of a Client.Put() operation.
//
// On a successful operation the value returned by PutResult.Version is non-nil.
//
// On failure that value is nil. Information about the existing row on failure
// may be available using PutResult.ExistingValue and PutResult.ExistingVersion,
// depending on the use of PutRequest.ReturnRow and whether the put had set the
// PutIfAbsent or PutIfVersion option.
type PutResult struct {
	Capacity
	DelayInfo

	// WriteResult is used to get the information about the existing row such as
	// ExistingValue and ExistingVersion on operation failure.
	WriteResult

	// Version represents the version of the new row if the operation was
	// successful. If the operation failed nil is returned.
	Version types.Version `json:"version"`

	// GeneratedValue represents the value generated if the operation
	// created a new value for an identity column or string as uuid
	// column. If a value was generated for the column, it is non-nil,
	// otherwise it is nil.
	GeneratedValue types.FieldValue `json:"generatedValue"`
}

// String returns a JSON string representation of the PutResult.
func (r PutResult) String() string {
	return jsonutil.AsJSON(r)
}

// Success returns whether the put operation succeeded.
func (r PutResult) Success() bool {
	return len(r.Version) > 0
}

// TableUsage represents a single usage record, or slice, that includes
// information about read and write throughput consumed during that period as
// well as the current information regarding storage capacity. In addition the
// count of throttling exceptions for the period is reported.
type TableUsage struct {
	// StartTime represents the start time for this usage record.
	StartTime time.Time `json:"startTime"`

	// EndTime represents the end time for this usage record.
	EndTime time.Time `json:"endTime"`

	// ReadUnits represents the number of read uits consumed during this period.
	ReadUnits int `json:"readUnits"`

	// WriteUnits represents the number of write uits consumed during this period.
	WriteUnits int `json:"writeUnits"`

	// StorageGB represents the amount of storage consumed by the table.
	// This information may be out of date as it is not maintained in real time.
	StorageGB int `json:"storageGB"`

	// ReadThrottleCount represents the number of read throttling exceptions on
	// this table in the time period.
	ReadThrottleCount int `json:"readThrottleCount"`

	// WriteThrottleCount represents the number of write throttling exceptions
	// on this table in the time period.
	WriteThrottleCount int `json:"writeThrottleCount"`

	// StorageThrottleCount represents the number of storage throttling
	// exceptions on this table in the time period.
	StorageThrottleCount int `json:"storageThrottleCount"`
}

// String returns a JSON string representation of the TableUsage.
func (r TableUsage) String() string {
	return jsonutil.AsJSON(r)
}

// TableUsageResult represents the result of a Client.GetTableUsage() operation.
//
// It encapsulates the dynamic state of the requested table.
//
// This is used for cloud service only.
type TableUsageResult struct {
	noCapacity
	DelayInfo

	// TableName represents table name used by the operation.
	TableName string `json:"tableName"`

	// UsageRecords represent a slice of usage records based on the parameters
	// of the TableUsageRequest used.
	UsageRecords []TableUsage `json:"usageRecords"`
}

// String returns a JSON string representation of the TableUsageResult.
func (r TableUsageResult) String() string {
	return jsonutil.AsJSON(r)
}

// WriteMultipleResult represents the result of a Client.WriteMultiple() operation.
//
// If the WriteMultiple succeeds, the execution result of each sub operation
// can be retrieved using WriteMultipleResult.ResultSet.
//
// If the WriteMultiple operation is aborted because of the failure of an
// operation with abortOnFail set to true, then the index of failed operation
// can be accessed using WriteMultipleResult.FailedOperationIndex, and the
// execution result of failed operation can be accessed using
// WriteMultipleResult.GetFailedOperationResult().
type WriteMultipleResult struct {
	Capacity
	DelayInfo

	// ResultSet represents the list of execution results for the operations.
	ResultSet []OperationResult `json:"resultSet"`

	// FailedOperationIndex represents the index of failed operation that
	// results in the entire WriteMultiple operation aborting.
	FailedOperationIndex int `json:"failedOperationIndex"`
}

// String returns a JSON string representation of the WriteMultipleResult.
func (r WriteMultipleResult) String() string {
	return jsonutil.AsJSON(r)
}

// IsSuccess checks whether the operation succeeded or not. It returns true if
// the WriteMultiple operation succeeded, or false if the operation is aborted
// due to the failure of a sub operation.
func (r WriteMultipleResult) IsSuccess() bool {
	return r.FailedOperationIndex == -1
}

// GetFailedOperationResult returns the result of the operation that results in
// the entire WriteMultiple operation aborting.
func (r WriteMultipleResult) GetFailedOperationResult() *OperationResult {
	if r.FailedOperationIndex == -1 || len(r.ResultSet) == 0 {
		return nil
	}

	return &r.ResultSet[0]
}

// OperationResult represents the result associated with the execution of an
// individual operation in the request.
type OperationResult struct {
	// WriteResult is used to get the information about the existing row such as
	// ExistingValue and ExistingVersion on operation failure.
	WriteResult

	// Success represents whether the operation succeeded.
	// A put or delete operation may fail if the condition is not matched.
	Success bool `json:"success"`

	// Version represents the version of the new row for put operation, or nil
	// if the put operation did not succeed or the operation is a delete operation.
	Version types.Version `json:"version"`

	// GeneratedValue represents the value generated if the operation
	// created a new value for an identity column or string as uuid
	// column. If a value was generated for the column, it is non-nil,
	// otherwise it is nil.
	//
	// This value is only valid for a put operation on a table with an identity
	// column.
	GeneratedValue types.FieldValue `json:"generatedValue"`
}

// String returns a JSON string representation of the OperationResult.
func (r OperationResult) String() string {
	return jsonutil.AsJSON(r)
}

// MultiDeleteResult represents the result of a Client.MultiDelete() operation.
//
// On a successful operation the number of rows deleted is available in
// MultiDeleteResult.NumDeleted.
//
// There is a limit on the amount of data consumed by a single call. If there
// are still more rows to delete, the continuation key can be obtained using
// MultiDeleteResult.ContinuationKey.
type MultiDeleteResult struct {
	Capacity
	DelayInfo

	// ContinuationKey represents the continuation key where the next
	// MultiDelete request resumes from.
	ContinuationKey []byte `json:"continuationKey"`

	// NumDeleted represents the number of rows deleted from the table.
	NumDeleted int `json:"numDeleted"`
}

// String returns a JSON string representation of the MultiDeleteResult.
func (r MultiDeleteResult) String() string {
	return jsonutil.AsJSON(r)
}

// PrepareResult represents the result of a Client.Prepare() operation.
//
// The returned PreparedStatement can be re-used for query execution using
// QueryRequest.PreparedStatement.
type PrepareResult struct {
	Capacity
	DelayInfo

	// PreparedStatement represents the value of the prepared statement.
	PreparedStatement PreparedStatement `json:"preparedStatement"`
}

// String returns a JSON string representation of the PrepareResult.
func (r PrepareResult) String() string {
	return jsonutil.AsJSON(r)
}

// QueryResult represents the result of a Client.Query() operation.
//
// It comprises a list of MapValue instances representing the query results.
// The shape of the values is based on the schema implied by the query. For
// example a query such as "SELECT * FROM ..." that returns an intact row will
// return values that conform to the schema of the table. Projections return
// instances that conform to the schema implied by the statement. UPDATE
// queries either return values based on a RETURNING clause or, by default,
// the number of rows affected by the statement.
//
// It is possible for a query to return no results in an empty list.
// This happens if the query reads the maximum amount of data allowed in a
// single request without matching a query predicate.
//
// Applications need to check QueryRequest.IsDone() and continue to get more
// results if the query request is not completed.
type QueryResult struct {
	Capacity
	DelayInfo

	// The query request with which this query result is associated.
	request *QueryRequest

	// results represents a slice of MapValues for the query results.
	// It is possible to have an empty results and a non-nil continuation key.
	results []*types.MapValue

	// continuationKey represents the continuation key that can be used to
	// obtain more results if non-nil.
	continuationKey []byte

	// The following 6 fields are used only for "internal" QueryResults, i.e.,
	// those received and processed by the receiveIter.

	// reachedLimit indicates whether the query has reached the size limit or number limit.
	reachedLimit bool

	// isComputed indicates whether the query result has been computed for
	// current query batch.
	isComputed bool

	// The following 4 fields are used during phase 1 of a sorting ALL_PARTITIONS query.
	//
	// In this case, the "results" may store query results from multiple partitions.
	// If so, the results are grouped by partition and the partitionIDs,
	// numResultsPerPart, and contKeys fields store the partition id,
	// the number of results, and the continuation key per partition.
	// The isInPhase1 specifies whether phase 1 is done.
	isInPhase1        bool
	partitionIDs      []int
	numResultsPerPart []int
	contKeysPerPart   [][]byte
}

func newQueryResult(req *QueryRequest, isComputed bool) *QueryResult {
	return &QueryResult{
		request:    req,
		isComputed: isComputed,
	}
}

func (r *QueryResult) compute() (err error) {
	if r.isComputed {
		return
	}

	if err = r.request.driver.compute(r); err != nil {
		return
	}

	r.isComputed = true
	return
}

// GetResults returns query results as a slice of *types.MapValue.
//
// It is possible to return an empty result even though the query is not finished.
func (r *QueryResult) GetResults() (res []*types.MapValue, err error) {
	err = r.compute()
	if err != nil {
		return
	}
	return r.results, nil
}

func (r *QueryResult) getContinuationKey() ([]byte, error) {
	err := r.compute()
	if err != nil {
		return nil, err
	}
	return r.continuationKey, nil
}

// ConsumedCapacity returns the consumed capacity by the query request.
//
// This implements the Result interface.
func (r *QueryResult) ConsumedCapacity() (Capacity, error) {
	err := r.compute()
	if err != nil {
		return Capacity{}, err
	}

	return Capacity{
		ReadKB:    r.ReadKB,
		WriteKB:   r.WriteKB,
		ReadUnits: r.ReadUnits,
	}, nil
}

// String returns a JSON string representation of the QueryResult.
func (r QueryResult) String() string {
	return jsonutil.AsJSON(r)
}

func validateWaitTimeout(timeout, pollInterval time.Duration) error {
	if pollInterval < time.Millisecond {
		return nosqlerr.NewIllegalArgument("the specified poll interval %v is less than the allowed minimum of %v",
			pollInterval, time.Millisecond)
	}

	if timeout <= pollInterval {
		return nosqlerr.NewIllegalArgument("the specified timeout must be greater than the poll interval %v, got %v",
			pollInterval, timeout)
	}

	return nil
}

func shouldRetryAfter(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done(): // Timeout elapsed or context was canceled.
		return false
	}
}
