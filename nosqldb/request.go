//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// GetRequest represents a request for retrieving a row from a table.
//
// It is used as the input to a Client.Get() operation which returns a single
// row based on the specified key.
type GetRequest struct {

	// TableName specifies the name of table from which to get the row.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// Key specifies the primary key used for the get operation.
	// It is required and must be non-nil.
	Key *types.MapValue `json:"key"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Consistency specifies desired consistency policy for the request.
	// It is optional.
	// If set, it must be either types.Absolute or types.Eventual, otherwise
	// an IllegalArgument error will be returned.
	// If not set, the default consistency value configured for Client is used,
	// which is determined by RequestConfig.DefaultConsistency().
	Consistency types.Consistency `json:"consistency"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *GetRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if err = validateKey(r.Key); err != nil {
		return
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	if err = validateConsistency(r.Consistency); err != nil {
		return
	}

	return
}

// setDefaults sets default timeout and consistency values specified in
// RequestConfig if they are not specified for the request.
func (r *GetRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Consistency == 0 {
		r.Consistency = cfg.DefaultConsistency()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *GetRequest) shouldRetry() bool {
	return true
}

func (r *GetRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *GetRequest) getTableName() string {
	return r.TableName
}

func (r *GetRequest) getNamespace() string {
	return r.Namespace
}

func (r *GetRequest) doesReads() bool {
	return true
}

func (r *GetRequest) doesWrites() bool {
	return false
}

// GetTableRequest represents a request for retrieving table information from server.
//
// It is used as the input of a Client.GetTable() operation which returns static
// information associated with a table, as returned in TableResult. This
// information only changes in response to a change in table schema or a change
// in provisioned throughput or capacity for the table.
type GetTableRequest struct {
	// TableName specifies the name of table.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// OperationID specifies the operation id to use for the request.
	// The operation id can be obtained via TableResult.OperationID.
	//
	// It is optional.
	// If set to a non-empty string, it represents an asynchronous table
	// operation that may be in progress. It is used to examine the result of
	// the operation and if the operation has failed, an error will be returned
	// in response to a Client.GetTable() operation. If the operation is in
	// progress or has completed successfully, the state of the table is
	// returned.
	OperationID string `json:"operationID,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *GetTableRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	return
}

func (r *GetTableRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *GetTableRequest) shouldRetry() bool {
	return false
}

func (r *GetTableRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *GetTableRequest) getTableName() string {
	return r.TableName
}

func (r *GetTableRequest) getNamespace() string {
	return r.Namespace
}

func (r *GetTableRequest) doesReads() bool {
	return false
}

func (r *GetTableRequest) doesWrites() bool {
	return false
}

// GetIndexesRequest represents a request for retrieving indexes information.
//
// It is used as the input to a Client.GetIndexes() operation which returns the
// information of a specific index or all indexes of the specified table, as
// returned in GetIndexesResult.
type GetIndexesRequest struct {
	// TableName specifies the name of table to which the indexes belong.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// IndexName specifies the name of index.
	//
	// It is optional.
	// If set, the request is intended for retrieving information of the
	// specific index.
	// If not set, the request is intended for retrieving information of
	// all indexes on the table.
	IndexName string `json:"indexName,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *GetIndexesRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	return
}

func (r *GetIndexesRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *GetIndexesRequest) shouldRetry() bool {
	return false
}

func (r *GetIndexesRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *GetIndexesRequest) getTableName() string {
	return r.TableName
}

func (r *GetIndexesRequest) getNamespace() string {
	return r.Namespace
}

func (r *GetIndexesRequest) doesReads() bool {
	return false
}

func (r *GetIndexesRequest) doesWrites() bool {
	return false
}

// ListTablesRequest represents a request to list all available tables.
//
// It is used as the input to a Client.ListTables() operation, which lists all
// available tables associated with the identity. If the list is large, it can
// be paged by using the StartIndex and Limit parameters. The list is returned
// in an array in ListTablesResult. Table names are returned sorted in
// alphabetical order in order to facilitate paging.
type ListTablesRequest struct {
	// StartIndex specifies the index to use to start returning table names.
	// This is related to the ListTablesResult.LastIndexReturned from a previous
	// request and can be used to page table names.
	//
	// It is optional.
	// If not set, the list starts at index 0.
	StartIndex uint `json:"startIndex,omitempty"`

	// Limit specifies the maximum number of tables to return in the operation.
	//
	// It is optional.
	// If not set, there is no limit.
	Limit uint `json:"limit,omitempty"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	common.InternalRequestData
}

func (r *ListTablesRequest) validate() error {
	return validateTimeout(r.Timeout)
}

func (r *ListTablesRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *ListTablesRequest) shouldRetry() bool {
	return false
}

func (r *ListTablesRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *ListTablesRequest) getTableName() string {
	return ""
}

func (r *ListTablesRequest) getNamespace() string {
	return r.Namespace
}

func (r *ListTablesRequest) doesReads() bool {
	return false
}

func (r *ListTablesRequest) doesWrites() bool {
	return false
}

// AddReplicaRequest is a request used to add a remote replica in another region
// to a local table.
//
// Cloud service only.
//
// Added in SDK Version 1.4.4
type AddReplicaRequest struct {
	// TableName specifies the name of an existing table.
	// It is required for this request.
	TableName string `json:"tableName"`

	// ReplicaName is the name of the region to add the replicated table in.
	// It is required for this request.
	ReplicaName string `json:"replicaName"`

	// MatchETag defines an ETag in the request that must be matched for the operation
	// to proceed. The ETag must be non-null and have been returned in a
	// previous TableResult. This is a form of optimistic concurrency
	// control allowing an application to ensure no unexpected modifications
	// have been made to the table.
	MatchETag string `json:"matchETag"`

	// Sets the read units for the replica table. This defaults
	// to the units on the existing local table
	ReadUnits int32 `json:"readUnits"`

	// Sets the write units for the replica table. This defaults
	// to the units on the existing local table
	WriteUnits int32 `json:"writeUnits"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	common.InternalRequestData
}

func (r *AddReplicaRequest) validate() error {
	return validateTimeout(r.Timeout)
}

func (r *AddReplicaRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}
}

func (r *AddReplicaRequest) shouldRetry() bool {
	return false
}

func (r *AddReplicaRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *AddReplicaRequest) getTableName() string {
	return r.TableName
}

func (r *AddReplicaRequest) getNamespace() string {
	return ""
}

func (r *AddReplicaRequest) doesReads() bool {
	return false
}

func (r *AddReplicaRequest) doesWrites() bool {
	return false
}

// DropReplicaRequest is a request used to remove a remote replica from another region.
//
// Cloud service only.
//
// Added in SDK Version 1.4.4
type DropReplicaRequest struct {
	// TableName specifies the name of an existing table.
	// It is required for this request.
	TableName string `json:"tableName"`

	// ReplicaName is the name of the region to drop the replicated table from.
	// It is required for this request.
	ReplicaName string `json:"replicaName"`

	// MatchETag defines an ETag in the request that must be matched for the operation
	// to proceed. The ETag must be non-null and have been returned in a
	// previous TableResult. This is a form of optimistic concurrency
	// control allowing an application to ensure no unexpected modifications
	// have been made to the table.
	MatchETag string `json:"matchETag"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	common.InternalRequestData
}

func (r *DropReplicaRequest) validate() error {
	return validateTimeout(r.Timeout)
}

func (r *DropReplicaRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}
}

func (r *DropReplicaRequest) shouldRetry() bool {
	return false
}

func (r *DropReplicaRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *DropReplicaRequest) getTableName() string {
	return r.TableName
}

func (r *DropReplicaRequest) getNamespace() string {
	return ""
}

func (r *DropReplicaRequest) doesReads() bool {
	return false
}

func (r *DropReplicaRequest) doesWrites() bool {
	return false
}

// ReplicaStatsRequest is a request used to remove a remote replica from another region.
//
// Cloud service only.
//
// ReplicaStatsRequest represents an operation
// which returns stats information for one, or all replicas of a replicated
// table, returned in ReplicaStatsResult. This information includes a
// time series of replica stats, as found ReplicaStats.
//
// It is possible to return a range of stats records or, by default, only the
// most recent stats records if startTime is not specified. Replica stats
// records are created on a regular basis and maintained for a period of time.
// Only records for time periods that have completed are returned so that a user
// never sees changing data for a specific range.
//
// Added in SDK Version 1.4.4
type ReplicaStatsRequest struct {
	// TableName specifies the name of an existing table.
	// It is required for this request.
	TableName string `json:"tableName"`

	// ReplicaName is the name of the region to query for stats. If this is left
	// empty, stats for all replicated regions will be returned.
	ReplicaName string `json:"replicaName"`

	// StartTime start time to use for the request in milliseconds since the
	// Epoch in UTC time. If no start time is set for this request the most
	// recent complete stats records are returned, the number of records is
	// up to the limit defined below.
	StartTime *time.Time

	// Limit defines the limit to the number of replica stats records desired. The
	// default value is 1000.
	Limit int32 `json:"limit"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	common.InternalRequestData
}

// SetStartTime is a convenience functon to set the start time from an ISO-formatted
// datetime string.
func (r *ReplicaStatsRequest) SetStartTime(startTime string) error {
	t, err := types.ParseDateTime(startTime)
	if err != nil {
		return err
	}
	r.StartTime = &t
	return nil
}

func (r *ReplicaStatsRequest) validate() error {
	return validateTimeout(r.Timeout)
}

func (r *ReplicaStatsRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}
}

func (r *ReplicaStatsRequest) shouldRetry() bool {
	return false
}

func (r *ReplicaStatsRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *ReplicaStatsRequest) getTableName() string {
	return r.TableName
}

func (r *ReplicaStatsRequest) getNamespace() string {
	return ""
}

func (r *ReplicaStatsRequest) doesReads() bool {
	return false
}

func (r *ReplicaStatsRequest) doesWrites() bool {
	return false
}

// SystemRequest represents a request used to perform any table-independent
// administrative operations such as create/drop of namespaces and
// security-relevant operations such as create/drop users and roles.
//
// Execution of operations specified by this request is implicitly asynchronous.
// These are potentially long-running operations and completion of the
// operation needs to be checked.
//
// SystemRequest is used as the input of a Client.DoSystemRequest() operation,
// which returns a SystemResult that can be used to poll until the operation
// succeeds or fails by calling the SystemResult.WaitForCompletion() method.
//
// This request is used for on-premise only.
type SystemRequest struct {
	// Statement specifies the statement used for the operation.
	// It is required.
	Statement string `json:"statement"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultTableRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	common.InternalRequestData
}

func (r *SystemRequest) validate() (err error) {
	if r.Statement == "" {
		return nosqlerr.NewIllegalArgument("Statement must be non-empty")
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	return
}

func (r *SystemRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultTableRequestTimeout()
	}
}

func (r *SystemRequest) shouldRetry() bool {
	return false
}

func (r *SystemRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *SystemRequest) getTableName() string {
	return ""
}

func (r *SystemRequest) getNamespace() string {
	return ""
}

func (r *SystemRequest) doesReads() bool {
	return false
}

func (r *SystemRequest) doesWrites() bool {
	return false
}

// SystemStatusRequest represents a request used to check the status of an
// operation started using a SystemRequest.
//
// It is used as the input to a Client.GetSystemStatus() operation which returns
// current status of the SystemRequest operation.
//
// This request is used for on-premise only.
type SystemStatusRequest struct {
	// Statement specifies the statement used for the operation.
	// It is optional.
	Statement string `json:"statement"`

	// OperationID specifies the operation id to use for the request. The
	// operation id can be obtained via SystemResult.OperationID.
	//
	// It is required and represents an asynchronous operation that may be in
	// progress. It is used to examine the result of the operation and if the
	// operation has failed an error will be returned in response to a
	// Client.GetSystemStatus() operation. If the operation is in progress or
	// has completed successfully, the state of the operation is returned.
	OperationID string `json:"operationID,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultTableRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	common.InternalRequestData
}

func (r *SystemStatusRequest) validate() (err error) {
	if r.OperationID == "" {
		return nosqlerr.NewIllegalArgument("OperationID must be non-empty")
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	return
}

func (r *SystemStatusRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultTableRequestTimeout()
	}
}

func (r *SystemStatusRequest) shouldRetry() bool {
	return true
}

func (r *SystemStatusRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *SystemStatusRequest) getTableName() string {
	return ""
}

func (r *SystemStatusRequest) getNamespace() string {
	return ""
}

func (r *SystemStatusRequest) doesReads() bool {
	return false
}

func (r *SystemStatusRequest) doesWrites() bool {
	return false
}

// TableRequest represents a request used to manage table schema and limits.
//
// The following operations are supported by TableRequest:
//
//	create tables
//	drop tables
//	modify tables: add or remove columns
//	create indexes
//	drop indexes
//	change table limits of an existing table
//
// Operation that is used to create a table must specify a Statement to define
// the table schema and a TableLimits to define the throughput, storage,
// and mode (provisioned or on demand) desired for the table.
//
// Operations that are used to drop, modify a table, or create, drop an index
// must specify a Statement.
//
// These operations that are used to manage table schema do not need to specify
// the TableName explicitly as the table name is inferred from the specified
// Statement. An IllegalArgument error will be returnerd if both the Statement
// and TableName are specified in a TableRequest.
//
// Operation that is used to change limits of an existing table must specify
// the TableName and a TableLimits to define the throughput and storage desired
// for the table.
//
// This request is also used to modify tags associated with an existing table.
// This use is mutually exclusive with respect to changing a table schema or
// its limits. To modify tags, specify only tags and the table name without
// a statement.
//
// Execution of operations specified by TableRequest is implicitly asynchronous.
// These are potentially long-running operations.
// This request is used as the input of a Client.DoTableRequest() operation,
// which returns a TableResult that can be used to poll until the table reaches
// the desired state.
type TableRequest struct {
	// Statement specifies the statement for table relevant operations.
	// It is required for operations that manage table schema.
	Statement string `json:"statement"`

	// TableName specifies the name of an existing table.
	//
	// It is required for operations that change table limits of an existing table.
	// It should not be specified for other operations.
	TableName string `json:"tableName,omitempty"`

	// TableLimits specifies desired read/write throughput, storage limits, and
	// mode (provisioned or on demand) for the table.
	//
	// It is required for operations that create tables or change table limits.
	// It should not be specified for other operations.
	//
	// TableLimits is used for cloud service only.
	// It will be ignored if used for on-premise.
	TableLimits *TableLimits `json:"tableLimits,omitempty"`

	// FreeFormTags define the free-form tags to use for the operation.
	// FreeFormTags are used in only 2 cases: table creation statements and tag
	// modification operations.
	// They are not used for other DDL operations.
	// If tags are set for an on-premise service they are silently ignored.
	// Added in SDK version 1.4.0
	FreeFormTags *types.FreeFormTags

	// DefinedTags define the tags to use for the operation.
	// DefinedTags are used in only 2 cases: table creation statements and tag
	// modification operations.
	// They are not used for other DDL operations.
	// If tags are set for an on-premise service they are silently ignored.
	// Added in SDK version 1.4.0
	DefinedTags *types.DefinedTags

	// MatchETag defines an ETag in the request that must be matched for the operation
	// to proceed. The ETag must be non-null and have been returned in a
	// previous TableResult. This is a form of optimistic concurrency
	// control allowing an application to ensure no unexpected modifications
	// have been made to the table.
	// If set for an on-premise service the ETag is silently ignored.
	// Added in SDK version 1.4.0
	MatchETag string

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultTableRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *TableRequest) validate() (err error) {
	if r.Statement == "" && r.TableName == "" {
		return nosqlerr.NewIllegalArgument("TableRequest: either Statement or TableName should be specified")
	}

	if r.Statement != "" && r.TableName != "" {
		return nosqlerr.NewIllegalArgument("TableRequest: cannot specify both Statement and TableName")
	}

	// Validate table limits value if it is specified.
	if r.TableLimits != nil {
		if err = r.TableLimits.validate(); err != nil {
			return
		}
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	return
}

func (r *TableRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultTableRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *TableRequest) shouldRetry() bool {
	return false
}

func (r *TableRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *TableRequest) getTableName() string {
	return r.TableName
}

func (r *TableRequest) getNamespace() string {
	return r.Namespace
}

func (r *TableRequest) doesReads() bool {
	return false
}

func (r *TableRequest) doesWrites() bool {
	return false
}

// TableLimits is used during table creation to specify the throughput and
// capacity to be consumed by the table. It is also used in an operation to
// change the limits of an existing table.
// Client.DoTableRequest() method and TableRequest are used to perform these
// operations. The specified throughput and capacity values are enforced by the
// system and used for billing purposes.
//
// Throughput limits are defined in terms of read units and write units.
//
// A read unit represents 1 eventually consistent read per second for data up to
// 1 KB in size. A read that is absolutely consistent is double that, consuming
// 2 read units for a read of up to 1 KB in size. This means that if an
// application is to use types.Absolute consistency, it may need to specify
// additional read units when creating a table.
//
// A write unit represents 1 write per second of data up to 1 KB in size.
//
// In addition to throughput table capacity must be specified to indicate the
// maximum amount of storage, in gigabytes, allowed for the table.
//
// All 3 values must be used whenever using this struct. There are no defaults
// and no mechanism to indicate "no change".
//
// TableLimits is used for cloud service only.
type TableLimits struct {
	// ReadUnits specifies the desired throughput of read operation in terms of
	// read units.
	ReadUnits uint `json:"readUnits"`

	// WriteUnits specifies the desired throughput of write operation in terms
	// of write units.
	WriteUnits uint `json:"writeUnits"`

	// StorageGB specifies the maximum amount of storage to be consumed by the
	// table, in gigabytes.
	StorageGB uint `json:"storageGB"`

	// CapacityMode specifies if the table is provisioned (the default) or
	// on demand.
	CapacityMode types.CapacityMode `json:"limitsMode"`
}

// ProvisionedTableLimits returns a TableLimits struct set up for
// provisioned (fixed maximum read/write limits) tables. This is the default.
func ProvisionedTableLimits(RUs uint, WUs uint, GB uint) *TableLimits {
	return &TableLimits{
		ReadUnits:    RUs,
		WriteUnits:   WUs,
		StorageGB:    GB,
		CapacityMode: types.Provisioned,
	}
}

// OnDemandTableLimits returns a TableLimits struct set up for
// on demand (flexible read/write limits) tables.
// Added in SDK Version 1.3.0
func OnDemandTableLimits(GB uint) *TableLimits {
	return &TableLimits{
		ReadUnits:    0,
		WriteUnits:   0,
		StorageGB:    GB,
		CapacityMode: types.OnDemand,
	}
}

func (l *TableLimits) validate() (err error) {
	if l.CapacityMode != types.Provisioned && l.CapacityMode != types.OnDemand {
		return nosqlerr.NewIllegalArgument("TableLimits CapacityMode must be one of " +
			"Provisioned or OnDemand")
	}
	if l.StorageGB == 0 {
		return nosqlerr.NewIllegalArgument("TableLimits StorageGB must be positive")
	}
	if l.CapacityMode == types.Provisioned {
		if l.ReadUnits == 0 || l.WriteUnits == 0 {
			return nosqlerr.NewIllegalArgument("TableLimits read/write units must be positive")
		}
	} else {
		if l.ReadUnits != 0 || l.WriteUnits != 0 {
			return nosqlerr.NewIllegalArgument("TableLimits read/write units must be zero for OnDemand table")
		}
	}
	return nil
}

// DeleteRequest represents a request for deleting a row from a table.
//
// This request can be used to perform unconditional and conditional deletes:
//
// 1. Delete any existing row. This is the default.
//
// 2. Delete only if the row exists and its version matches a specific version.
//
// For the latter case, a MatchVersion for the request must be specified.
// Using this option in conjunction with specifying the ReturnRow allows
// information about the existing row to be returned if the operation fails
// because of a version mismatch. On success no information is returned.
// Specifying the ReturnRow may incur additional cost and affect operation
// latency.
//
// This request is used as the input to a Client.Delete() operation, which
// returns a DeleteResult. If the operation succeeds, DeleteResult.Success
// returns true. Additional information such as previous row information, may be
// available in DeleteResult.
type DeleteRequest struct {
	// TableName specifies the name of table from which to delete the row.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// Key specifies the primary key used for the delete operation.
	// It is required and must be non-nil.
	Key *types.MapValue `json:"key"`

	// ReturnRow specifies whether information about the existing row should be
	// returned on failure because of a version mismatch.
	// If a match version has not been specified via MatchVersion, this
	// parameter is ignored and there will be no return information.
	//
	// It is optional and defaults to false.
	//
	// Using this option may incur additional cost.
	ReturnRow bool `json:"returnRow"`

	// MatchVersion specifies the version to use for a conditional delete
	// operation. The version is usually obtained from GetResult.Version or
	// other method that returns a version.
	//
	// It is optional.
	// If set, the delete operation will succeed only if the row exists and its
	// version matches the one specified.
	//
	// Using this option will incur additional cost.
	MatchVersion types.Version `json:"matchVersion,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Durability is currently only used in On-Prem installations.
	// Added in SDK Version 1.3.0
	Durability types.Durability `json:"durability"`

	// isSubRequest specifies whether this is a sub request of a WriteMultiple
	// operation.
	// It is for internal use only.
	isSubRequest bool

	// abortOnFail specifies whether a failure of this operation during a WriteMultiple
	// operation should cause the whole operation to fail.
	// It is copied from the parent WriteOperation, and is for internal use only.
	abortOnFail bool

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *DeleteRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if err = validateKey(r.Key); err != nil {
		return
	}

	if !r.isSubRequest {
		if err = validateTimeout(r.Timeout); err != nil {
			return
		}
	}

	return
}

func (r *DeleteRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *DeleteRequest) shouldRetry() bool {
	return true
}

func (r *DeleteRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *DeleteRequest) getTableName() string {
	return r.TableName
}

func (r *DeleteRequest) getNamespace() string {
	return r.Namespace
}

func (r *DeleteRequest) doesReads() bool {
	return true
}

func (r *DeleteRequest) doesWrites() bool {
	return true
}

// PutRequest represents a request used to put a row into a table.
//
// This request can be used to perform unconditional and conditional puts:
//
// 1. Overwrite any existing row. This is the default.
//
// 2. Succeed only if the row does not exist. Specify types.PutIfAbsent for the
// PutOption parameter for this case.
//
// 3. Succeed only if the row exists. Specify types.PutIfPresent for the
// PutOption parameter for this case.
//
// 4. Succeed only if the row exists and its version matches a specific version.
// Specify types.PutIfVersion for the PutOption parameter and a desired version
// for the MatchVersion parameter for this case.
//
// Information about the existing row can be returned on failure of a put
// operation using types.PutIfAbsent or types.PutIfVersion by using the
// ReturnRow option. Use of the ReturnRow option incurs additional cost and
// may affect operation latency.
//
// This request is used as the input to a Client.Put() operation, which returns
// a PutResult. On a successful operation the returned PutResult.Version is
// non-nil. Additional information, such as previous row information, may be
// available in PutResult.
type PutRequest struct {
	// TableName specifies the name of table which the row to put into.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// Value specifies the value of the row to put.
	// It is required and must be non-nil.
	Value *types.MapValue `json:"value"`

	// Use StructValue to use a native struct as a record value.
	StructValue any

	// PutOption specifies the put option for the operation.
	//
	// It is optional and performs an unconditional put by default.
	// If set, it must be types.PutIfAbsent, types.PutIfPresent or types.PutIfVersion.
	PutOption types.PutOption `json:"putOption"`

	// ReturnRow specifies whether information about the existing row should be
	// returned on failure because of a version mismatch or failure of a
	// PutIfAbsent operation.
	ReturnRow bool `json:"returnRow"`

	// TTL specifies the time to live (TTL) value, causing the time to live on
	// the row to be set to the specified value on put.
	// It is optional.
	TTL *types.TimeToLive `json:"ttl"`

	// UseTableTTL specifies whether to use the table's default TTL for the row.
	// If true, and there is an existing row, causes the operation to update
	// the time to live (TTL) value of the row based on the table's default
	// TTL if set. If the table has no default TTL this setting has no effect.
	// By default updating an existing row has no effect on its TTL.
	UseTableTTL bool `json:"useTableTTL"`

	// MatchVersion specifies the desired version to use for a conditional put
	// operation that uses the PutIfVersion option.
	// The Version is usually obtained from GetResult.Version or other method
	// that returns a Version. When set, the put operation will succeed only if
	// the row exists and its Version matches the one specified.
	// This condition exists to allow an application to ensure that it is
	// updating a row in an atomic read-modify-write cycle.
	//
	// Using this mechanism incurs additional cost.
	MatchVersion types.Version `json:"matchVersion,omitempty"`

	// ExactMatch specifies whether the provided Value must be an exact match
	// for the table schema. An exact match means that there are no required
	// fields missing and that there are no extra, unknown fields.
	// If set to true, and the provided Value does not match the table schema
	// exactly, the operation will fail.
	// The default behavior is to not require an exact match.
	ExactMatch bool `json:"exactMatch"`

	// IdentityCacheSize specifies the number of generated identity values that
	// are requested from the server during a put.
	// This takes precedence over the DDL identity CACHE option that was set
	// during creation of the identity column.
	// If this is set to a value that is less than or equal to 0, the DDL identity
	// CACHE value is used.
	IdentityCacheSize int `json:"identityCacheSize"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Durability is currently only used in On-Prem installations.
	// Added in SDK Version 1.3.0
	Durability types.Durability `json:"durability"`

	// isSubRequest specifies whether this is a sub request of a WriteMultiple
	// operation.
	// It is for internal use only.
	isSubRequest bool

	// abortOnFail specifies whether a failure of this operation during a WriteMultiple
	// operation should cause the whole operation to fail.
	// It is copied from the parent WriteOperation, and is for internal use only.
	abortOnFail bool

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *PutRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if !r.isSubRequest {
		if err = validateTimeout(r.Timeout); err != nil {
			return
		}
	}

	if r.PutOption == types.PutIfVersion && r.MatchVersion == nil {
		return nosqlerr.NewIllegalArgument("PutRequest: must specify a MatchVersion for the PutIfVersion operation")
	}

	if r.PutOption != types.PutIfVersion && r.MatchVersion != nil {
		return nosqlerr.NewIllegalArgument("PutRequest: MatchVersion cannot be specified for put options other than PutIfVersion")
	}

	if r.UseTableTTL && r.TTL != nil {
		return nosqlerr.NewIllegalArgument("PutRequest: UseTableTTL and TTL are mutual exclusive, cannot specify both of them")
	}

	return nil
}

func (r *PutRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *PutRequest) shouldRetry() bool {
	return true
}

func (r *PutRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *PutRequest) getTableName() string {
	return r.TableName
}

func (r *PutRequest) getNamespace() string {
	return r.Namespace
}

func (r *PutRequest) doesReads() bool {
	return r.PutOption != 0
}

func (r *PutRequest) doesWrites() bool {
	return true
}

// updateTTL indicates whether the put operation should update TTL of the specified row.
// It returns true if the operation specifies the UseTableTTL option or sets a TTL value.
func (r *PutRequest) updateTTL() bool {
	return r.UseTableTTL || r.TTL != nil
}

// TableUsageRequest represents the input of a Client.GetTableUsage() operation
// which returns dynamic information associated with a table, as returned in
// TableUsageResult. This information includes a time series of usage snapshots,
// each indicating data such as read and write throughput, throttling events,
// etc, as found in TableUsageResult.UsageRecords.
//
// It is possible to return a range of usage records or, by default, only the
// most recent usage record. Usage records are created on a regular basis and
// maintained for a period of time. Only records for time periods that have
// completed are returned so that a user never sees changing data for a specific
// range.
//
// This request is used for cloud service only.
type TableUsageRequest struct {
	// TableName specifies the name of table for the request.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// StartTime specifies the start time to use for the request.
	// It is optional.
	// If no time range is set for this request the most recent complete usage
	// record is returned.
	StartTime time.Time `json:"startTime,omitempty"`

	// EndTime specifies the end time to use for the request.
	// It is optional.
	// If no time range is set for this request the most recent complete usage
	// record is returned.
	EndTime time.Time `json:"endTime,omitempty"`

	// Limit specifies the limit on the number of usage records desired.
	// It is optional.
	// If not set, or set to 0 there is no limit, but not all usage records may
	// be returned in a single request due to size limitations.
	Limit uint `json:"limit,omitempty"`

	// StartIndex sets the index to use to start returning usage records.
	// This is related to the TableUsageResult.LastReturnedIndex from a previous
	// request and can be used to page usage records.
	// If not set, the list starts at index 0.
	// Added in SDK version 1.4.0
	StartIndex int `json:"startIndex,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *TableUsageRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	if !r.StartTime.IsZero() && !r.EndTime.IsZero() && r.StartTime.After(r.EndTime) {
		return nosqlerr.NewIllegalArgument("TableUsageRequest: EndTime must be after StartTime")
	}

	return
}

func (r *TableUsageRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *TableUsageRequest) shouldRetry() bool {
	return false
}

func (r *TableUsageRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *TableUsageRequest) getTableName() string {
	return r.TableName
}

func (r *TableUsageRequest) getNamespace() string {
	return r.Namespace
}

func (r *TableUsageRequest) doesReads() bool {
	return false
}

func (r *TableUsageRequest) doesWrites() bool {
	return false
}

// PrepareRequest encapsulates a query prepare call. Query preparation allows
// queries to be compiled (prepared) and reused, saving time and resources.
// Use of prepared queries vs. direct execution of query strings is highly
// recommended.
//
// Prepared queries are implemented as PreparedStatement which supports bind
// variables in queries which can be used to more easily reuse a query by
// parameterization.
type PrepareRequest struct {
	// Statement specifies a query statement.
	// It is required and must be non-empty.
	Statement string `json:"statement"`

	// GetQueryPlan specifies whether to include the query execution plan in
	// the PrepareResult returned for this request.
	GetQueryPlan bool `json:"getQueryPlan"`

	// GetQuerySchema specifies whether the JSON value of the query result schema
	// for the query should be included in the PrepareResult returned for this request.
	// Added in SDK Version 1.4.0
	GetQuerySchema bool `json:"getQuerySchema"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name in the SQL query string
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *PrepareRequest) validate() (err error) {
	if r.Statement == "" {
		return nosqlerr.NewIllegalArgument("PrepareRequest: Statement must be non-empty")
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	return
}

func (r *PrepareRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *PrepareRequest) shouldRetry() bool {
	return true
}

func (r *PrepareRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *PrepareRequest) getTableName() string {
	// TODO
	return ""
}

func (r *PrepareRequest) getNamespace() string {
	return r.Namespace
}

func (r *PrepareRequest) doesReads() bool {
	return false
}

func (r *PrepareRequest) doesWrites() bool {
	return false
}

// FPArithSpec specifies the desired representation in terms of decimal
// precision (number of digits) and rounding rules for the floating-point values
// as arithematic operation results.
type FPArithSpec struct {
	// Precision specifies the desired decimal precision (in number of digits)
	// for arithematic operation result of floating-point value.
	Precision uint

	// RoundingMode determines how an arithematic operation result of
	// floating-point value is rounded to the desired precision.
	RoundingMode big.RoundingMode
}

var (
	// Decimal32 represents the IEEE 754R Decimal32 format, that is 7 digits,
	// and a rounding mode of big.ToNearestEven.
	Decimal32 = FPArithSpec{
		Precision:    7,
		RoundingMode: big.ToNearestEven,
	}

	// Decimal64 represents the IEEE 754R Decimal64 format, that is 16 digits,
	// and a rounding mode of big.ToNearestEven.
	Decimal64 = FPArithSpec{
		Precision:    16,
		RoundingMode: big.ToNearestEven,
	}

	// Decimal128 represents the IEEE 754R Decimal128 format, that is 34 digits,
	// and a rounding mode of big.ToNearestEven.
	Decimal128 = FPArithSpec{
		Precision:    34,
		RoundingMode: big.ToNearestEven,
	}

	// Unlimited represents a setting of 0 precision and a rounding mode of
	// big.ToNearestAway.
	// This is used for unlimited precision arithmetic.
	Unlimited = FPArithSpec{
		Precision:    0,
		RoundingMode: big.ToNearestAway,
	}
)

// QueryRequest encapsulates a query. A query may be either a string query
// statement or a prepared query, which may include bind variables.
// A query request cannot have both a string statement and prepared query, but
// it must have one or the other.
//
// For performance reasons prepared queries are preferred for queries that may
// be reused. Prepared queries bypass compilation of the query. They also allow
// for parameterized queries using bind variables.
type QueryRequest struct {
	// Statement specifies a query statement.
	Statement string `json:"statement,omitempty"`

	// PreparedStatement specifies the prepared query statement.
	PreparedStatement *PreparedStatement `json:"preparedStatement,omitempty"`

	// Limit specifies the limit on number of items returned by the operation.
	// This allows an operation to return less than the default amount of data.
	Limit uint `json:"limit,omitempty"`

	// MaxReadKB specifies the limit on the total data read during this operation, in KB.
	//
	// For cloud service, this value can only reduce the system defined limit.
	// An attempt to increase the limit beyond the system defined limit will
	// cause an IllegalArgument error. This limit is independent of read units
	// consumed by the operation.
	//
	// It is recommended that for tables with relatively low provisioned read
	// throughput that this limit be reduced to less than or equal to one half
	// of the provisioned throughput in order to avoid or reduce throttling
	// errors.
	MaxReadKB uint `json:"maxReadKB,omitempty"`

	// MaxWriteKB specifies the limit on the total data written during this operation, in KB.
	//
	// This limit is independent of write units consumed by the operation.
	MaxWriteKB int `json:"maxWriteKB"`

	// MaxMemoryConsumption specifies the maximum number of memory in bytes that
	// may be consumed by the query at the client for operations such as
	// duplicate elimination (which may be required due to the use of an index
	// on an array or map) and sorting. Such operations may consume a lot of
	// memory as they need to cache the full result set or a large subset of
	// it at the client memory.
	//
	// The default value is 1GB.
	MaxMemoryConsumption int64 `json:"maxMemoryConsumption"`

	// MaxServerMemoryConsumption specifies the maximum number of memory bytes allowed
	// to be consumed by the statement at a replication node. In general, queries
	// do not consume a lot of memory while executing at a replcation node and the
	// value of this parameter has no effect. Currently, the only exceptions are
	// queries that use the array_collect function. For such queries, if
	// the maximum amount of memory is exceeded, execution of the query
	// at the replication node will be terminated (without error) and the
	// set of query results that have been computed so far will be sent
	// to the driver. The driver will keep executing the query, sending
	// more requests to the replication nodes for additional results.
	// So, for queries that use array_collect, increasing the value of this
	// parameter will decrease the number of interactions between the driver
	// and the replication nodes at the expense of consuming the memory
	// consumption at the nodes.
	//
	// The default value is 10MB, and for applications running on the
	// cloud, it can not be increased beyond this default.
	MaxServerMemoryConsumption int64 `json:"maxServerMemoryConsumption"`

	// FPArithSpec specifies the desired representation in terms of decimal
	// precision (number of digits) and rounding rules for the floating-point
	// values as arithematic operation results.
	//
	// The default is Decimal32, that is in IEEE 745R Decimal32 format.
	FPArithSpec *FPArithSpec

	// Consistency specifies desired consistency policy for the request.
	// It is optional.
	// If set, it must be either types.Absolute or types.Eventual, otherwise
	// an IllegalArgument error will be returned.
	// If not set, the default consistency value configured for Client is used,
	// which is determined by RequestConfig.DefaultConsistency().
	Consistency types.Consistency `json:"consistency,omitempty"`

	// Durability is currently only used in On-Prem installations.
	// This setting only applies if the query modifies
	// a row using an INSERT, UPSERT, or DELETE statement. If the query is
	// read-only it is ignored.
	// Added in SDK Version 1.4.0
	Durability types.Durability `json:"durability"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// traceLevel sets the desired tracing level used to trace the query execution.
	traceLevel int

	// continuationKey specifies the continuation key.
	// This is used to continue an operation that returned this key in its QueryResult.
	continuationKey []byte

	// The query driver bound to this query request.
	// This is only used for advanced query.
	driver *queryDriver

	// isInternal indicates if this is an internal request that is created and
	// submitted for execution by the receiveIter.
	isInternal bool

	// shardID represents the id of shard at which the QueryRequest should be executed.
	// This is only used for advanced queries where sorting is required.
	shardID *int

	// TableName is only used in the cloud service.
	// TableName should be set to the table specified in the query.
	// If this is not set, cloud rate limiting may not work properly for the
	// query request.
	TableName string

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name in the SQL query string
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	// virtualScan is used for internal queru requests only.
	virtualScan *virtualScan

	common.InternalRequestData
}

func (r *QueryRequest) validate() (err error) {
	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	if err = validateConsistency(r.Consistency); err != nil {
		return
	}

	if r.Statement == "" && r.PreparedStatement == nil {
		return nosqlerr.NewIllegalArgument("QueryRequest: either Statement or PreparedStatement should be set")
	}

	return
}

func (r *QueryRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Consistency == 0 {
		r.Consistency = cfg.DefaultConsistency()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *QueryRequest) shouldRetry() bool {
	return true
}

func (r *QueryRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *QueryRequest) getTableName() string {
	return r.TableName
}

func (r *QueryRequest) getNamespace() string {
	return r.Namespace
}

func (r *QueryRequest) doesReads() bool {
	return true
}

func (r *QueryRequest) doesWrites() bool {
	return true
}

func (r *QueryRequest) getVirtualScan() *virtualScan {
	return r.virtualScan
}

// copyInternal creates an internal QueryRequest out of the application provided QueryRequest.
func (r *QueryRequest) copyInternal() *QueryRequest {
	return &QueryRequest{
		Timeout:              r.Timeout,
		Limit:                r.Limit,
		MaxReadKB:            r.MaxReadKB,
		MaxWriteKB:           r.MaxWriteKB,
		MaxMemoryConsumption: r.MaxMemoryConsumption,
		Consistency:          r.Consistency,
		Durability:           r.Durability,
		PreparedStatement:    r.PreparedStatement,
		driver:               r.driver,
		traceLevel:           r.traceLevel,
		TableName:            r.TableName,
		InternalRequestData:  r.InternalRequestData,
		isInternal:           true,
	}
}

// hasDriver reports whether the QueryRequest is bound with a query driver.
//
// A request used for advanced query will be bound with a query driver.
func (r *QueryRequest) hasDriver() bool {
	return r.driver != nil
}

// isPrepared reports whether the query request has been prepared.
func (r *QueryRequest) isPrepared() bool {
	return r.PreparedStatement != nil
}

// isSimpleQuery reports whether the QueryRequest represents a simple query.
func (r *QueryRequest) isSimpleQuery() bool {
	return r.PreparedStatement != nil && r.PreparedStatement.isSimpleQuery()
}

// isInternalRequest reports whether this is an internal request that is created
// and submitted for execution by the receiveIter.
func (r *QueryRequest) isInternalRequest() bool {
	return r.isInternal
}

func (r *QueryRequest) setShardID(shardID int) {
	r.shardID = &shardID
}

func (r *QueryRequest) getShardID() int {
	if r.shardID == nil {
		return -1
	}
	return *r.shardID
}

func (r *QueryRequest) getFPArithSpec() *FPArithSpec {
	return r.FPArithSpec
}

// The default value of maximum number of memory, in bytes, allowed to be
// consumed by a query at client, that is 1GB.
const defaultMaxMem int64 = 1024 * 1024 * 1024

// GetMaxMemoryConsumption returns the maximum number of memory bytes that
// may be consumed by the query at the client for operations such as duplicate
// elimination and sorting.
func (r *QueryRequest) GetMaxMemoryConsumption() int64 {
	if r.MaxMemoryConsumption == 0 {
		r.MaxMemoryConsumption = defaultMaxMem
	}

	return r.MaxMemoryConsumption
}

// The default value of maximum memory, in bytes, allowed to be
// consumed by a query at server, that is 10MB
const defaultMaxServerMem int64 = 10 * 1024 * 1024

// GetMaxServerMemoryConsumption returns the maximum number of memory bytes that
// may be consumed by the query at the server.
func (r *QueryRequest) GetMaxServerMemoryConsumption() int64 {
	if r.MaxServerMemoryConsumption == 0 {
		r.MaxServerMemoryConsumption = defaultMaxServerMem
	}

	return r.MaxServerMemoryConsumption
}

func (r *QueryRequest) setContKey(contKey []byte) {
	r.continuationKey = contKey
	if r.driver != nil && !r.isInternal && contKey == nil {
		r.driver.close()
		r.driver = nil
	}
}

// IsDone reports whether the query execution is done.
// It returns true if there are no more query results to be generated, otherwise false.
func (r *QueryRequest) IsDone() bool {
	return r.continuationKey == nil
}

// Close terminates the query execution and releases any memory consumed by the
// query at the client. An application should use this method if it wishes
// to terminate query execution before retrieving all of the query results.
func (r *QueryRequest) Close() {
	r.setContKey(nil)
}

// PreparedStatement encapsulates a prepared query statement. It includes state
// that can be sent to a server and executed without re-parsing the query. It
// includes bind variables which may be set for each successive use of the
// query. The prepared query itself is read-only but this object contains a
// mutable map of bind variables and is not goroutine-safe if variables are
// used.
//
// A single instance of PreparedStatement is goroutine-safe if bind variables
// are not used. If bind variables are to be used and the statement shared
// among goroutines additional instances of PreparedStatement should be created.
type PreparedStatement struct {

	// sqlText represents the application provided SQL text.
	sqlText string

	// queryPlan is the string representation of query plan.
	queryPlan string

	// querySchema is the string representation of query schema.
	querySchema string

	// tableName is the table name returned from a prepared query result, if any.
	tableName string

	// namespace is the namespace returned from a prepared query result, if any.
	namespace string

	// operation is the operation code for the query.
	operation byte

	// driverQueryPlan represents the part of query plan that must be executed at the driver.
	// It is received from the NoSQL database proxy when the query is prepared there.
	// It is deserialized by the driver and not sent back to the database proxy.
	//
	// This is only used for advanced queries.
	driverQueryPlan planIter

	// statement represents the serialized PreparedStatement created at the backend store.
	// It is opaque for the driver.
	// It is received from the NoSQL database proxy and sent back to the proxy
	// every time a new batch of results is needed.
	statement []byte

	// The number of registers required to run the full query plan.
	//
	// This is only used for advanced queries.
	numRegisters int

	// The number of iterators in the full query plan.
	//
	// This is only used for advanced queries.
	numIterators int

	// variableToIDs maps the name of each external variable to its id, which is
	// a position in a FieldValue array stored in the runtimeControlBlock and
	// holding the values of the variables.
	//
	// This is only used for advanced queries.
	variableToIDs map[string]int

	// bindVariables is a map that associates the name to the value for external
	// variables used in the query.
	//
	// This map is populated by the application using the SetVariable() method.
	// It is sent to the NoSQL database proxy every time a new batch of results is needed.
	// The values in this map are also placed in the runtimeControlBlock
	// FieldValue array, just before the query starts its execution at the driver.
	bindVariables map[string]interface{}
}

// The minimum length of byte sequences that represent the serialized prepared statement.
// This is used for sanity check.
const minSerializedStmtLen = 10

func newPreparedStatement(sqlText, queryPlan string,
	statement []byte, driverPlan planIter, numIterators, numRegisters int,
	variableToIDs map[string]int) (*PreparedStatement, error) {

	if len(statement) < minSerializedStmtLen {
		return nil, nosqlerr.NewIllegalArgument("invalid prepared query, cannot be nil")
	}

	return &PreparedStatement{
		sqlText:         sqlText,
		queryPlan:       queryPlan,
		statement:       statement,
		driverQueryPlan: driverPlan,
		numIterators:    numIterators,
		numRegisters:    numRegisters,
		variableToIDs:   variableToIDs,
	}, nil
}

// SetVariable sets value for the specified variable used for the query.
// Existing variables with the same name are silently overwritten.
// The name and type of the value are validated when the query is executed.
func (p *PreparedStatement) SetVariable(name string, value interface{}) error {
	if len(p.variableToIDs) > 0 {
		if _, ok := p.variableToIDs[name]; !ok {
			return nosqlerr.NewIllegalArgument("the query does not contain the variable %s", name)
		}
	}

	if p.bindVariables == nil {
		p.bindVariables = make(map[string]interface{}, 5)
	}

	p.bindVariables[name] = value
	return nil
}

func (p *PreparedStatement) isSimpleQuery() bool {
	return p.driverQueryPlan == nil
}

func (p *PreparedStatement) getBoundVarValues() []types.FieldValue {
	n := len(p.bindVariables)
	if n == 0 {
		return nil
	}

	values := make([]types.FieldValue, n)
	for k, v := range p.bindVariables {
		id, ok := p.variableToIDs[k]
		if ok && id < n {
			values[id] = v
		}
	}

	return values
}

// GetQueryPlan returns the string (JSON) representation of the
// query execution plan, if it was requested in the PrepareRequest; empty otherwise.
// Added in SDK Version 1.4.0
func (p *PreparedStatement) GetQueryPlan() string {
	return p.queryPlan
}

// GetQuerySchema returns the string (JSON) representation of the
// schema of the query result for this query, if it was requested in
// the PrepareRequest; empty otherwise.
// Added in SDK Version 1.4.0
func (p *PreparedStatement) GetQuerySchema() string {
	return p.querySchema
}

// WriteMultipleRequest represents the input to a Client.WriteMultiple() operation.
//
// This request can be used to perform a sequence of PutRequest or DeleteRequest
// operations associated with a table that share the same shard key portion of
// their primary keys, the WriteMultiple operation as whole is atomic.
// It is an efficient way to atomically modify multiple related rows.
//
// On a successful operation WriteMultipleResult.Success returns true.
// The execution result of each operations can be retrieved using WriteMultipleResult.ResultSet.
//
// If the WriteMultiple operation is aborted because of the failure of an
// operation with AbortOnFail set to true, then WriteMultipleResult.Success
// returns false, the index of failed operation can be accessed using
// WriteMultipleResult.FailedOperationIndex.
type WriteMultipleRequest struct {
	// TableName specifies the name of table for the request.
	// This is now ignored: table name(s) are derived from sub-requests.
	TableName string `json:"tableName"`

	// Operations specifies a list of operations for the request.
	// Usually it should not be set explicitly, instead, use the AddPutRequest()
	// or AddDeleteRequest() methods to add the operations.
	Operations []*WriteOperation `json:"operations"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Durability is currently only used in On-Prem installations.
	// Added in SDK Version 1.3.0
	Durability types.Durability `json:"durability"`

	// checkSubReqSize represents whether to check sub request size.
	// This is for internal use, and is set automatically by client.
	checkSubReqSize bool

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name in the SQL query string
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	// NOTE: unlike TableName, the Namespace in WriteMultipleRequest is used
	// as the namespace for all sub-requests. Namespaces in sub-requests will
	// be ignored.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *WriteMultipleRequest) validate() (err error) {
	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	if len(r.Operations) == 0 {
		return nosqlerr.NewIllegalArgument("WriteMultipleRequest: must specify at least one operation")
	}

	if len(r.Operations) == 1 {
		return nil
	}

	return r.validateTables()
}

func (r *WriteMultipleRequest) validateTables() (err error) {
	topTableName := ""
	for i, op := range r.Operations {
		if op == nil {
			return nosqlerr.NewIllegalArgument("WriteMultipleRequest: the %s operation is nil", ordinal(i))
		}
		if err = op.validate(); err != nil {
			return
		}
		if topTableName == "" {
			topTableName = r.getTopTableName(op.tableName())
		} else {
			// check for parent/child table names
			opTopTable := r.getTopTableName(op.tableName())
			if !strings.EqualFold(topTableName, opTopTable) {
				return nosqlerr.NewIllegalArgument("WriteMultipleRequest: "+
					"All sub requests should operate on the same table or "+
					"descendant tables belonging to the same top level "+
					"table. The table '%s' is different from the table of "+
					"other requests: '%s'", opTopTable, topTableName)
			}
		}
	}

	return nil
}

func (r *WriteMultipleRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	for _, op := range r.Operations {
		op.setDefaults(cfg)
	}
}

func (r *WriteMultipleRequest) shouldRetry() bool {
	return true
}

func (r *WriteMultipleRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *WriteMultipleRequest) getTableName() string {
	return r.TableName
}

func (r *WriteMultipleRequest) getNamespace() string {
	return r.Namespace
}

func (r *WriteMultipleRequest) doesReads() bool {
	return true
}

func (r *WriteMultipleRequest) doesWrites() bool {
	return true
}

// Clear removes all of the operations from the WriteMultiple request.
func (r *WriteMultipleRequest) Clear() {
	r.TableName = ""
	r.Operations = nil
}

// return the top table name based on dot (".") separators
func (r *WriteMultipleRequest) getTopTableName(tableName string) string {
	if tableName == "" {
		return tableName
	}
	return strings.Split(tableName, ".")[0]
}

// AddPutRequest adds a Put request as a sub request of the WriteMultiple request.
// If abortOnFail is set to true, the WriteMultiple request will fail if any of
// the sub requests fail.
func (r *WriteMultipleRequest) AddPutRequest(p *PutRequest, abortOnFail bool) (err error) {
	if p == nil {
		return nosqlerr.NewIllegalArgument("PutRequest must be non-nil")
	}
	p.isSubRequest = true
	if err = p.validate(); err != nil {
		return
	}
	op := &WriteOperation{
		PutRequest:  p,
		AbortOnFail: abortOnFail,
	}
	r.Operations = append(r.Operations, op)
	return r.validateTables()
}

// AddDeleteRequest adds a Delete request as a sub request of the WriteMultiple request.
// If abortOnFail is set to true, the WriteMultiple request will fail if any of
// the sub requests fail.
func (r *WriteMultipleRequest) AddDeleteRequest(d *DeleteRequest, abortOnFail bool) (err error) {
	if d == nil {
		return nosqlerr.NewIllegalArgument("DeleteRequest must be non-nil")
	}
	d.isSubRequest = true
	if err = d.validate(); err != nil {
		return
	}
	op := &WriteOperation{
		DeleteRequest: d,
		AbortOnFail:   abortOnFail,
	}
	r.Operations = append(r.Operations, op)
	return r.validateTables()
}

// MultiDeleteRequest represents the input to a Client.MultiDelete operation
// which can be used to delete a range of values that match the primary key and
// range provided.
//
// A range is specified using a partial key plus a range based on the portion of
// the key that is not provided. For example if a table's primary key is
// <id, timestamp> and the its shard key is the "id", it is possible to delete a
// range of timestamp values for a specific id by providing an id but no
// timestamp in the value used for Key and providing a range of timestamp values
// in the FieldRange.
//
// Because this operation can exceed the maximum amount of data modified in a
// single operation, a continuation key can be used to continue the operation.
// The continuation key is obtained from MultiDeleteResult.ContinuationKey and
// set in a new request using MultiDeleteRequest.ContinuationKey. Operations
// with a continuation key still require the primary key.
type MultiDeleteRequest struct {
	// TableName specifies the name of table for the request.
	// It is required and must be non-empty.
	TableName string `json:"tableName"`

	// Key specifies the partial key used for the request.
	// It is required and must be non-nil.
	Key *types.MapValue `json:"key"`

	// ContinuationKey specifies the continuation key to use to continue the operation.
	ContinuationKey []byte `json:"continuationKey,omitempty"`

	// FieldRange specifies the FieldRange to be used for the operation.
	// It is optional, but required to delete a specific range of rows.
	FieldRange *types.FieldRange `json:"fieldRange,omitempty"`

	// MaxWriteKB specifies the limit on the total KB write during this operation.
	//
	// It is optional and has no effect for on-premise.
	//
	// When use for cloud service, if this value is not set, or set to 0, there
	// is no application-defined limit. This value can only reduce the system
	// defined limit. An attempt to increase the limit beyond the system defined
	// limit will cause an IllegalArgument error.
	MaxWriteKB uint `json:"maxWriteKB,omitempty"`

	// Timeout specifies the timeout value for the request.
	// It is optional.
	// If set, it must be greater than or equal to 1 millisecond, otherwise an
	// IllegalArgument error will be returned.
	// If not set, the default timeout value configured for Client is used,
	// which is determined by RequestConfig.DefaultRequestTimeout().
	Timeout time.Duration `json:"timeout"`

	// Durability is currently only used in On-Prem installations.
	// Added in SDK Version 1.3.0
	Durability types.Durability `json:"durability"`

	// Namespace is used on-premises only. It defines a namespace to use
	// for the request. It is optional.
	// If a namespace is specified in the table name for the request
	// (using the namespace:tablename format), that value will override this
	// setting.
	// If not set, the default namespace value configured for Client is used,
	// which is determined by RequestConfig.DefaultNamespace().
	// This is only available with on-premises installations using NoSQL
	// Server versions 23.3 and above.
	Namespace string `json:"namespace,omitempty"`

	common.InternalRequestData
}

func (r *MultiDeleteRequest) validate() (err error) {
	if err = validateTableName(r.TableName); err != nil {
		return
	}

	if err = validateTimeout(r.Timeout); err != nil {
		return
	}

	if err = validateKey(r.Key); err != nil {
		return
	}

	if r.FieldRange == nil {
		return
	}

	return validateFieldRange(r.FieldRange)
}

func (r *MultiDeleteRequest) setDefaults(cfg *RequestConfig) {
	if r.Timeout == 0 {
		r.Timeout = cfg.DefaultRequestTimeout()
	}

	if r.Namespace == "" {
		r.Namespace = cfg.DefaultNamespace()
	}
}

func (r *MultiDeleteRequest) shouldRetry() bool {
	return true
}

func (r *MultiDeleteRequest) timeout() time.Duration {
	return r.Timeout
}

func (r *MultiDeleteRequest) getTableName() string {
	return r.TableName
}

func (r *MultiDeleteRequest) getNamespace() string {
	return r.Namespace
}

func (r *MultiDeleteRequest) doesReads() bool {
	return true
}

func (r *MultiDeleteRequest) doesWrites() bool {
	return true
}

// WriteOperation represents a put or delete operation that can be added into
// a WriteMultipleRequest. Either specify a PutRequest or DeleteRequest for the
// WriteOperation. Specifying both PutRequest and DeleteRequest in a single
// WriteOperation would cause an IllegalArgument error.
type WriteOperation struct {
	// AbortOnFail specifies whether to abort all operations that included in
	// the same WriteMultipleRequest when this operation fails.
	AbortOnFail bool `json:"abortOnFail"`

	// DeleteRequest specifies a delete operation.
	DeleteRequest *DeleteRequest `json:"deleteRequest,omitempty"`

	// PutRequest specifies a put operation.
	PutRequest *PutRequest `json:"putRequest,omitempty"`
}

func (op *WriteOperation) validate() error {
	if op.DeleteRequest != nil && op.PutRequest != nil {
		return nosqlerr.NewIllegalArgument("only one of the PutRequest or DeleteRequest may be specified for WriteOperation")
	}

	if op.DeleteRequest == nil && op.PutRequest == nil {
		return nosqlerr.NewIllegalArgument("either PutRequest or DeleteRequest should be specified for WriteOperation")
	}

	if op.DeleteRequest != nil {
		return op.DeleteRequest.validate()
	}

	return op.PutRequest.validate()
}

func (op *WriteOperation) setDefaults(cfg *RequestConfig) {
	if op.DeleteRequest != nil {
		op.DeleteRequest.setDefaults(cfg)
		return
	}

	if op.PutRequest != nil {
		op.PutRequest.setDefaults(cfg)
		return
	}
}

func (op *WriteOperation) tableName() string {
	if op.DeleteRequest != nil {
		return op.DeleteRequest.TableName
	}

	if op.PutRequest != nil {
		return op.PutRequest.TableName
	}

	return ""
}

// validateTimeout validates the specified timeout is greater than or equal to 1 millisecond.
func validateTimeout(timeout time.Duration) error {
	if timeout < time.Millisecond {
		return nosqlerr.NewIllegalArgument("Timeout must be greater than or equal to 1 millisecond")
	}

	return nil
}

// validateConsistency validates the specified consistency value is either
// types.Eventual or types.Absolute, otherwise it returns an IllegalArgument
// error.
func validateConsistency(consistency types.Consistency) error {
	switch consistency {
	case types.Eventual, types.Absolute:
		return nil

	default:
		return nosqlerr.NewIllegalArgument("Consistency must be either Absolute or Eventual")
	}
}

// validateTableName validates the specified table name is non-empty.
func validateTableName(tableName string) error {
	if tableName == "" {
		return nosqlerr.NewIllegalArgument("TableName must be non-empty")
	}

	return nil
}

// validateKey validates the specified key is non-nil and contains a least
// one entry in the key.
func validateKey(key *types.MapValue) error {
	if key == nil {
		return nosqlerr.NewIllegalArgument("Key must be non-nil")
	}

	if key.Len() == 0 {
		return nosqlerr.NewIllegalArgument("Key must be non-empty")
	}

	return nil
}

// validateFieldRange validates the specified field range values. The Start
// and End values specified must be of the same type, and at least one of them
// must be specified.
func validateFieldRange(r *types.FieldRange) error {
	if r == nil {
		return nosqlerr.NewIllegalArgument("FieldRange is nil")
	}

	if r.Start == nil && r.End == nil {
		return nosqlerr.NewIllegalArgument("must specify a Start or End value for FieldRange")
	}

	if r.Start != nil && r.End != nil {
		t1 := reflect.TypeOf(r.Start).Kind()
		t2 := reflect.TypeOf(r.End).Kind()
		if t1 != t2 {
			return nosqlerr.NewIllegalArgument("FieldRange Start type (%T) is different from End type (%T)",
				r.Start, r.End)
		}
	}

	return nil
}

// validateTableLimits validates the specified table limits, it returns an
// IllegalArgument error if the limits is nil or the limit values are zero.
func validateTableLimits(limits *TableLimits) error {
	if limits == nil {
		return nosqlerr.NewIllegalArgument("TableLimits must be non-nil")
	}

	return limits.validate()
}

func ordinal(i int) string {
	if i < 0 {
		return ""
	}

	var sfx string
	n := i + 1
	switch n {
	case 11, 12, 13:
		sfx = "th"
	default:
		switch n % 10 {
		case 1:
			sfx = "st"
		case 2:
			sfx = "nd"
		case 3:
			sfx = "rd"
		default:
			sfx = "th"
		}
	}

	return fmt.Sprintf("%d%s", n, sfx)
}
