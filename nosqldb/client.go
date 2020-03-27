//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/auth"
	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/jsonutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
)

// Client represents an Oracle NoSQL database client used to access the Oracle
// NoSQL database cloud service or on-premise Oracle NoSQL database servers.
type Client struct {
	// Config specifies the configuration parameters associated with the Client.
	// Most configuration parameters have default values that should suffice for use.
	Config

	// HTTPClient represents an HTTP client associated with a Client instance.
	// It is used to send Client requests to server and receive responses.
	HTTPClient *httputil.HTTPClient

	// logger specifies a Client logger used to log events.
	logger *logger.Logger

	// requestURL represents the server URL that is the target of all client requests.
	requestURL string

	// requestID represents a unique request id associated with each request.
	// It is used the keep track of a request.
	requestID int32

	// serverHost represents the host of NoSQL server.
	serverHost string

	// executor specifies a request executor.
	// This is used internally by tests for customizing request execution.
	executor httputil.RequestExecutor

	// handleResponse specifies a function that is used to handle the response
	// returned from server.
	// This is used internally by tests for customizing response processing.
	handleResponse func(httpResp *http.Response, req Request) (Result, error)

	// isCloud represents whether the client connects to the cloud service or
	// cloud simulator.
	isCloud bool
}

var (
	errNilRequest       = nosqlerr.NewIllegalArgument("request must be non-nil")
	errNilContext       = nosqlerr.NewIllegalArgument("nil context")
	errUnexpectedResult = errors.New("got unexpected result for the request")
)

// NewClient creates a Client instance with the specified Config.
// If any errors occurred during the creation, it returns a non-nil error and
// a nil Client that should not be used. Applications should check the returned
// error before using the returned Client instance.
//
// Applications should call the Close() method on the Client when it terminates.
func NewClient(cfg Config) (*Client, error) {
	err := cfg.setDefaults()
	if err != nil {
		return nil, err
	}

	if cfg.httpClient == nil {
		cfg.httpClient, err = httputil.NewHTTPClient(cfg.HTTPConfig)
		if err != nil {
			return nil, err
		}
	}

	c := &Client{
		Config:     cfg,
		HTTPClient: cfg.httpClient,
		requestURL: cfg.Endpoint + sdkutil.DataServiceURI,
		requestID:  0,
		serverHost: cfg.host,
		executor:   cfg.httpClient,
		logger:     cfg.Logger,
		isCloud:    cfg.IsCloud() || cfg.IsCloudSim(),
	}
	c.handleResponse = c.processResponse
	return c, nil
}

// Close releases any resources used by Client.
func (c *Client) Close() error {
	if c.AuthorizationProvider != nil {
		c.AuthorizationProvider.Close()
	}

	return nil
}

// Get retrieves the row associated with a primary key.
//
// The table name and primary key for the get operation must be specified in the
// GetRequest, otherwise an IllegalArgument error is returned.
//
// On success the returned GetResult is non-nil, the value of the row is
// available in GetResult.Value. If there are no matching rows GetResult.Value
// will be nil.
//
// The default Consistency used for the operation is types.Eventual unless an
// explicit value has been set using GetRequest.Consistency or
// RequestConfig.Consistency.
//
// Use of types.Absolute consistency may affect latency of the operation and may
// result in additional cost for the operation.
func (c *Client) Get(req *GetRequest) (*GetResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*GetResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// GetTable retrieves static information about the specified table including its
// state, provisioned throughput, capacity and schema. Dynamic information such
// as usage is obtained using GetTableUsage.
//
// The table name must be specified in the GetTableRequest, otherwise an
// IllegalArgument error is returned.
//
// Throughput, capacity and usage information is only available in the returned
// TableResult when using the Cloud Service and will be nil or not defined for
// on-premise.
func (c *Client) GetTable(req *GetTableRequest) (*TableResult, error) {
	return c.getTableWithContext(context.Background(), req)
}

func (c *Client) getTableWithContext(ctx context.Context, req *GetTableRequest) (*TableResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.executeWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*TableResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// GetIndexes retrieves information about an index, or all indexes on a table.
// If no index name is specified in the GetIndexesRequest, then information on
// all indexes is returned.
//
// The table name must be specified in the GetIndexesRequest, otherwise an
// IllegalArgument error is returned.
//
// On success the returned GetIndexesResult is non-nil and contains desired
// index information.
func (c *Client) GetIndexes(req *GetIndexesRequest) (*GetIndexesResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*GetIndexesResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// DoTableRequest performs an operation that manages table schema or changes
// table limits.
//
// This method can be used to perform the following operations:
//
//   create tables
//   drop tables
//   modify tables: add or remove columns
//   create indexes
//   drop indexes
//   change table limits of an existing table
//
// These operations are implicitly asynchronous. DoTableRequest does not wait
// for completion of the operation, it returns a TableResult that contains an
// operation id representing the operation being performed. The caller should
// use the TableResult.WaitForCompletion() method to determine when it has completed.
func (c *Client) DoTableRequest(req *TableRequest) (*TableResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*TableResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// DoTableRequestAndWait performs an operation that manages table schema or
// changes table limits and waits for completion of the operation.
//
// This method can be used to perform the following operations:
//
//   create tables
//   drop tables
//   modify tables: add or remove columns
//   create indexes
//   drop indexes
//   change table limits of an existing table
//
// These are potentially long-running operations that take time to complete.
// This method allows specifying a timeout that represents a time duration to
// wait for completion of the operation, and a pollInterval that represents a
// time duration to wait between two consecutive polling attempts. If the
// operation does not complete when the specified timeout elapses, a
// RequestTimeout error is returned.
func (c *Client) DoTableRequestAndWait(req *TableRequest, timeout, pollInterval time.Duration) (*TableResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*TableResult); ok {
		return res.WaitForCompletion(c, timeout, pollInterval)
	}

	return nil, errUnexpectedResult
}

// DoSystemRequest performs a system operation such as administrative operations
// that do not affect a specific table. For table-specific operations use
// DoTableRequest() or DoTableRequestAndWait().
//
// Examples of statements in the SystemRequest passed to this method include:
//
//   CREATE NAMESPACE mynamespace
//   CREATE USER some_user IDENTIFIED BY password
//   CREATE ROLE some_role
//   GRANT ROLE some_role TO USER some_user
//
// These operations are implicitly asynchronous. DoSystemRequest does not wait
// for completion of the operation, it returns a SystemResult that contains an
// operation id representing the operation being performed. The caller must poll
// using SystemResult.WaitForCompletion() method to determine when it has
// completed.
//
// This method is used for on-premise only.
func (c *Client) DoSystemRequest(req *SystemRequest) (*SystemResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*SystemResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// DoSystemRequestAndWait is a convenience method used to perform a system
// operation such as administrative operations that do not affect a specific
// table, and waits for completion of the operation. For table-specific
// operations use DoTableRequest() or DoTableRequestAndWait().
//
// Examples of statements passed to this method include:
//
//   CREATE NAMESPACE mynamespace
//   CREATE USER some_user IDENTIFIED BY password
//   CREATE ROLE some_role
//   GRANT ROLE some_role TO USER some_user
//
// These are potentially long-running operations that take time to complete.
// This method allows specifying a timeout that represents a time duration to
// wait for completion of the operation, and a pollInterval that represents a
// time duration to wait between two consecutive polling attempts. If the
// operation does not complete when the specified timeout elapses, a
// RequestTimeout error is returned.
//
// This method is used for on-premise only.
func (c *Client) DoSystemRequestAndWait(statement string, timeout, pollInterval time.Duration) (*SystemResult, error) {
	req := &SystemRequest{
		Statement: statement,
		Timeout:   timeout,
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*SystemResult); ok {
		return res.WaitForCompletion(c, timeout, pollInterval)
	}

	return nil, errUnexpectedResult
}

// GetSystemStatus checks the status of an operation previously performed using
// DoSystemRequest().
func (c *Client) GetSystemStatus(req *SystemStatusRequest) (*SystemResult, error) {
	return c.getSystemStatusWithContext(context.Background(), req)
}

func (c *Client) getSystemStatusWithContext(ctx context.Context, req *SystemStatusRequest) (*SystemResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.executeWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*SystemResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// ListTables lists all available table names. If further information about a
// specific table is desired the GetTable method may be used. If a given
// identity has access to a large number of tables the list may be paged by
// specifying the StartIndex and Limit field of the request.
func (c *Client) ListTables(req *ListTablesRequest) (*ListTablesResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*ListTablesResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// ListNamespaces returns the namespaces in a store as a slice of string.
//
// This method is used for on-premise only.
func (c *Client) ListNamespaces() ([]string, error) {
	res, err := c.DoSystemRequestAndWait("show as json namespaces", 30*time.Second, time.Second)
	if err != nil {
		return nil, err
	}

	obj, err := jsonutil.ToObject(res.ResultString)
	if err != nil {
		return nil, err
	}

	array, ok := jsonutil.GetArrayFromObject(obj, "namespaces")
	if !ok {
		return nil, fmt.Errorf("cannot find JSON array field \"namespaces\" from JSON: %s", res.ResultString)
	}

	namespaces := make([]string, 0, len(array))
	for _, e := range array {
		ns, err := jsonutil.ExpectString(e)
		if err != nil {
			return nil, err
		}

		namespaces = append(namespaces, ns)
	}

	return namespaces, nil
}

// ListRoles returns the roles in a store as a slice of string
//
// This method is used for on-premise only.
func (c *Client) ListRoles() ([]string, error) {
	res, err := c.DoSystemRequestAndWait("show as json roles", 30*time.Second, time.Second)
	if err != nil {
		return nil, err
	}

	obj, err := jsonutil.ToObject(res.ResultString)
	if err != nil {
		return nil, err
	}

	array, ok := jsonutil.GetArrayFromObject(obj, "roles")
	if !ok {
		return nil, fmt.Errorf("cannot find JSON array field \"roles\" from JSON: %s", res.ResultString)
	}

	roles := make([]string, 0, len(array))
	for _, e := range array {
		obj, err = jsonutil.ExpectObject(e)
		if err != nil {
			return nil, err
		}

		name, ok := jsonutil.GetStringFromObject(obj, "name")
		if !ok {
			return nil, fmt.Errorf("cannot find role name from JSON: %s", res.ResultString)
		}

		roles = append(roles, name)
	}

	return roles, nil
}

// UserInfo encapsulates the information associated with a user including the
// id and user name in the system.
//
// This is used for on-premise only.
type UserInfo struct {
	// ID represents user id.
	ID string

	// Name represents user name.
	Name string
}

// ListUsers returns the users in a store as a slice of UserInfo.
//
// This method is used for on-premise only.
func (c *Client) ListUsers() ([]UserInfo, error) {
	res, err := c.DoSystemRequestAndWait("show as json users", 30*time.Second, time.Second)
	if err != nil {
		return nil, err
	}

	obj, err := jsonutil.ToObject(res.ResultString)
	if err != nil {
		return nil, err
	}

	array, ok := jsonutil.GetArrayFromObject(obj, "users")
	if !ok {
		return nil, fmt.Errorf("cannot find JSON array field \"users\" from JSON: %s", res.ResultString)
	}

	users := make([]UserInfo, 0, len(array))
	for _, e := range array {
		obj, err = jsonutil.ExpectObject(e)
		if err != nil {
			return nil, err
		}

		id, ok := jsonutil.GetStringFromObject(obj, "id")
		if !ok {
			return nil, fmt.Errorf("cannot find user id from JSON: %s", res.ResultString)
		}

		name, ok := jsonutil.GetStringFromObject(obj, "name")
		if !ok {
			return nil, fmt.Errorf("cannot find user name from JSON: %s", res.ResultString)
		}

		users = append(users, UserInfo{ID: id, Name: name})
	}

	return users, nil
}

// Put puts a row into a table.
//
// This method creates a new row or overwrites an existing row entirely. The
// value used for the put is specified in PutRequest.Value and must contain a
// complete primary key and all required fields.
//
// It is not possible to put part of a row. Any fields that are not provided
// will be defaulted, overwriting any existing value. Fields that are not
// nullable or defaulted must be provided or an error will be returned.
//
// By default a put operation is unconditional, but put operations can be
// conditional based on existence, or not, of a previous value as well as
// conditional on the version of the existing value.
//
// a. Use PutIfAbsent option to do a put only if there is no existing row that
// matches the primary key.
//
// b. Use PutIfPresent option to do a put only if there is an existing row that
// matches the primary key.
//
// c. Use PutIfVersion option to do a put only if there is an existing row that
// matches the primary key and its version matches that provided.
//
// If put operation succeeds, this method retuns a non-nil PutResult.Version
// representing the current version of the row that is put.
//
// If put operation fails, this method returns a nil PutResult.Version.
// It is also possible, on failure, to return information about the existing
// row. The row, including it's version can be optionally returned if a put
// operation fails because of a version mismatch or if the operation fails
// because the row already exists. The existing row information will only be
// returned if PutRequest.ReturnRow is true and one of the following occurs:
//
// a. The PutIfAbsent option is used and the operation fails because the row
// already exists.
//
// b. The PutIfVersion option is used and the operation fails because the row
// exists and its version does not match.
//
// Use of PutRequest.ReturnRow may result in additional consumed read capacity.
// If the operation is successful there will be no information returned about
// the previous row.
func (c *Client) Put(req *PutRequest) (*PutResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*PutResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// Delete deletes a row from a table.
//
// The row is identified using a primary key specified in DeleteRequest.Key.
//
// By default a delete operation is unconditional and will succeed if the
// specified row exists. Delete operations can be made conditional based on
// whether the version of an existing row matches that specified in
// DeleteRequest.MatchVersion.
//
// It is also possible, on failure, to return information about the existing
// row. The row, including it's version can be optionally returned if a delete
// operation fails because of a version mismatch. The existing row information
// will only be returned if DeleteRequest.ReturnRow is true and the operation
// fails because DeleteRequest.MatchVersion is used and the operation fails
// because the row exists and its version does not match.
//
// Use of DeleteRequest.ReturnRow may result in additional consumed read
// capacity. If the operation is successful there will be no information
// returned about the previous row.
func (c *Client) Delete(req *DeleteRequest) (*DeleteResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*DeleteResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// GetTableUsage gets dynamic information about the specified table such as the
// current throughput usage. Usage information is collected in time slices and
// returned in individual usage records. It is possible to specify a time-based
// range of usage records using StartTime and EndTime of TableUsageRequest.
//
// This method is used for cloud service only.
func (c *Client) GetTableUsage(req *TableUsageRequest) (*TableUsageResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*TableUsageResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// WriteMultiple executes a sequence of operations associated with a table that
// share the same shard key portion of their primary keys, all the specified
// operations are executed within the scope of a single transaction.
//
// When execute on the cloud service, there are some size-based limitations on
// this operation:
//
//   1. The max number of individual operations (put, delete) in a single WriteMultiple request is 50.
//   2. The total request size is limited to 25MB.
//
func (c *Client) WriteMultiple(req *WriteMultipleRequest) (*WriteMultipleResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	req.checkSubReqSize = c.isCloud
	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*WriteMultipleResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// MultiDelete deletes multiple rows from a table in an atomic operation.
//
// The key used may be partial but must contain all of the fields that are in
// the shard key.
//
// A range may be specified to delete a range of keys.
func (c *Client) MultiDelete(req *MultiDeleteRequest) (*MultiDeleteResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*MultiDeleteResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// Prepare prepares a query for execution and reuse. See the Query() method for
// general information and restrictions.
//
// It is recommended that prepared queries are used when the same query will run
// multiple times as execution is much more efficient than starting with a query
// string every time. The query language and Query() method support query
// variables to assist with re-use.
func (c *Client) Prepare(req *PrepareRequest) (*PrepareResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*PrepareResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// Query is used to query a table based on the query statement specified in the
// QueryRequest.
//
// Queries that include a full shard key will execute much more efficiently than
// more distributed queries that must go to multiple shards.
//
// Table-style and system-style queries such as "CREATE TABLE ..." or
// "CREATE USER ..." are not supported by this method. Those operations must be
// performed using DoTableRequest or DoSystemRequest as appropriate.
//
// When execute on the cloud service, the amount of data read by a single query
// request is limited by a system default and can be further limited using
// QueryRequest.MaxReadKB. This limits the amount of data read and not the
// amount of data returned, which means that a query can return zero results but
// still have more data to read. This situation is detected by checking if the
// returned QueryResult.ContinuationKey is nil. For this reason queries should
// always operate in a loop, acquiring more results, until the continuation key
// is nil, indicating that the query is done. Inside the loop the continuation
// key is applied to the QueryRequest using QueryRequest.ContinuationKey.
func (c *Client) Query(req *QueryRequest) (*QueryResult, error) {
	if req == nil {
		return nil, errNilRequest
	}

	res, err := c.execute(req)
	if err != nil {
		return nil, err
	}

	if res, ok := res.(*QueryResult); ok {
		return res, nil
	}

	return nil, errUnexpectedResult
}

// nextRequestID returns the next client-scoped request id. It should be used
// with the client id to obtain a globally unique scope.
func (c *Client) nextRequestID() int32 {
	return atomic.AddInt32(&c.requestID, 1)
}

// processRequest processes the specified request before it is sent to server.
// This method applies default configurations such as timeout and consistency
// values for the request if they are not specified for the request.
func (c *Client) processRequest(req Request) (data []byte, err error) {
	if req == nil {
		return nil, errNilRequest
	}

	// Set default values for the request with the global request configurations
	// associated with the Client. The values will be overwritten if request
	// specific configurations are set.
	req.setDefaults(&c.RequestConfig)

	// Validates the request, returns immediately if validation fails.
	if err = req.validate(); err != nil {
		return nil, err
	}

	data, err = serializeRequest(req)
	if err != nil || !c.isCloud {
		return
	}

	// check request size for cloud
	if err = checkRequestSizeLimit(req, len(data)); err != nil {
		return nil, err
	}

	return
}

// execute is used for request execution.
//
// It sends the request to server, retries the request upon receiving errors
// that are retryable. On success, it parses the response as desired operation result.
func (c *Client) execute(req Request) (Result, error) {
	return c.executeWithContext(context.Background(), req)
}

func (c *Client) executeWithContext(ctx context.Context, req Request) (Result, error) {
	data, err := c.processRequest(req)
	if err != nil {
		return nil, err
	}

	return c.doExecute(ctx, req, data)
}

func (c *Client) doExecute(ctx context.Context, req Request, data []byte) (result Result, err error) {
	if req == nil {
		return nil, errNilRequest
	}

	if ctx == nil {
		return nil, errNilContext
	}

	if queryReq, ok := req.(*QueryRequest); ok && !queryReq.isInternalRequest() {

		// If the QueryRequest represents an advanced query, it will be bound
		// with a queryDriver the first time the execute() is called for the query.
		// Subsequent calls on the execute() for this query return a new, empty
		// QueryResult. The actual computation of a result batch will take
		// place when the application calls GetResults() on the QueryResult.
		if queryReq.hasDriver() {
			c.logger.Debug("the QueryRequest has been bound with a QueryDriver")
			return newQueryResult(queryReq, false), nil
		}

		// This is the 1st execute() call for the advanced query.
		// If the query has been prepared before, create a queryDriver and bind
		// it with the QueryRequest. Then create and return an empty QueryResult.
		// The actual computation of a result batch will take place when the
		// application calls GetResults() on the QueryResult.
		if queryReq.isPrepared() && !queryReq.isSimpleQuery() {
			c.logger.Debug("the QueryRequest is prepared, but is not bound with a QueryDriver")
			driver := newQueryDriver(queryReq)
			driver.client = c
			driver.topologyInfo = queryReq.getTopologyInfo()
			return newQueryResult(queryReq, false), nil
		}

		// This is either a simple query or an advanced query that has not been
		// prepared, which also implies that this is the 1st execute() call on
		// this query.
		// For a non-prepared advanced query, the effect of this 1st execute()
		// call is to send the query to the NoSQL database servers for compilation,
		// get back the prepared query, but no query results, create a QueryDriver,
		// and bind it with the QueryRequest and return an empty QueryResult.
		c.logger.Debug("the QueryRequest is neither prepared nor bound to a QueryDriver")
	}

	var timeout time.Duration
	var authStr string
	var httpReq *http.Request
	var httpResp *http.Response

	reqTimeout := req.timeout()
	secInfoTimeout := c.DefaultSecurityInfoTimeout()
	numRetries := 0
	numThrottleRetries := 0
	startTime := time.Now()

	for {

		if err != nil {
			isSecErr := nosqlerr.IsSecurityInfoUnavailable(err)
			if isSecErr {
				timeout = secInfoTimeout
			} else {
				timeout = reqTimeout
			}

			if time.Since(startTime) > timeout {
				return nil, nosqlerr.NewWithCause(nosqlerr.RequestTimeout, err,
					"request timed out after %d attempt(s). Timeout: %v", numRetries+1, timeout)
			}

			if !c.handleError(err, req, numThrottleRetries) {
				return nil, err
			}

			if isSecErr {
				c.logger.Fine("Client.execute() got error %v, numRetries: %d, numThrottleRetries: %d",
					err, numRetries, numThrottleRetries)
			} else {
				c.logger.Info("Client.execute() got error %v, numRetries: %d, numThrottleRetries: %d",
					err, numRetries, numThrottleRetries)
				// Only count errors other than SecurityInfoUnavailable as throttle retries.
				numThrottleRetries++
			}
			// Increase number of retries
			numRetries++
		}

		// Handle errors that may occur when retrieving authorization string.
		authStr, err = c.getAuthString(req)
		if err != nil {
			continue
		}

		httpReq, err = httputil.NewPostRequest(c.requestURL, data)
		if err != nil {
			return nil, err
		}

		reqID := int(c.nextRequestID())
		httpReq.Header.Add("x-nosql-request-id", strconv.Itoa(reqID))
		httpReq.Header.Add("Host", c.serverHost)
		httpReq.Header.Set("Content-Length", strconv.Itoa(len(data)))
		httpReq.Header.Set("Content-Type", "application/octet-stream")
		httpReq.Header.Set("Accept", "application/octet-stream")
		httpReq.Header.Set("Connection", "keep-alive")
		httpReq.Header.Set("User-Agent", sdkutil.UserAgent())

		// The authorization string could be empty when the client connects to a
		// non-secure on-premise NoSQL database server over database proxy.
		if authStr != "" {
			httpReq.Header.Set("Authorization", authStr)
		}

		err = c.signHTTPRequest(httpReq)
		if err != nil {
			return nil, err
		}

		reqCtx, reqCancel := context.WithTimeout(ctx, reqTimeout)
		httpReq = httpReq.WithContext(reqCtx)
		httpResp, err = c.executor.Do(httpReq)
		if err != nil {
			reqCancel()
			continue
		}

		result, err = c.handleResponse(httpResp, req)
		// Cancel request context after response body has been read.
		reqCancel()
		if err != nil {
			continue
		}

		return result, nil
	}
}

// handleError handles the specified error, returns a bool flag indicating
// whether the request should continue to retry.
//
// If the error is retryable, this method calls the RetryHandler configured for
// the client to proceed with retry handling. Otherwise, it returns false
// indicating the request should not be retried.
func (c *Client) handleError(err error, req Request, numRetries int) (shouldRetry bool) {
	if isRetryableError(err) {
		c.logger.Fine("got retryable error: %v", err)
		return c.handleRetry(err, req, uint(numRetries))
	}

	c.logger.Fine("got non-retryable error: %v", err)
	return false
}

// handleRetry checks if the specified request should continue to retry upon
// receiving the specified error and having attempted the specified number
// of retries. If the request should retry, handleRetry will pause the current
// goroutine for a duration according to the RetryHandler configurations.
func (c *Client) handleRetry(err error, req Request, numRetries uint) bool {
	if c.RetryHandler == nil {
		return false
	}

	c.logger.LogWithFn(logger.Fine, func() string {
		return fmt.Sprintf("retry for request: %s, number of throttle retries: %d, error: %v",
			reflect.TypeOf(req).String(), numRetries, err)
	})

	if c.RetryHandler.ShouldRetry(req, numRetries, err) {
		c.RetryHandler.Delay(req, numRetries, err)
		return true
	}

	if maxRetries := c.RetryHandler.MaxNumRetries(); numRetries >= maxRetries {
		c.logger.Fine("number of retries has reached the maximum of %d", maxRetries)
	}

	return false
}

// getAuthString returns an authorization string for the specified request.
func (c *Client) getAuthString(opReq Request) (string, error) {
	if c.AuthorizationProvider == nil {
		return "", nil
	}

	switch scheme := c.AuthorizationProvider.AuthorizationScheme(); scheme {
	case auth.BearerToken:
		req := &accessTokenRequest{opReq}
		return c.AuthorizationProvider.AuthorizationString(req)
	case auth.Signature:
		// signature method requires an http.Request - auth is added in the Sign() method later
		return "", nil
	default:
		return "", nosqlerr.NewIllegalArgument("unsupported authorization scheme: %s", scheme)
	}
}

func (c *Client) signHTTPRequest(httpReq *http.Request) error {
	if c.AuthorizationProvider == nil {
		return nil
	}

	switch c.AuthorizationProvider.AuthorizationScheme() {
	case auth.Signature:
		// currently this is the only provider that uses an actual http.Request
		return c.AuthorizationProvider.SignHTTPRequest(httpReq)
	case auth.BearerToken:
		// no changes to http req for this method
		return nil
	default:
	}

	return nosqlerr.NewIllegalArgument("unsupported authorization scheme for http request signing")
}

// serializeRequest serializes the specified request into a slice of bytes that
// will be sent to the server. The serial version is always written followed by
// the actual request payload.
func serializeRequest(req Request) (data []byte, err error) {
	wr := binary.NewWriter()
	if _, err = wr.WriteSerialVersion(proto.SerialVersion); err != nil {
		return
	}

	if err = req.serialize(wr); err != nil {
		return
	}

	return wr.Bytes(), nil
}

// processResponse processes the http response returned from server.
//
// If the http response status code is 200, this method reads in response
// content and parses them as an appropriate result suitable for the request.
// Otherwise, it returns the http error.
func (c *Client) processResponse(httpResp *http.Response, req Request) (Result, error) {
	data, err := ioutil.ReadAll(httpResp.Body)
	httpResp.Body.Close()
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode == http.StatusOK {
		return c.processOKResponse(data, req)
	}

	return nil, c.processNotOKResponse(data, httpResp.StatusCode)
}

func (c *Client) processOKResponse(data []byte, req Request) (Result, error) {
	buf := bytes.NewBuffer(data)
	rd := binary.NewReader(buf)
	code, err := rd.ReadByte()
	if err != nil {
		return nil, err
	}

	// A zero byte represents the operation succeeded.
	if code == 0 {
		res, err := req.deserialize(rd)
		if err != nil {
			return nil, err
		}

		if queryReq, ok := req.(*QueryRequest); ok && !queryReq.isSimpleQuery() {
			queryReq.driver.client = c
		}

		return res, nil
	}

	// Operation failed, read the error message.
	s, err := rd.ReadString()
	if err != nil {
		return nil, err
	}

	var msg string
	if s != nil {
		msg = *s
	}

	return nil, wrapResponseErrors(int(code), msg)
}

// processNotOKResponse processes the http response whose status code is not 200.
func (c *Client) processNotOKResponse(data []byte, statusCode int) error {
	if statusCode == http.StatusBadRequest && len(data) > 0 {
		return fmt.Errorf("error response: %s", string(data))
	}

	return fmt.Errorf("error response: %d %s", statusCode, http.StatusText(statusCode))
}

// wrapResponseErrors wraps the error code and message returned from server into appropriate errors.
func wrapResponseErrors(code int, msg string) error {
	errCode := nosqlerr.ErrorCode(code)
	switch errCode {
	case nosqlerr.UnknownError, nosqlerr.UnknownOperation:
		return nosqlerr.New(errCode, "unknown error: %s", msg)

	case nosqlerr.BadProtocolMessage:
		return nosqlerr.NewIllegalArgument("bad protocol message: %s", msg)

	default:
		return nosqlerr.New(errCode, msg)
	}
}

// isRetryableError checks if the specified error is retryable.
//
// An error is retryable if it is a temporary url.Error or is a retryable nosqlerr.Error.
func isRetryableError(err error) bool {
	// http.Client.Do() returns *url.Error. Retry if it is a temporary error.
	if err, ok := err.(*url.Error); ok && err.Temporary() {
		return true
	}

	if err, ok := err.(*nosqlerr.Error); ok && err.Retryable() {
		return true
	}

	return false
}
