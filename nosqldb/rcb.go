//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/sdkutil"
	"github.com/oracle/nosql-go-sdk/nosqldb/logger"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

const (
	// envQueryTraceLevel is the name of environment variable that specifies a
	// level for tracing NoSQL queries.
	// The value of trace level must be an integer that is greater than 0.
	envQueryTraceLevel string = "NOSQL_QUERY_TRACE_LEVEL"

	// envQueryTraceFile is the name of environment variable that specifies a
	// destination file where NoSQL query tracing outputs are written.
	envQueryTraceFile string = "NOSQL_QUERY_TRACE_FILE"
)

// queryTracer is a specialized logger used for tracing NoSQL queries.
// The traceLevel must be an integer that is greater than 0, otherwise
// query tracing is disabled.
type queryTracer struct {
	*logger.Logger
	traceLevel int
}

// newQueryLogger creates a queryTracer with trace level and trace file
// configurations from environment variables.
func newQueryLogger() (tracer *queryTracer, err error) {
	s, ok := os.LookupEnv(envQueryTraceLevel)
	if !ok {
		return nil, nil
	}

	traceLevel, err := strconv.Atoi(s)
	if err != nil {
		return nil, fmt.Errorf("the value of environment variable %q "+
			"must be an integer, got invalid value: %v", envQueryTraceLevel, s)
	}

	if traceLevel <= 0 {
		return nil, fmt.Errorf("the value of environment variable %q "+
			"must be greater than 0, got invalid value: %v", envQueryTraceLevel, traceLevel)
	}

	var out io.Writer
	filePath, ok := os.LookupEnv(envQueryTraceFile)
	if ok {
		traceFile, err := sdkutil.ExpandPath(filePath)
		if err != nil {
			return nil, fmt.Errorf("invalid value of environment variable %q: %v",
				envQueryTraceFile, err)
		}

		file, err := os.OpenFile(traceFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0640)
		if err != nil {
			return nil, fmt.Errorf("cannot open query trace file %s: %v", traceFile, err)
		}

		out = file
	}

	if out == nil {
		out = os.Stderr
	}

	return &queryTracer{
		Logger:     logger.New(out, logger.Trace, false),
		traceLevel: traceLevel,
	}, nil
}

// runtimeControlBlock (RCB) stores all state of an executing query plan.
// There is a single RCB instance per query execution, and all plan iterators
// have access to that instance during the execution.
type runtimeControlBlock struct {
	// queryDriver acts as a query coordinator and gives access to the
	// Client, QueryRequest and PreparedStatement.
	*queryDriver

	// externalVars specifies a slice of values of the external variables set
	// for the operation.
	externalVars []types.FieldValue

	// rootIter represents the root plan iterator of the query plan tree.
	rootIter planIter

	// iterStates specifies a slice of planIterState.
	// It contains as many elements as there are planIter instances in the
	// query plan to be executed.
	iterStates []planIterState

	// registers specifies a slice of field values.
	// It contains as many elements as required by the instances in the query
	// plan to be executed.
	registers []types.FieldValue

	// reachedLimit indicates if the query execution reached the size-based or
	// number-based limit. If so, query execution must stop and a batch of
	// results (potentially empty) must be returned to the application.
	reachedLimit bool

	// Capacity represents the total capacity (readUnits/readKB/writeKB) that
	// were consumed during the execution of one query batch.
	Capacity

	// memoryConsumption represents the number of memory in bytes that were
	// consumed by the query at the client for operations such as duplicate
	// elimination and sorting.
	memoryConsumption int64

	// sqlHashTag is a portion of the hash value of SQL text, used as a tag
	// for query tracing.
	sqlHashTag []byte
}

func newRCB(driver *queryDriver, rootIter planIter, numIterators, numRegisters int,
	externalVars []types.FieldValue) *runtimeControlBlock {

	return &runtimeControlBlock{
		queryDriver:  driver,
		rootIter:     rootIter,
		iterStates:   make([]planIterState, numIterators),
		registers:    make([]types.FieldValue, numRegisters),
		externalVars: externalVars,
	}
}

// setState saves the specified state for the plan iterator at the specified
// position in RCB.
func (rcb *runtimeControlBlock) setState(pos int, state planIterState) {
	rcb.iterStates[pos] = state
}

// getState retrieves the plan iterator state stored at the specified position in RCB.
func (rcb *runtimeControlBlock) getState(pos int) planIterState {
	return rcb.iterStates[pos]
}

// setRegValue saves the value associated with the specified register id in RCB.
func (rcb runtimeControlBlock) setRegValue(regID int, value types.FieldValue) {
	rcb.registers[regID] = value
}

// getRegValue retrieves the value associated with the specified register id from RCB.
func (rcb *runtimeControlBlock) getRegValue(regID int) types.FieldValue {
	return rcb.registers[regID]
}

// getExternalVar retrieves the value of external variable associated with the
// specified variable id from RCB.
func (rcb *runtimeControlBlock) getExternalVar(varID int) types.FieldValue {
	return rcb.externalVars[varID]
}

// trace is used to trace the execution of query plans.
// It writes the specified message to query trace logger if the specified level
// is greater than or equal to logger's trace level.
func (rcb *runtimeControlBlock) trace(level int, messageFormat string, messageArgs ...interface{}) {
	queryLogger := rcb.getClient().queryLogger
	if queryLogger == nil || level < queryLogger.traceLevel {
		return
	}

	if rcb.sqlHashTag == nil {
		var sql string
		ps := rcb.getRequest().PreparedStatement
		if ps != nil {
			sql = ps.sqlText
		} else {
			sql = rcb.getRequest().Statement
		}
		data := md5.Sum([]byte(sql))
		// To generate a compact output, use the first 4 bytes as a tag.
		rcb.sqlHashTag = data[:4]
		queryLogger.Trace("[%x] SQL: %s", rcb.sqlHashTag, sql)
	}

	tag := fmt.Sprintf("[%x] ", rcb.sqlHashTag)
	queryLogger.Trace(tag+messageFormat, messageArgs...)
}

// openIter is a convenience method that sets the plan iterator at specified
// position to the open state.
func (rcb *runtimeControlBlock) openIter(pos int) error {
	state := open
	rcb.setState(pos, &state)
	return nil
}

// closeIter is a convenience method that sets the plan iterator at specified
// position to the closed state.
func (rcb *runtimeControlBlock) closeIter(pos int) error {
	state := rcb.getState(pos)
	if state != nil {
		return state.close()
	}
	return nil
}

// incMemoryConsumption increases the total memory consumed for the query at client by v.
// It will return an error if the total consumed memory exceeds the maximum
// memory allowed for the query at client.
func (rcb *runtimeControlBlock) incMemoryConsumption(v int64) error {
	rcb.memoryConsumption += v
	if max := rcb.getRequest().GetMaxMemoryConsumption(); rcb.memoryConsumption > max {
		return nosqlerr.NewIllegalState("the consumed memory %d bytes at client "+
			"has exceeded the maximum of %d bytes for the query",
			rcb.memoryConsumption, max)
	}

	return nil
}

// decMemoryConsumption decreases the total memory consumed for the query at client by v.
func (rcb *runtimeControlBlock) decMemoryConsumption(v int64) {
	rcb.memoryConsumption -= v
}

// addConsumedCapacity adds the specified capacity (readUnits/readKB/writeKB)
// into the total capacity consumed for the query.
func (rcb *runtimeControlBlock) addConsumedCapacity(c Capacity) {
	rcb.ReadKB += c.ReadKB
	rcb.ReadUnits += c.ReadUnits
	rcb.WriteKB += c.WriteKB
}

// resetConsumedCapacity resets the consumed capacity to zero.
func (rcb *runtimeControlBlock) resetConsumedCapacity() {
	rcb.ReadKB = 0
	rcb.ReadUnits = 0
	rcb.WriteKB = 0
}

const (
	// The pointer size.
	ptrSize = 4 << (^uintptr(0) >> 63)

	// Size of the map header, that is the hmap struct, see runtime/map.go.
	hmapSize = 8 + 5*ptrSize
)

// sizeOf reports the memory in bytes that is consumed by the specified value.
//
// The returned size may be inaccurate, especially when the underlying data
// structure is a map. This is used to estimate how much memory is consumed by
// queries that involve sorting and/or duplicate elimination.
func sizeOf(v interface{}) int {
	return dataSize(reflect.ValueOf(v))
}

func dataSize(v reflect.Value, ignoreTypeSize ...bool) int {
	sz := int(v.Type().Size())
	if len(ignoreTypeSize) > 0 && ignoreTypeSize[0] {
		sz = 0
	}

	switch v.Kind() {
	case reflect.Ptr:
		if !v.IsNil() {
			sz += dataSize(v.Elem())
		}
		return sz

	case reflect.String:
		return sz + v.Len()

	case reflect.Slice:
		n := v.Len()
		for i := 0; i < n; i++ {
			sz += dataSize(v.Index(i))
		}

		// Account for the memory allocated for the backing array but are
		// not referenced by the slice.
		if c := v.Cap(); c > n {
			sz += int(v.Type().Elem().Size()) * (c - n)
		}
		return sz

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			switch f.Kind() {
			case reflect.Ptr, reflect.Map, reflect.Slice, reflect.String:
				// Ignore the the pointer/slice header/string header size as
				// it is already counted as part of the struct size.
				sz += dataSize(f, true)
			}
		}
		return sz

	case reflect.Map:
		sz += int(hmapSize)
		for _, key := range v.MapKeys() {
			sz += dataSize(key)
			sz += dataSize(v.MapIndex(key))
		}

		// There is memory overhead for the buckets, possibly overflow buckets
		// and other internal meta data maintained for the hashtable, which is
		// not easy to calculate accurately. For simplicity, do not account for them.
		return sz

	default:
		return sz
	}
}

// topologyInfo represents the NoSQL database topology information required for
// query execution.
type topologyInfo struct {
	// seqNum represents the sequence number of the topology.
	seqNum int

	// shardIDs specifies a slice of int values that represent the shard IDs.
	shardIDs []int
}

// equals checks if ti equals to the specified otherTopo.
func (ti *topologyInfo) equals(otherTopo *topologyInfo) bool {
	if ti == otherTopo {
		return true
	}

	if ti == nil || otherTopo == nil {
		return ti == otherTopo
	}

	if ti.seqNum != otherTopo.seqNum {
		return false
	}

	if ti.getNumShards() != otherTopo.getNumShards() {
		return false
	}

	// Sort the slice of shard IDs and compare.
	sort.Ints(ti.shardIDs)
	sort.Ints(otherTopo.shardIDs)
	return reflect.DeepEqual(ti.shardIDs, otherTopo.shardIDs)
}

// getNumShards returns the number of shards.
func (ti *topologyInfo) getNumShards() int {
	return len(ti.shardIDs)
}

// getShardID returns the shard id associated with the specified index.
func (ti *topologyInfo) getShardID(index int) int {
	return ti.shardIDs[index]
}

// dummyContKey is a dummy value of the continuation key.
var dummyContKey = []byte{0}

// queryDriver drives the execution of "advanced" queries at the client and
// contains all the dynamic state needed for this execution. The state is
// preserved across the query requests submitted by the application (i.e., across batches).
type queryDriver struct {
	// The NoSQL database client that executes the query.
	client *Client

	// The query request this query driver represents for.
	request *QueryRequest

	// The continuation key of the query request.
	continuationKey []byte

	// The NoSQL database topology information required for query execution.
	topologyInfo *topologyInfo

	// The compilation cost consumed for preparing the query statement.
	prepareCost int

	// The RuntimeControlBlock used to store state of an executing query plan.
	rcb *runtimeControlBlock

	// batchSize specifies the maximum number of results the application will
	// receive per Client.Query() invocation.
	batchSize int

	// results specifies a slice of MapValue that contains the query results.
	results []*types.MapValue

	// err represents a non-retriable error returned by a query batch.
	err error
}

// newQueryDriver creates a query driver for the specified query request.
func newQueryDriver(req *QueryRequest) *queryDriver {
	batchSize := proto.DefaultBatchQueryNumberLimit
	if req.Limit > 0 {
		batchSize = int(req.Limit)
	}

	req.driver = &queryDriver{
		request:   req,
		batchSize: batchSize,
	}

	return req.driver
}

// getClient returns the NoSQL database client that executes the query request.
func (d *queryDriver) getClient() *Client {
	return d.client
}

// getRequest returns the query request with which this driver is associated.
func (d *queryDriver) getRequest() *QueryRequest {
	return d.request
}

// getTopologyInfo returns the NoSQL database topology information that is
// required for query execution.
func (d *queryDriver) getTopologyInfo() *topologyInfo {
	return d.topologyInfo
}

// setQueryResult sets the query results cached in the query driver for the
// specified QueryResult instance.
func (d *queryDriver) setQueryResult(res *QueryResult) {
	res.results = d.results
	res.continuationKey = d.continuationKey
	res.Capacity = d.rcb.Capacity

	d.results = nil
	d.rcb.resetConsumedCapacity()
}

// close terminates query execution.
func (d *queryDriver) close() {
	prepStmt := d.request.PreparedStatement
	if prepStmt != nil && prepStmt.driverQueryPlan != nil {
		prepStmt.driverQueryPlan.close(d.rcb)
	}

	d.results = nil
}

// compute computes and sets a batch of query results for the specified QueryResult.
func (d *queryDriver) compute(res *QueryResult) (err error) {
	prepStmt := d.request.PreparedStatement
	if prepStmt.isSimpleQuery() {
		return nosqlerr.NewIllegalState("this is a simple query request that does not " +
			"need to be computed at client")
	}

	if d.request.driver != d {
		return nosqlerr.NewIllegalState("the query driver is not associated with the query request")
	}

	// If there was a non-retryable error returned by a previous execution of
	// query batch, return that error to the application.
	if d.err != nil {
		return fmt.Errorf("query request cannot be continued because the previous execution "+
			"returned a non-retryable error: %v.\nPlease set the continuation key to nil "+
			"in order to execute the query from the beginning", d.err)
	}

	// The query results may be non-empty if a retryable error was returned
	// during a previous batch. In this case, the results stores the query
	// results computed before the error was returned, and in this batch we just
	// return what we have.
	if d.results != nil {
		d.setQueryResult(res)
		return nil
	}

	iter := prepStmt.driverQueryPlan
	if d.rcb == nil {
		d.rcb = newRCB(d, iter, prepStmt.numIterators, prepStmt.numRegisters, prepStmt.getBoundVarValues())
		// Adds the compilation cost consumed for preparing the query statement.
		d.rcb.ReadKB += d.prepareCost
		d.rcb.ReadUnits += d.prepareCost
		err = iter.open(d.rcb)
		if err != nil {
			return
		}
	}

	var more bool
	d.results = make([]*types.MapValue, 0, d.batchSize)
	for i := 0; i < d.batchSize; i++ {
		more, err = iter.next(d.rcb)
		if err != nil {
			e, ok := err.(*nosqlerr.Error)
			// If this is not a retryable error, save it so that we return it
			// immediately if the application resubmits the QueryRequest.
			if !ok || !e.Retryable() {
				d.err = err
				d.results = nil
				iter.close(d.rcb)
			}

			return err
		}

		if !more {
			break
		}

		res := iter.getResult(d.rcb)
		mv, ok := res.(*types.MapValue)
		if !ok {
			return nosqlerr.NewIllegalState("the query result is expected to be a *types.MapValue, got %T", res)
		}

		d.results = append(d.results, mv)
	}

	if more {
		// If the query has reached the batch size limit but there are more
		// results available, set a dummy continuation key.
		d.continuationKey = dummyContKey
	} else {
		if d.rcb.reachedLimit {
			d.continuationKey = dummyContKey
			d.rcb.reachedLimit = false
		} else {
			state := iter.getState(d.rcb)
			if state == nil || !state.isDone() {
				return nosqlerr.NewIllegalState("the query execution terminates " +
					"but the plan iterator is not in DONE state")
			}

			d.continuationKey = nil
		}
	}

	d.setQueryResult(res)
	d.request.setContKey(d.continuationKey)

	return nil
}
