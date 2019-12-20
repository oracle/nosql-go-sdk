//
// Copyright (C) 2019 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package nosqldb

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

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

// getTraceLevel gets the trace level set for the query request.
func (rcb *runtimeControlBlock) getTraceLevel() int {
	return rcb.getRequest().traceLevel
}

// TODO:
// trace is used to trace the execution of query plan.
func (rcb *runtimeControlBlock) trace(level int, messageFormat string, messageArgs ...interface{}) {
	// if rcb.getTraceLevel() < level {
	// 	return
	// }

	// TODO:
	// fmt.Printf(messageFormat+"\n", messageArgs...)
	// return 0
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

var objRefOverhead int

func init() {
	objRefOverhead = 4
}

// TODO:
func sizeOf(v interface{}) int {
	switch v := v.(type) {
	case int32:
		return 4
	case int64:
		return 8
	case int:
		return 4
	case []byte:
		return len(v)
	case string:
		return len(v)
	default:
		return 0
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
