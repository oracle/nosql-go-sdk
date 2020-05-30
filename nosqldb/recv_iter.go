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
	"container/heap"
	"crypto/md5"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// distributionKind represents how the data being queried are distributed in the database.
type distributionKind int

const (
	// singlePartition means the query will go to a single partition and use the
	// primary index for its execution.
	// This is the case where the query specifies a complete shard key.
	singlePartition distributionKind = iota // 0

	// allPartitions means the query will go to all partitions and use the
	// primary index for its execution.
	// This is the case where the query does not specify a complete shard key.
	allPartitions // 1

	// allShards means the query will go to all shards.
	// This is the case where the query uses a secondary index for its execution.
	allShards // 2
)

// String returns the string representation of distributionKind.
//
// This implements the fmt.Stringer interface.
func (k distributionKind) String() string {
	switch k {
	case singlePartition:
		return "SINGLE_PARTITION"
	case allPartitions:
		return "ALL_PARTITIONS"
	case allShards:
		return "ALL_SHARDS"
	default:
		return ""
	}
}

var _ planIter = (*receiveIter)(nil)

type chksum [md5.Size]byte

// receiveIterState represents the dynamic state for a receive iterator.
type receiveIterState struct {
	*iterState

	// topologyInfo represents the NoSQL database topology information required
	// for query execution.
	//
	// This is used for sorting all-shard queries only.
	topologyInfo *topologyInfo

	// scanner represents the remote scanner used for non-sorting queries.
	scanner *remoteScanner

	// sortedScanners represents the remote scanners used for sorting queries.
	//
	// For all-shard queries there is one remote scanner per shard.
	// For all-partition queries a remote scanner is created for each partition
	// that has at least one result.
	sortedScanners remoteScannerHeap

	// isInSortPhase1 indicates if the query execution is in sort phase 1 where
	// the client caches at least one result per partition, except from
	// partitions that do not contain any results at all.
	//
	// This is used for sorting all-partition queries.
	isInSortPhase1 bool

	// continuationKey specifies the continuation key to be used for the next
	// batch request during sort-phase-1 of a sorting, all-partition query.
	continuationKey []byte

	// primKeys is a fast lookup table that stores MD5 checksum of the binary
	// representation of primary keys of all the results seen so far.
	//
	// This is used for duplicate elimination.
	primKeys map[chksum]struct{}

	// memoryConsumption represents the memory consumed by this receiveIter.
	// Memory consumption is counted for sorting all-partiton queries and/or
	// queries that do duplicate elimination.
	//
	// The memory taken by results cached in sortedScanners and/or primary keys
	// stored in primKeys are counted.
	memoryConsumption int64

	// dupElimMemory represents the memory consumed for duplicate elimination.
	dupElimMemory int64

	// totalResultSize and totalNumResults represent the total size in bytes
	// and number of results fetched by this receiveIter so far respectively.
	// They are used to compute the average result size, which is then used to
	// compute the max number of results to fetch from a partition during a
	// sort-phase-2 request for a sorting, all-partition query.
	totalResultSize int64
	totalNumResults int64
}

func newReceiveIterState(rcb *runtimeControlBlock, iter *receiveIter) *receiveIterState {
	topoInfo := rcb.getTopologyInfo()
	state := &receiveIterState{
		iterState:      newIterState(),
		isInSortPhase1: true,
		topologyInfo:   topoInfo,
	}

	if iter.doesDupElim() {
		state.primKeys = make(map[chksum]struct{}, 100)
		// Account for the memory allocated for the map used for duplicate elimination.
		state.dupElimMemory = int64(sizeOf(state.primKeys))
		state.memoryConsumption = state.dupElimMemory
	}

	switch {
	case !iter.doesSort() || iter.distKind == singlePartition:
		state.scanner = newRemoteScanner(rcb, state, false, -1, iter)

	case iter.distKind == allPartitions:
		scanners := make([]*remoteScanner, 0, 1000)
		state.sortedScanners = remoteScannerHeap(scanners)

	case iter.distKind == allShards:
		numShards := topoInfo.getNumShards()
		scanners := make([]*remoteScanner, numShards)
		for i := 0; i < numShards; i++ {
			scanners[i] = newRemoteScanner(rcb, state, true, topoInfo.getShardID(i), iter)
		}
		state.sortedScanners = remoteScannerHeap(scanners)
	}

	return state
}

func (st *receiveIterState) done() (err error) {
	if err = st.iterState.done(); err != nil {
		return
	}

	st.primKeys = nil
	st.sortedScanners = nil
	return nil
}

func (st *receiveIterState) close() (err error) {
	if err = st.iterState.close(); err != nil {
		return
	}

	st.primKeys = nil
	st.sortedScanners = nil
	return nil
}

// receiveIter requests and receives results from the Oracle NoSQL database proxy.
//
// For sorting queries, it performs a merge sort of the received results.
// It also performs duplicate elimination for queries that require it.
// Note that a query can do both sorting and duplicate elimination.
type receiveIter struct {
	*planIterDelegate

	// distKind represents the distribution kind of the query.
	distKind distributionKind

	// sortFields specifies the names of top-level fields that contain the
	// values on which to sort the received results.
	//
	// This is used for sorting queries.
	sortFields []string

	// sortSpecs represents the corresponding sorting specs of the fields
	// specified in sortFields.
	//
	// This is used for sorting queries.
	sortSpecs []*sortSpec

	// primKeyFields specifies the names of top-level fields that contain the
	// primary key values within the received results.
	//
	// This is used for duplicate elimination.
	primKeyFields []string
}

func newReceiveIter(r proto.Reader) (iter *receiveIter, err error) {
	delegate, err := newPlanIterDelegate(r, recv)
	if err != nil {
		return
	}

	kind, err := r.ReadInt16()
	if err != nil {
		return
	}

	sortFields, err := readStringArray(r)
	if err != nil {
		return
	}

	sortSpecs, err := readSortSpecs(r)
	if err != nil {
		return
	}

	primKeyFields, err := readStringArray(r)
	if err != nil {
		return
	}

	iter = &receiveIter{
		planIterDelegate: delegate,
		distKind:         distributionKind(int(kind)),
		sortFields:       sortFields,
		sortSpecs:        sortSpecs,
		primKeyFields:    primKeyFields,
	}
	return
}

// doesSort returns true if the receive iterator is used for sorting query,
// otherwise returns false.
func (iter *receiveIter) doesSort() bool {
	return iter.sortFields != nil
}

// doesDupElim returns true if duplicate elimination is required for the query,
// otherwise returns false.
func (iter *receiveIter) doesDupElim() bool {
	return iter.primKeyFields != nil
}

func (iter *receiveIter) open(rcb *runtimeControlBlock) (err error) {
	state := newReceiveIterState(rcb, iter)
	rcb.setState(iter.statePos, state)
	err = rcb.incMemoryConsumption(state.memoryConsumption)
	return
}

// reset resets the state for receive iterator.
//
// This is implemented to satisfy the planIter interface.
// This method should not be called on a receiveIter.
func (iter *receiveIter) reset(rcb *runtimeControlBlock) (err error) {
	return nosqlerr.NewIllegalState("the receive iterator cannot be reset")
}

func (iter *receiveIter) next(rcb *runtimeControlBlock) (bool, error) {
	st := rcb.getState(iter.statePos)
	state, ok := st.(*receiveIterState)
	if !ok {
		return false, fmt.Errorf("wrong iterator state type, expect *receiveIterState, got %T", st)
	}

	if state.isDone() {
		rcb.trace(1, "receiveIter.next() : done")
		return false, nil
	}

	if iter.doesSort() {
		return iter.sortingNext(rcb, state)
	}

	return iter.simpleNext(rcb, state)
}

// simpleNext retrieves the next result from receiveIter for non-sorting queries.
func (iter *receiveIter) simpleNext(rcb *runtimeControlBlock, state *receiveIterState) (more bool, err error) {
	var res *types.MapValue
	var isDup bool

	for {
		res, err = state.scanner.next()
		if err != nil {
			return false, err
		}

		if res == nil {
			break
		}

		rcb.trace(1, "receiveIter.simpleNext() : got result %v", res)

		isDup, err = iter.checkDuplicate(rcb, state, res)
		if err != nil {
			return false, err
		}

		if isDup {
			continue
		}

		rcb.setRegValue(iter.resultReg, res)
		return true, nil
	}

	rcb.trace(1, "receiveIter.simpleNext() : no result. Reached limit = %t", rcb.reachedLimit)

	if rcb.reachedLimit {
		return false, nil
	}

	err = state.done()
	return false, err
}

// sortingNext retrieves the next result from receiveIter for sorting queries.
func (iter *receiveIter) sortingNext(rcb *runtimeControlBlock, state *receiveIterState) (more bool, err error) {
	if iter.distKind == allPartitions && state.isInSortPhase1 {
		err = iter.initPartitionSort(rcb, state)
		return false, err
	}

	var scanner *remoteScanner
	var res *types.MapValue
	var isDup bool

	if state.sortedScanners.Len() > 0 {
		heap.Init(&state.sortedScanners)
	}

	for {
		if state.sortedScanners.Len() == 0 {
			err = state.done()
			return false, err
		}

		// Process results from the first (highest priority) scanner.
		scanner = state.sortedScanners[0]
		if scanner == nil {
			err = state.done()
			return false, err
		}

		res = scanner.nextLocal()
		if res != nil {
			rcb.trace(1, "receiveIter.sortingNext() : got result : %#v", res)

			convertEmptyToNull(res)
			rcb.setRegValue(iter.resultReg, res)

			if scanner.isDone() {
				// Remove the scanner if it has done.
				// This is equivalent to heap.Remove(&state.sortedScanners, 0).
				heap.Pop(&state.sortedScanners)
				rcb.trace(1, "receiveIter.sortingNext() : done with partition/shard %d", scanner.shardOrPartID)
			} else {
				// Keep the scanner in the heap, but need to re-establish the
				// heap ordering because the state of sorted scanner has changed.
				heap.Fix(&state.sortedScanners, 0)
			}

			isDup, err = iter.checkDuplicate(rcb, state, res)
			if err != nil {
				return false, err
			}

			if isDup {
				continue
			}

			return true, nil
		}

		// This scanner has done, remove it and continue with the next scanner.
		if scanner.isDone() {
			heap.Pop(&state.sortedScanners)
			continue
		}

		// The scanner has no cached results but it is not done, indicating it
		// has remote results, so send a request to fetch more results.
		err = scanner.fetch()
		if err != nil {
			// If this is a retryable error, keep the scanner in the heap so as
			// to process results from this scanner later, otherwise remove it.
			e, ok := err.(*nosqlerr.Error)
			if !ok || !e.Retryable() {
				heap.Pop(&state.sortedScanners)
			}

			return false, err
		}

		// Re-check after a remote fetch.
		// If there are no remote results available, remove the scanner,
		// otherwise re-establish the heap ordering.
		if scanner.isDone() {
			heap.Pop(&state.sortedScanners)
			rcb.trace(1, "receiveIter.sortingNext() : done with "+
				"partition/shard %d", scanner.shardOrPartID)
		} else {
			heap.Fix(&state.sortedScanners, 0)
		}

		iter.handleTopologyChange(rcb, state)

		// For simplicity, we do not want to allow the possibility of another
		// remote fetch during the same batch, so whether or not the batch
		// limit was reached during the above fetch, we set reachedLimit to true
		// and return false for this method, thus terminating the current batch.
		rcb.reachedLimit = true
		return false, nil
	}
}

// convertEmptyToNull converts EmptyValue to NullValue for the entries in the
// specified MapValue.
func convertEmptyToNull(m *types.MapValue) {
	for k, v := range m.Map() {
		if _, ok := v.(*types.EmptyValue); ok {
			m.Map()[k] = types.NullValueInstance
		}
	}
}

// checkDuplicate returns true if the received result is a duplicate of an
// existing one, returns false otherwise.
func (iter *receiveIter) checkDuplicate(rcb *runtimeControlBlock, state *receiveIterState, res *types.MapValue) (bool, error) {
	if state.primKeys == nil {
		return false, nil
	}

	data, err := iter.createBinaryPrimKey(res)
	if err != nil {
		return false, err
	}

	checksum := md5.Sum(data)
	if _, ok := state.primKeys[checksum]; ok {
		rcb.trace(1, "receiveIter.checkDuplicate() : result is duplicate")
		return true, nil
	}

	state.primKeys[checksum] = struct{}{}
	// Calculate the memory consumed for duplicate elimination.
	// Only count the map's key, that is the MD5 checksum,
	// the map's value is not counted as it is an empty struct that takes 0 byte.
	memory := int64(sizeOf(checksum))
	state.memoryConsumption += memory
	state.dupElimMemory += memory
	err = rcb.incMemoryConsumption(memory)
	return false, err
}

// createBinaryPrimKey serializes the primary key values as byte sequences.
// This is used for duplicate elimination.
func (iter *receiveIter) createBinaryPrimKey(res *types.MapValue) ([]byte, error) {
	w := binary.NewWriter()
	var err error

	for i, fieldName := range iter.primKeyFields {
		v, ok := res.Get(fieldName)
		if !ok {
			return nil, fmt.Errorf("receiveIter.createBinaryPrimKey(): cannot find primary key field %s", fieldName)
		}

		switch v := v.(type) {
		case int:
			_, err = w.WritePackedInt(v)
		case int64:
			_, err = w.WritePackedLong(v)
		case float64:
			_, err = w.WriteDouble(v)
		case *big.Rat:
			strNum := v.RatString()
			_, err = w.WriteString(&strNum)
		case string:
			_, err = w.WriteString(&v)
		case time.Time:
			s := v.UTC().Format(time.RFC3339Nano)
			_, err = w.WriteString(&s)
		default:
			return nil, fmt.Errorf("unexpected type for primary key column %s, at result column %d: %T"+
				fieldName, i+1, v)
		}

		if err != nil {
			return nil, err
		}
	}

	return w.Bytes(), nil
}

// initPartitionSort tries to receive and cache at least one result per partition
// except from partitions that do not contain any results at all.
//
// This is used for sort-phase-1 all-partition sorting queries.
func (iter *receiveIter) initPartitionSort(rcb *runtimeControlBlock, state *receiveIterState) error {
	if !state.isInSortPhase1 {
		return nil
	}

	// Create and execute a request to get at least one result from the partition
	// whose id is specified in the continuationKey and from any other partition
	// that is co-located with that partition.
	queryReq := rcb.getRequest().copyInternal()
	queryReq.setContKey(state.continuationKey)

	rcb.trace(1, "receiveIter.initPartitionSort() : executing remote request for sorting phase 1")

	queryRes, err := rcb.getClient().Query(queryReq)
	if err != nil {
		return err
	}

	state.isInSortPhase1 = queryRes.isInPhase1
	state.continuationKey, err = queryRes.getContinuationKey()
	if err != nil {
		return err
	}

	capacity, err := queryRes.ConsumedCapacity()
	if err != nil {
		return err
	}

	rcb.addConsumedCapacity(capacity)
	rcb.trace(1, "receiveIter.initPartitionSort() : got result : "+
		"reached limit = %t, in phase 1 = %t", queryRes.reachedLimit, queryRes.isInPhase1)

	// For each partition P that was accessed during the execution of the above
	// query, collect the results for P and create a scanner that will be used
	// during phase 2 to collect further results from P only.
	off := 0
	for i, partID := range queryRes.partitionIDs {
		n := queryRes.numResultsPerPart[i]
		contKey := queryRes.contKeysPerPart[i]
		partitionResults := queryRes.results[off : off+n]
		off += n
		scanner := newRemoteScanner(rcb, state, false, partID, iter)
		scanner.addResults(partitionResults, contKey)
		heap.Push(&state.sortedScanners, scanner)
	}

	rcb.trace(1, "receiveIter.initPartitionSort() : memory consumption = %d", state.memoryConsumption)

	// For simplicity, if the size limit was not reached during this batch of
	// sort phase 1, we don't start a new batch. We let the application do it.
	// Furthermore, this means that each remote fetch will be done with the max
	// amount of read limit, which will reduce the total number of fetches.
	rcb.reachedLimit = true
	return nil
}

// handleTopologyChange checks if the new topology differs from the current
// topology received earlier. If so, updates the topology.
func (iter *receiveIter) handleTopologyChange(rcb *runtimeControlBlock, state *receiveIterState) {
	newTopoInfo := rcb.getTopologyInfo()
	if iter.distKind == allPartitions || newTopoInfo.equals(state.topologyInfo) {
		return
	}

	// As a side effect of the "newTopoInfo.equals(state.topologyInfo)" invocation,
	// the shard IDs have been sorted.
	newShards := newTopoInfo.shardIDs
	currShards := state.topologyInfo.shardIDs

	var i int
	n := len(currShards)
	// shardIDs stores current shard IDs that exists in the new topology.
	shardIDs := make(map[int]bool, n)
	for _, newShardID := range newShards {
		i = sort.SearchInts(currShards, newShardID)
		// This is a new shard.
		if i >= n || currShards[i] != newShardID {
			scanner := newRemoteScanner(rcb, state, true, newShardID, iter)
			state.sortedScanners = append(state.sortedScanners, scanner)
		} else {
			shardIDs[currShards[i]] = true
		}
	}

	// Re-build the heap.
	heap.Init(&state.sortedScanners)

	for _, currShardID := range currShards {
		if shardIDs[currShardID] {
			continue
		}

		// This shard does not exist any more.
		for j, scanner := range state.sortedScanners {
			if scanner.shardOrPartID == currShardID {
				heap.Remove(&state.sortedScanners, j)
				break
			}
		}
	}

	state.topologyInfo = newTopoInfo
}

func (iter *receiveIter) getPlan() string {
	return iter.planIterDelegate.getExecPlan(iter)
}

func (iter *receiveIter) displayContent(sb *strings.Builder, f *planFormatter) {
	f.printIndent(sb)
	sb.WriteString("DistributionKind : ")
	sb.WriteString(iter.distKind.String())
	sb.WriteString(",\n")

	if iter.sortFields != nil {
		f.printIndent(sb)
		sb.WriteString("Sort Fields : ")
		for i := 0; i < len(iter.sortFields); i++ {
			sb.WriteString(iter.sortFields[i])
			if i < len(iter.sortFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(",\n")
	}

	if iter.primKeyFields != nil {
		f.printIndent(sb)
		sb.WriteString("Primary Key Fields : ")
		for i, pkFieldName := range iter.primKeyFields {
			sb.WriteString(pkFieldName)
			if i < len(iter.primKeyFields)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(",\n")
	}
}

// remoteScanner fetches results from the Oracle NoSQL database server.
//
// For all-shard, ordering queries, there is one remoteScanner per shard.
// In this case, each remoteScanner will fetch results only from the shard
// specified by shardOrPartID.
//
// For all-partition, ordering queries, there is one remoteScanner for each
// partition that has at least one query result.
// In this case, each remoteScanner will fetch results only from the partition
// specified by shardOrPartID.
//
// For non-ordering queries, there is a single remoteScanner.
// It will fetch as many results as possible starting from the shard or
// partition specified in continuationKey. It may fetch results from more than
// one shard or partition.
type remoteScanner struct {
	rcb               *runtimeControlBlock
	state             *receiveIterState
	isForShard        bool
	shardOrPartID     int
	results           []*types.MapValue
	resultSize        int64
	nextResultPos     int
	continuationKey   []byte
	moreRemoteResults bool

	*receiveIter
}

func newRemoteScanner(rcb *runtimeControlBlock, state *receiveIterState,
	isForShard bool, shardOrPartID int, iter *receiveIter) *remoteScanner {

	return &remoteScanner{
		rcb:               rcb,
		state:             state,
		isForShard:        isForShard,
		shardOrPartID:     shardOrPartID,
		moreRemoteResults: true,
		receiveIter:       iter,
	}
}

// isDone checks if the remote scanner is done.
// It returns true if it has no results cached locally and no remote results
// available, returns false otherwise.
func (s *remoteScanner) isDone() bool {
	return !s.moreRemoteResults && (s.results == nil || s.nextResultPos >= len(s.results))
}

// hasLocalResults checks if the remote scanner has local results that are not processed.
func (s *remoteScanner) hasLocalResults() bool {
	return s.results != nil && s.nextResultPos < len(s.results)
}

// addResults sets the specified results and continuation key to the scanner for
// further processing.
func (s *remoteScanner) addResults(results []*types.MapValue, contKey []byte) {
	s.results = results
	s.continuationKey = contKey
	s.moreRemoteResults = contKey != nil
	s.addMemoryConsumption()
}

// nextLocal returns the next local result or nil if there are no local results.
func (s *remoteScanner) nextLocal() *types.MapValue {
	if s.results != nil && s.nextResultPos < len(s.results) {
		m := s.results[s.nextResultPos]
		s.nextResultPos++
		return m
	}

	return nil
}

// next returns the next result from the scanner.
// It returns the next local result if available, or if there are more remote
// results available tries to fetch them.
func (s *remoteScanner) next() (mv *types.MapValue, err error) {
	mv = s.nextLocal()
	if mv != nil {
		return mv, nil
	}

	s.results = nil
	s.nextResultPos = 0

	if !s.moreRemoteResults || s.rcb.reachedLimit {
		return nil, nil
	}

	err = s.fetch()
	if err != nil {
		return nil, err
	}

	if len(s.results) == 0 {
		return nil, nil
	}

	mv = s.results[s.nextResultPos]
	s.nextResultPos++
	return mv, nil
}

// The maximum number of results allowed for each fetch operation.
const maxNumResults int64 = 2048

// fetch fetches results remotely from the NoSQL database servers.
func (s *remoteScanner) fetch() (err error) {
	req := s.rcb.getRequest().copyInternal()
	req.setContKey(s.continuationKey)
	if s.isForShard {
		req.setShardID(s.shardOrPartID)
	} else {
		req.setShardID(-1)
	}

	if s.doesSort() && !s.isForShard {
		s.state.memoryConsumption -= s.resultSize
		s.rcb.decMemoryConsumption(s.resultSize)
		numResults := (req.MaxMemoryConsumption - s.state.dupElimMemory) /
			(int64((len(s.state.sortedScanners) + 1)) * (s.state.totalResultSize / s.state.totalNumResults))
		if numResults > maxNumResults {
			numResults = maxNumResults
		}

		req.Limit = uint(numResults)
	}

	s.rcb.trace(1, "remoteScanner : executing remote request. shardOrPartID = %d", s.shardOrPartID)

	res, err := s.rcb.getClient().Query(req)
	if err != nil {
		return
	}

	s.results = res.results
	s.continuationKey, err = res.getContinuationKey()
	if err != nil {
		return
	}

	s.nextResultPos = 0
	s.moreRemoteResults = s.continuationKey != nil

	capacity, err := res.ConsumedCapacity()
	if err != nil {
		return
	}

	s.rcb.addConsumedCapacity(capacity)

	// For simplicity, if the query is a sorting one, we consider the current
	// batch done as soon as we get the response back from the NoSQL database
	// servers, even if the batch limit was not reached there.
	if res.reachedLimit || s.doesSort() {
		s.rcb.reachedLimit = true
	}

	if s.doesSort() && !s.isForShard {
		err = s.addMemoryConsumption()
	}

	s.rcb.trace(1, "remoteScanner: got %d remote results, hasMoreRemoteResults=%t, reachedLimit=%t, readKB=%d, "+
		"readUnits=%d, writeKB=%d, memory consumption=%d", len(s.results), s.moreRemoteResults, res.reachedLimit, res.ReadKB,
		res.ReadUnits, res.WriteKB, s.state.memoryConsumption)

	return
}

// addMemoryConsumption adds the memory consumed for caching results locally to
// the total memory consumed for current query batch at client.
func (s *remoteScanner) addMemoryConsumption() (err error) {
	s.resultSize = 0
	n := len(s.results)
	for i := 0; i < n; i++ {
		s.resultSize += int64(sizeOf(s.results[i]))
	}

	s.state.totalNumResults += int64(n)
	s.state.totalResultSize += s.resultSize
	s.state.memoryConsumption += s.resultSize
	err = s.rcb.incMemoryConsumption(s.resultSize)
	return
}

// remoteScannerHeap represents a slice of remoteScanners.
// It implements the heap.Interface, can be used as a min-heap.
type remoteScannerHeap []*remoteScanner

// Len returns the number of remoteScanners.
func (p remoteScannerHeap) Len() int {
	return len(p)
}

// Swap swaps the remoteScanners with index i and j from the slice.
func (p remoteScannerHeap) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Less reports whether the remoteScanner with index i should sort before the one with index j.
func (p remoteScannerHeap) Less(i, j int) bool {
	s1 := p[i]
	s2 := p[j]

	if !s1.hasLocalResults() {
		if !s2.hasLocalResults() {
			return s1.shardOrPartID < s2.shardOrPartID
		}

		return true
	}

	if !s2.hasLocalResults() {
		return false
	}

	v1 := s1.results[s1.nextResultPos]
	v2 := s2.results[s2.nextResultPos]

	cmp := &resultsBySortSpec{
		results:    []*types.MapValue{v1, v2},
		sortFields: s1.sortFields,
		sortSpecs:  s1.sortSpecs,
	}

	return cmp.Less(0, 1)
}

// Push adds the specified v (must be of type *remoteScanner) to the slice.
//
// To add a new remote scanner to the heap, use heap.Push method in the heap package.
func (p *remoteScannerHeap) Push(v interface{}) {
	*p = append(*p, v.(*remoteScanner))
}

// Pop removes and returns the last remote scanner with index Len()-1 from the slice.
//
// To remove a remote scanner from the heap, use heap.Pop method in the heap package.
func (p *remoteScannerHeap) Pop() interface{} {
	old := *p
	n := len(old)
	v := old[n-1]
	*p = old[0 : n-1]
	return v
}
