//
// Copyright (c) 2019, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package common

import (
	"reflect"
	"sort"
	"time"
)

// InternalRequestDataInt is used to give all requests a
// set of common internal data (rate limiters, retry stats, etc)
type InternalRequestDataInt interface {
	RateLimiterPairInt
	GetRetryTime() time.Duration
	SetRetryTime(d time.Duration)
	SetTopology(ti *TopologyInfo)
	GetTopoSeqNum() int
}

// InternalRequestData is the actual struct that gets included
// in every Request type
type InternalRequestData struct {
	RateLimiterPair
	retryTime time.Duration
	topology  *TopologyInfo
}

// GetRetryTime returns the current time spent in the client in retries
func (ird *InternalRequestData) GetRetryTime() time.Duration {
	return ird.retryTime
}

// SetRetryTime sets the current time spent in the client in retries
func (ird *InternalRequestData) SetRetryTime(d time.Duration) {
	ird.retryTime = d
}

// SetTopologyInfo sets the topology info used for the query
func (ird *InternalRequestData) SetTopology(ti *TopologyInfo) {
	ird.topology = ti
}

// GetTopologyInfo returns the entire topology info
func (ird *InternalRequestData) GetTopologyInfo() *TopologyInfo {
	return ird.topology
}

func (ird *InternalRequestData) GetTopoSeqNum() int {
	if ird.topology == nil {
		return -1
	}
	return ird.topology.SeqNum
}

// InternalResultDataInt is used to give all requests a
// set of common internal data (rate limiters, retry stats, etc)
type InternalResultDataInt interface {
	SetTopology(ti *TopologyInfo)
	GetTopologyInfo() *TopologyInfo
	GetTopoSeqNum() int
}

// InternalResultData is the actual struct that gets included
// in every Result type
type InternalResultData struct {
	topology *TopologyInfo
}

// SetTopologyInfo sets the topology info used for the query
func (ird *InternalResultData) SetTopology(ti *TopologyInfo) {
	if ti != nil {
		ird.topology = ti
	}
}

// SetTopologyOrErr sets the topology info used for the query, or returns err
// if that was set
func (ird *InternalResultData) SetTopologyOrErr(ti *TopologyInfo, err error) error {
	if err != nil {
		return err
	}
	if ti != nil {
		ird.topology = ti
	}
	return nil
}

// GetTopologyInfo returns the entire topology info
func (ird *InternalResultData) GetTopologyInfo() *TopologyInfo {
	return ird.topology
}

// TopologyInfo represents the NoSQL database topology information required for execution.
type TopologyInfo struct {
	// seqNum represents the sequence number of the topology.
	SeqNum int

	// shardIDs specifies a slice of int values that represent the shard IDs.
	ShardIDs []int
}

// equals checks if ti equals to the specified otherTopo.
func (ti *TopologyInfo) Equals(otherTopo *TopologyInfo) bool {
	if ti == otherTopo {
		return true
	}

	if ti == nil || otherTopo == nil {
		return ti == otherTopo
	}

	if ti.SeqNum != otherTopo.SeqNum {
		return false
	}

	if ti.GetNumShards() != otherTopo.GetNumShards() {
		return false
	}

	// Sort the slice of shard IDs and compare.
	sort.Ints(ti.ShardIDs)
	sort.Ints(otherTopo.ShardIDs)
	return reflect.DeepEqual(ti.ShardIDs, otherTopo.ShardIDs)
}

// GetTopologyInfo returns the entire topology info
func (ti *TopologyInfo) GetTopologyInfo() *TopologyInfo {
	return ti
}

// GetNumShards returns the number of shards.
func (ti *TopologyInfo) GetNumShards() int {
	if ti == nil {
		return 0
	}
	return len(ti.ShardIDs)
}

// GetShardID returns the shard id associated with the specified index.
func (ti *TopologyInfo) GetShardID(index int) int {
	if ti == nil || len(ti.ShardIDs) == 0 {
		return -1
	}
	return ti.ShardIDs[index]
}

// GetLastShardID returns the last shard in the ShardIDs array
func (ti *TopologyInfo) GetLastShardID() int {
	if ti == nil || len(ti.ShardIDs) == 0 {
		return -1
	}
	return ti.ShardIDs[len(ti.ShardIDs)-1]
}
