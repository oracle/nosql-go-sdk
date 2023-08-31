//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/common"
	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

type serializer interface {
	// NSON serialization: default
	serialize(w proto.Writer, serialVersion int16) error
	deserialize(r proto.Reader, serialVersion int16) (Result, int, error)
	// V3/2 serialization: previous
	serializeV3(w proto.Writer, serialVersion int16) error
	deserializeV3(r proto.Reader, serialVersion int16) (Result, error)
}

// Request is an interface that defines common functions for operation requests.
type Request interface {
	serializer
	getTableName() string
	getNamespace() string
	validate() error
	setDefaults(cfg *RequestConfig)
	shouldRetry() bool
	timeout() time.Duration
	doesReads() bool
	doesWrites() bool
	common.InternalRequestDataInt
}

// functions common to both V3(binary) and V4(nson)

func checkRequestSizeLimit(req Request, size int) error {
	limit := proto.RequestSizeLimit
	if _, ok := req.(*WriteMultipleRequest); ok {
		limit = proto.BatchRequestSizeLimit
	}

	if size > limit {
		return nosqlerr.New(nosqlerr.RequestSizeLimitExceeded,
			"the request size of %d exceeds the limit of %d", size, limit)
	}

	return nil
}

func writeNonEmptyString(w proto.Writer, s string) (err error) {
	if s == "" {
		_, err = w.WriteString(nil)
	} else {
		_, err = w.WriteString(&s)
	}
	return
}

// timeToMs converts the specified time to the number of milliseconds since
// Unix Epoch.
func timeToMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	// Converts to milliseconds
	return t.UnixNano() / 1e6
}

func toUnixTime(timeMs int64) time.Time {
	return time.Unix(0, timeMs*int64(time.Millisecond))
}

func toDuration(seconds int64) time.Duration {
	return time.Duration(seconds * int64(time.Second))
}

func toOperationState(st byte) types.OperationState {
	return types.OperationState(st + 1)
}

func readPackedIntArray(r proto.Reader) ([]int, error) {
	n, err := r.ReadPackedInt()
	if err != nil {
		return nil, err
	}

	if n < -1 {
		return nil, nosqlerr.NewIllegalArgument("invalid length of int array: %d", n)
	}

	if n == -1 {
		return nil, nil
	}

	array := make([]int, n)
	for i := 0; i < n; i++ {
		array[i], err = r.ReadPackedInt()
		if err != nil {
			return nil, err
		}
	}

	return array, nil
}
