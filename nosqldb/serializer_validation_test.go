//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"testing"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/stretchr/testify/assert"
)

func TestReadPackedIntArrayRejectsLargeCount(t *testing.T) {
	w := binary.NewWriter()
	_, err := w.WritePackedInt(maxStructuralCount + 1)
	assert.NoError(t, err)

	r := binary.NewReader(bytes.NewBuffer(w.Bytes()))
	_, err = readPackedIntArray(r)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "invalid number of int array elements")
	}
}

func TestReadNsonCollectionCountRejectsLengthPastRemaining(t *testing.T) {
	w := binary.NewWriter()
	_, err := w.WriteInt(5)
	assert.NoError(t, err)
	_, err = w.WriteInt(1)
	assert.NoError(t, err)

	r := binary.NewReader(bytes.NewBuffer(w.Bytes()))
	_, err = readNsonCollectionCount(r, "array")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "exceeds remaining bytes")
	}
}

func TestNewRCBRejectsLargeCounts(t *testing.T) {
	tests := []struct {
		name         string
		numIterators int
		numRegisters int
		want         string
	}{
		{"iterators", maxStructuralCount + 1, 1, "invalid number of iterators"},
		{"registers", 1, maxStructuralCount + 1, "invalid number of registers"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rcb, err := newRCB(nil, nil, tc.numIterators, tc.numRegisters, nil)
			assert.Nil(t, rcb)
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.want)
			}
		})
	}
}
