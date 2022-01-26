//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"testing"
)

const (
	v119     = 119
	v120     = 120
	max1     = 0xFF
	max2     = 0xFFFF
	max3     = 0xFFFFFF
	max4     = 0xFFFFFFFF
	max5     = 0xFFFFFFFFFF
	max6     = 0xFFFFFFFFFFFF
	max7     = 0xFFFFFFFFFFFFFF
	minInt32 = -1 << 31
	maxInt32 = 1<<31 - 1
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)

func TestPackedInt32(t *testing.T) {
	tests := []struct {
		start, end int32
		want       uint // expected number of bytes to be written
	}{
		// 1 byte
		{-v119, v120, 1},
		// 2 bytes
		{-max1 - v119 - 1, -v119 - 1, 2},
		{v120 + 1, max1 + v120 + 1, 2},
		// 3 bytes
		{-max2 - v119 - 1, -max2, 3},
		{-max1 - v119 - 99, -max1 - v119 - 2, 3},
		{max1 + v120 + 2, max1 + v120 + 99, 3},
		{max2, max2 + v120 + 1, 3},
		// 4 bytes
		{-max3 - v119 - 1, -max3, 4},
		{-max2 - v119 - 99, -max2 - v119 - 2, 4},
		{max2 + v120 + 2, max2 + v120 + 99, 4},
		{max3, max3 + v120 + 1, 4},
		// 5 bytes
		{minInt32, minInt32 + 99, 5},
		{maxInt32 - 99, maxInt32, 5},
	}

	for _, r := range tests {
		testInt32Range(t, r.start, r.end, r.want)
	}
}

func TestPackedInt64(t *testing.T) {
	tests := []struct {
		start, end int64
		want       uint // expected number of bytes to be written
	}{
		// 1 byte
		{-v119, v120, 1},
		// 2 bytes
		{-max1 - v119 - 1, -v119 - 1, 2},
		{v120 + 1, max1 + v120 + 1, 2},
		// 3 bytes
		{-max2 - v119 - 1, -max2, 3},
		{-max1 - v119 - 99, -max1 - v119 - 2, 3},
		{max1 + v120 + 2, max1 + v120 + 99, 3},
		{max2, max2 + v120 + 1, 3},
		// 4 bytes
		{-max3 - v119 - 1, -max3, 4},
		{-max2 - v119 - 99, -max2 - v119 - 2, 4},
		{max2 + v120 + 2, max2 + v120 + 99, 4},
		{max3, max3 + v120 + 1, 4},
		// 5 bytes
		{-max4 - v119 - 1, -max4, 5},
		{-max3 - v119 - 99, -max3 - v119 - 2, 5},
		{max3 + v120 + 2, max3 + v120 + 99, 5},
		{max4, max4 + v120 + 1, 5},
		// 6 bytes
		{-max5 - v119 - 1, -max5, 6},
		{-max4 - v119 - 99, -max4 - v119 - 2, 6},
		{max4 + v120 + 2, max4 + v120 + 99, 6},
		{max5, max5 + v120 + 1, 6},
		// 7 bytes
		{-max6 - v119 - 1, -max6, 7},
		{-max5 - v119 - 99, -max5 - v119 - 2, 7},
		{max5 + v120 + 2, max5 + v120 + 99, 7},
		{max6, max6 + v120 + 1, 7},
		// 8 bytes
		{-max7 - v119 - 1, -max7, 8},
		{-max6 - v119 - 99, -max6 - v119 - 2, 8},
		{max6 + v120 + 2, max6 + v120 + 99, 8},
		{max7, max7 + v120 + 1, 8},
		// 9 bytes
		{minInt64, minInt64 + 99, 9},
		{maxInt64 - 99, maxInt64, 9},
	}

	for _, r := range tests {
		testInt64Range(t, r.start, r.end, r.want)
	}
}

func testInt32Range(t *testing.T, start, end int32, want uint) {
	n := (end - start + 1) * maxPackedInt32Length
	buf := make([]byte, n)
	off := uint(0)
	for i := start; i <= end; i++ {
		prev := off
		off = writeSortedInt32(buf, off, i)
		bytes := off - prev
		if bytes != want {
			t.Errorf("writeSortedInt32(value=%d) got byteLen=%d; want %v", i, bytes, want)
		}
		bytes = uint(getWriteSortedInt32Length(i))
		if bytes != want {
			t.Errorf("getWriteSortedInt32Length(value=%d) got byteLen=%d; want %d", i, bytes, want)
		}
		// avoids overflow in next loop
		if i == maxInt32 {
			break
		}
	}

	off = 0
	for i := start; i <= end; i++ {
		bytes := uint(getReadSortedInt32Length(buf, off))
		if bytes != want {
			t.Errorf("getReadSortedInt32Length(value=%d) got byteLen=%d; want %d", i, bytes, want)
		}
		value := readSortedInt32(buf, off)
		if value != i {
			t.Errorf("readSortedInt32 got %d; want %d", value, i)
		}
		off += bytes

		if i == maxInt32 {
			break
		}
	}
}

func testInt64Range(t *testing.T, start, end int64, want uint) {
	n := (end - start + 1) * maxPackedInt64Length
	buf := make([]byte, n)
	off := uint(0)
	for i := start; i <= end; i++ {
		prev := off
		off = writeSortedInt64(buf, off, i)
		bytes := off - prev
		if bytes != want {
			t.Errorf("writeSortedInt64(value=%d) got byteLen=%d; want %d", i, bytes, want)
		}
		bytes = uint(getWriteSortedInt64Length(i))
		if bytes != want {
			t.Errorf("getWriteSortedInt64Length(value=%d) got byteLen=%d; want %d", i, bytes, want)
		}
		// avoids overflow in next loop
		if i == maxInt64 {
			break
		}
	}

	off = 0
	for i := start; i <= end; i++ {
		bytes := uint(getReadSortedInt64Length(buf, off))
		if bytes != want {
			t.Errorf("getReadSortedInt64Length(value=%d) got byteLen=%d; want %d", i, bytes, want)
		}
		value := readSortedInt64(buf, off)
		if value != i {
			t.Errorf("readSortedInt64 got %d; want %d", value, i)
		}
		off += bytes

		if i == maxInt64 {
			break
		}
	}
}
