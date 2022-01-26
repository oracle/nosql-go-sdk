//
// Copyright (c) 2019, 2022 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// This file implements reading and writing packed integer values.
// This is ported from JE's PackedInteger class.

package binary

const (
	// The maximum number of bytes needed to store a signed 32-bit integer
	maxPackedInt32Length = 5
	// The maximum number of bytes needed to store a signed 64-bit integer
	maxPackedInt64Length = 9
)

// readSortedInt32 reads a sorted packed int32 value from buf starting at
// offset off and returns it
func readSortedInt32(buf []byte, off uint) int32 {
	var byteLen int
	var negative bool
	// The first byte stores the length of the value part.
	b1 := int(buf[off])
	off++
	// Adjust the byteLen to the real length of the value part.
	switch {
	case b1 < 0x08:
		byteLen = 0x08 - b1
		negative = true
	case b1 > 0xF7:
		byteLen = b1 - 0xF7
		negative = false
	default:
		return int32(b1 - 127)
	}

	// The following bytes on the buf store the value as a big endian integer.
	// We extract the significant bytes from the buf and put them into the
	// value in big endian order.
	value := int32(0)
	if negative {
		value = ^int32(0) // 0xFFFFFFFF
	}

	if byteLen > 3 {
		value = (value << 8) | int32(buf[off])
		off++
	}
	if byteLen > 2 {
		value = (value << 8) | int32(buf[off])
		off++
	}
	if byteLen > 1 {
		value = (value << 8) | int32(buf[off])
		off++
	}
	value = (value << 8) | int32(buf[off])
	off++

	// After get the adjusted value, we have to adjust it back to the
	// original value.
	if negative {
		value -= 119
	} else {
		value += 121
	}
	return value
}

// readSortedInt64 reads a sorted packed int64 value from buf starting at
// offset off and returns it
func readSortedInt64(buf []byte, off uint) int64 {
	var byteLen int
	var negative bool
	// The first byte stores the length of the value part.
	b1 := int(buf[off])
	off++
	// Adjust the byteLen to the real length of the value part.
	switch {
	case b1 < 0x08:
		byteLen = 0x08 - b1
		negative = true
	case b1 > 0xF7:
		byteLen = b1 - 0xF7
		negative = false
	default:
		return int64(b1 - 127)
	}

	// The following bytes on the buf store the value as a big endian integer.
	// We extract the significant bytes from the buf and put them into the
	// value in big endian order.
	value := int64(0)
	if negative {
		value = ^int64(0) // 0xFFFFFFFFFFFFFFFF
	}

	if byteLen > 7 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	if byteLen > 6 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	if byteLen > 5 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	if byteLen > 4 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	if byteLen > 3 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	if byteLen > 2 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	if byteLen > 1 {
		value = (value << 8) | int64(buf[off])
		off++
	}
	value = (value << 8) | int64(buf[off])
	off++

	// After obtaining the adjusted value, we have to adjust it back to the
	// original value.
	if negative {
		value -= 119
	} else {
		value += 121
	}
	return value
}

// getReadSortedInt32Length returns the number of bytes that would be read by
// readSortedInt32.
func getReadSortedInt32Length(buf []byte, off uint) int {
	// The first byte stores the length of the value part.
	b1 := int(buf[off])
	switch {
	case b1 < 0x08:
		return 1 + 0x08 - b1
	case b1 > 0xF7:
		return 1 + b1 - 0xF7
	default:
		return 1
	}
}

// getReadSortedInt64Length returns the number of bytes that would be read by
// readSortedInt64
func getReadSortedInt64Length(buf []byte, off uint) int {
	// The length is stored in the same way for int32 and int64
	return getReadSortedInt32Length(buf, off)
}

// writeSortedInt32 writes an int32 value as a packed sorted integer into buf
// starting at offset off, returns the next offset to be written.
func writeSortedInt32(buf []byte, off uint, value int32) uint {

	// Values in the inclusive range [-119,120] are stored in a single byte.
	// For values outside that range, the first byte stores the number of
	// additional bytes. The additional bytes store
	// (value + 119 for negative and value - 121 for positive) as an unsigned
	// big endian integer.
	byte1Off := off
	off++
	if value < -119 {
		// If the value < -119, then first adjust the value by adding 119.
		// Then the adjusted value is stored as an unsigned big endian integer.
		value += 119

		// Store the adjusted value as an unsigned big endian integer.
		// For a negative integer, from left to right, the first significant
		// byte is the byte which is not equal to 0xFF. Also please note that,
		// because the adjusted value is stored in big endian integer, we
		// extract the significant byte from left to right.
		//
		// In the left to right order, if the first byte of the adjusted value
		// is a significant byte, it will be stored in the 2nd byte of the buf.
		// Then we will look at the 2nd byte of the adjusted value to see if
		// this byte is the significant byte, if yes, this byte will be stored
		// in the 3rd byte of the buf, and the like.
		if (value | 0x00FFFFFF) != ^int32(0) {
			buf[off] = byte(value >> 24)
			off++
		}
		if (value | 0x0000FFFF) != ^int32(0) {
			buf[off] = byte(value >> 16)
			off++
		}
		if (value | 0x000000FF) != ^int32(0) {
			buf[off] = byte(value >> 8)
			off++
		}
		buf[off] = byte(value)
		off++

		// valueLen is the length of the value part stored in buf. Because the
		// first byte of buf is used to stored the length, we need to subtract
		// one.
		valueLen := off - byte1Off - 1

		// The first byte stores the number of additional bytes. Here we store
		// the result of 0x08 - valueLen, rather than directly store valueLen.
		// The reason is to implement natural sort order for byte-by-byte
		// comparison.
		buf[byte1Off] = byte(0x08 - valueLen)
		return off
	}

	if value > 120 {
		// If the value > 120, then first adjust the value by subtracting 121.
		// Then the adjusted value is stored as an unsigned big endian integer.
		value -= 121

		// Store the adjusted value as an unsigned big endian integer.
		// For a positive integer, from left to right, the first significant
		// byte is the byte which is not equal to 0x00.
		//
		// In the left to right order, if the first byte of the adjusted value
		// is a significant byte, it will be stored in the 2nd byte of the buf.
		// Then we will look at the 2nd byte of the adjusted value to see if
		// this byte is the significant byte, if yes, this byte will be stored
		// in the 3rd byte of the buf, and the like.
		if ((value >> 24) & 0x000000FF) != 0 {
			buf[off] = byte(value >> 24)
			off++
		}
		if ((value >> 16) & 0x0000FFFF) != 0 {
			buf[off] = byte(value >> 16)
			off++
		}
		if ((value >> 8) & 0x00FFFFFF) != 0 {
			buf[off] = byte(value >> 8)
			off++
		}
		buf[off] = byte(value)
		off++

		// valueLen is the length of the value part stored in buf. Because the
		// first byte of buf is used to stored the length, we need to subtract
		// one.
		valueLen := off - byte1Off - 1

		// The first byte stores the number of additional bytes. Here we store
		// the result of 0xF7 + valueLen, rather than directly store valueLen.
		// The reason is to implement natural sort order for byte-by-byte
		// comparison.
		buf[byte1Off] = byte(0xF7 + valueLen)
		return off
	}

	// If -119 <= value <= 120, only one byte is needed to store the value.
	// The stored value is the original value plus 127.
	buf[byte1Off] = byte(value + 127)
	return off
}

// writeSortedInt64 writes an int64 value as a packed sorted integer into buf
// starting at offset off, returns the next offset to be written.
func writeSortedInt64(buf []byte, off uint, value int64) uint {

	// Values in the inclusive range [-119,120] are stored in a single byte.
	// For values outside that range, the first byte stores the number of
	// additional bytes. The additional bytes store
	// (value + 119 for negative and value - 121 for positive) as an unsigned
	// big endian integer.
	byte1Off := off
	off++
	if value < -119 {
		// If the value < -119, then first adjust the value by adding 119.
		// Then the adjusted value is stored as an unsigned big endian integer.
		value += 119

		// Store the adjusted value as an unsigned big endian integer.
		// For an negative integer, from left to right, the first significant
		// byte is the byte which is not equal to 0xFF. Also please note that,
		// because the adjusted value is stored in big endian integer, we
		// extract the significant byte from left to right.
		//
		// In the left to right order, if the first byte of the adjusted value
		// is a significant byte, it will be stored in the 2nd byte of the buf.
		// Then we will look at the 2nd byte of the adjusted value to see if
		// this byte is the significant byte, if yes, this byte will be stored
		// in the 3rd byte of the buf, and the like.
		if (value | 0x00FFFFFFFFFFFFFF) != ^int64(0) {
			buf[off] = byte(value >> 56)
			off++
		}
		if (value | 0x0000FFFFFFFFFFFF) != ^int64(0) {
			buf[off] = byte(value >> 48)
			off++
		}
		if (value | 0x000000FFFFFFFFFF) != ^int64(0) {
			buf[off] = byte(value >> 40)
			off++
		}
		if (value | 0x00000000FFFFFFFF) != ^int64(0) {
			buf[off] = byte(value >> 32)
			off++
		}
		if (value | 0x0000000000FFFFFF) != ^int64(0) {
			buf[off] = byte(value >> 24)
			off++
		}
		if (value | 0x000000000000FFFF) != ^int64(0) {
			buf[off] = byte(value >> 16)
			off++
		}
		if (value | 0x00000000000000FF) != ^int64(0) {
			buf[off] = byte(value >> 8)
			off++
		}
		buf[off] = byte(value)
		off++

		// valueLen is the length of the value part stored in buf. Because
		// the first byte of buf is used to stored the length, so we need to
		// subtract one.
		valueLen := off - byte1Off - 1

		// The first byte stores the number of additional bytes. Here we store
		// the result of 0x08 - valueLen, rather than directly store valueLen.
		// The reason is to implement nature sort order for byte-by-byte
		// comparison.
		buf[byte1Off] = byte(0x08 - valueLen)
		return off
	}

	if value > 120 {

		// If the value > 120, then first adjust the value by subtracting 121.
		// Then the adjusted value is stored as an unsigned big endian integer.
		value -= 121

		// Store the adjusted value as an unsigned big endian integer.
		// For a positive integer, from left to right, the first significant
		// byte is the byte which is not equal to 0x00.
		//
		// In the left to right order, if the first byte of the adjusted value
		// is a significant byte, it will be stored in the 2nd byte of the buf.
		// Then we will look at the 2nd byte of the adjusted value to see if
		// this byte is the significant byte, if yes, this byte will be stored
		// in the 3rd byte of the buf, and the like.
		if ((value >> 56) & 0x00000000000000FF) != 0 {
			buf[off] = byte(value >> 56)
			off++
		}
		if ((value >> 48) & 0x000000000000FFFF) != 0 {
			buf[off] = byte(value >> 48)
			off++
		}
		if ((value >> 40) & 0x0000000000FFFFFF) != 0 {
			buf[off] = byte(value >> 40)
			off++
		}
		if ((value >> 32) & 0x00000000FFFFFFFF) != 0 {
			buf[off] = byte(value >> 32)
			off++
		}
		if ((value >> 24) & 0x000000FFFFFFFFFF) != 0 {
			buf[off] = byte(value >> 24)
			off++
		}
		if ((value >> 16) & 0x0000FFFFFFFFFFFF) != 0 {
			buf[off] = byte(value >> 16)
			off++
		}
		if ((value >> 8) & 0x00FFFFFFFFFFFFFF) != 0 {
			buf[off] = byte(value >> 8)
			off++
		}
		buf[off] = byte(value)
		off++

		// valueLen is the length of the value part stored in buf. Because the
		// first byte of buf is used to stored the length, so we need to
		// subtract one.
		valueLen := off - byte1Off - 1

		// The first byte stores the number of additional bytes. Here we store
		// the result of 0xF7 + valueLen, rather than directly store valueLen.
		// The reason is to implement nature sort order for byte-by-byte
		// comparison.
		buf[byte1Off] = byte(0xF7 + valueLen)
		return off
	}

	// If -119 <= value <= 120, only one byte is needed to store the value.
	// The stored value is the original value adds 127.
	buf[byte1Off] = byte(value + 127)
	return off
}

// getWriteSortedInt32Length returns the number of bytes that the value would
// be written by writeSortedInt32
func getWriteSortedInt32Length(value int32) int {

	if value < -119 {
		// Adjust the value.
		value += 119

		// Find the left most significant byte of the adjusted value, and return
		// the length accordingly.
		if (value | 0x000000FF) == ^int32(0) {
			return 2
		}
		if (value | 0x0000FFFF) == ^int32(0) {
			return 3
		}
		if (value | 0x00FFFFFF) == ^int32(0) {
			return 4
		}
		return 5
	}

	if value > 120 {
		value -= 121

		if ((value >> 8) & 0x00FFFFFF) == 0 {
			return 2
		}
		if ((value >> 16) & 0x0000FFFF) == 0 {
			return 3
		}
		if ((value >> 24) & 0x000000FF) == 0 {
			return 4
		}
		return 5
	}

	// If -119 <= value <= 120, only one byte is needed to store the value.
	return 1
}

// getWriteSortedInt64Length returns the number of bytes that the value would
// be written by writeSortedInt64
func getWriteSortedInt64Length(value int64) int {

	if value < -119 {
		// Adjust the value.
		value += 119

		// Find the left most significant byte of the adjusted value, and return
		// the length accordingly.
		if (value | 0x00000000000000FF) == ^int64(0) {
			return 2
		}
		if (value | 0x000000000000FFFF) == ^int64(0) {
			return 3
		}
		if (value | 0x0000000000FFFFFF) == ^int64(0) {
			return 4
		}
		if (value | 0x00000000FFFFFFFF) == ^int64(0) {
			return 5
		}
		if (value | 0x000000FFFFFFFFFF) == ^int64(0) {
			return 6
		}
		if (value | 0x0000FFFFFFFFFFFF) == ^int64(0) {
			return 7
		}
		if (value | 0x00FFFFFFFFFFFFFF) == ^int64(0) {
			return 8
		}
		return 9
	}

	if value > 120 {
		value -= 121

		if ((value >> 8) & 0x00FFFFFFFFFFFFFF) == 0 {
			return 2
		}
		if ((value >> 16) & 0x0000FFFFFFFFFFFF) == 0 {
			return 3
		}
		if ((value >> 24) & 0x000000FFFFFFFFFF) == 0 {
			return 4
		}
		if ((value >> 32) & 0x00000000FFFFFFFF) == 0 {
			return 5
		}
		if ((value >> 40) & 0x0000000000FFFFFF) == 0 {
			return 6
		}
		if ((value >> 48) & 0x000000000000FFFF) == 0 {
			return 7
		}
		if ((value >> 56) & 0x00000000000000FF) == 0 {
			return 8
		}
		return 9
	}

	// If -119 <= value <= 120, only one byte is needed to store the value
	return 1
}
