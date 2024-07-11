//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// StructReader reads byte sequences from the underlying binary.Reader and decodes the
// bytes to construct native structs according to the Binary Protocol
// which defines the data exchange format between the Oracle NoSQL Database
// proxy and drivers.
type StructReader struct {
	// The underlying binary.Reader.
	reader *Reader

	// TODO: more?
}

// NewStructReader creates a reader for the binary protocol.
func NewStructReader(b *bytes.Buffer) *StructReader {
	return &StructReader{
		reader: NewReader(b),
	}
}

// DisardMap reads and discards a map value. It is used when skipping over
// unused/unknown fields.
func (sr *StructReader) DiscardMap() error {
	_, err := sr.reader.ReadInt()
	if err != nil {
		return err
	}
	// The number of entries in the map.
	size, err := sr.reader.ReadInt()
	if err != nil {
		return err
	}
	for i := 0; i < size; i++ {
		// field name
		key, err := sr.reader.ReadString()
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Disacrding field '%s'\n", *key)
		err = sr.ReadFieldValue(reflect.Value{})
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadMap reads a structured byte sequences that represent the encoding of a
// Map value, and decodes the bytes into the passed-in field, which must
// be either a struct or a map[string]<specific type>
func (sr *StructReader) ReadMap(v reflect.Value) error {
	// The integer value that represents the number of bytes consumed by the map.
	// This is discarded as it is not used.
	_, err := sr.reader.ReadInt()
	if err != nil {
		return err
	}

	// The number of entries in the map.
	size, err := sr.reader.ReadInt()
	if err != nil {
		return err
	}

	t := v.Type()

	var fields structFields

	// Check type of target:
	//   struct or
	//   map[String]T
	switch v.Kind() {
	case reflect.Map:
		// Map key must have a string kind
		switch t.Key().Kind() {
		case reflect.String:
			switch t.Elem().Kind() {
			case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
				reflect.Float32, reflect.Float64, reflect.Array, reflect.Map, reflect.Pointer,
				reflect.String, reflect.Struct:
			default:
				return fmt.Errorf("cannot read NSON into a map with complex values (%v)", t.Elem().Kind())
			}
		default:
			return fmt.Errorf("cannot read NSON into a map with non-string key (%v)", t.Key().Kind())
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
	case reflect.Struct:
		fields = cachedTypeFields(t)
		// ok
	default:
		return fmt.Errorf("cannot read NSON into non-struct value (%v)", v.Kind())
	}

	var mapElem reflect.Value

	for i := 0; i < size; i++ {
		// field name
		key, err := sr.reader.ReadString()
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Handling field '%s'\n", *key)

		// Figure out field corresponding to key.
		var subv reflect.Value

		if v.Kind() == reflect.Map {
			elemType := t.Elem()
			if !mapElem.IsValid() {
				mapElem = reflect.New(elemType).Elem()
			} else {
				mapElem.SetZero()
			}
			subv = mapElem
		} else {
			f := fields.byExactName[string(*key)]
			if f == nil {
				f = fields.byFoldedName[string(foldName([]byte(*key)))]
			}
			if f != nil {
				subv = v
				//destring = f.quoted
				for _, i := range f.index {
					if subv.Kind() == reflect.Pointer {
						if subv.IsNil() {
							// If a struct embeds a pointer to an unexported type,
							// it is not possible to set a newly allocated value
							// since the field is unexported.
							//
							// See https://golang.org/issue/21357
							if !subv.CanSet() {
								return fmt.Errorf("cannot set embedded pointer to unexported struct: %v", subv.Type().Elem())
							}
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}
			} else {
				fmt.Fprintf(os.Stdout, "nosql: unknown field '%s'\n", *key)
			}
		}

		err = sr.ReadFieldValue(subv)
		if err != nil {
			return err
		}

		// Write value back to map;
		// if using struct, subv points into struct already.
		if v.Kind() == reflect.Map {
			kv := reflect.New(t.Key()).Elem()
			kv.SetString(string(*key))
			if kv.IsValid() {
				v.SetMapIndex(kv, subv)
			}
		}
	}

	return nil
}

// DiscardArray reads a structured byte sequences that represent the encoding of an
// array, decodes the bytes and discards them. It is used when skipping over unused or
// unknown struct fields.
func (sr *StructReader) DiscardArray() error {
	if _, err := sr.reader.ReadInt(); err != nil {
		return err
	}
	size, err := sr.reader.ReadInt()
	if err != nil {
		return err
	}
	for i := 0; i < size; i++ {
		if err := sr.ReadFieldValue(reflect.Value{}); err != nil {
			return err
		}
	}
	return nil
}

// ReadArray reads a structured byte sequences that represent the encoding of an
// array, decodes the bytes and puts the values into the given array.
func (sr *StructReader) ReadArray(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		// ok
	default:
		return fmt.Errorf("invalid value type, expected array or slice: %v", v.Kind())
	}

	// The integer value that represents the number of bytes consumed by the array.
	// This is discarded as it is not used.
	if _, err := sr.reader.ReadInt(); err != nil {
		return err
	}

	// The number of elements in the array.
	size, err := sr.reader.ReadInt()
	if err != nil {
		return err
	}

	if v.Kind() == reflect.Slice {
		if size >= v.Cap() {
			v.Grow(size - v.Cap())
		}
		if size > v.Len() {
			v.SetLen(size)
		}
	}

	for i := 0; i < size; i++ {
		if i < v.Len() {
			if err := sr.ReadFieldValue(v.Index(i)); err != nil {
				return err
			}
		} else {
			// Ran out of fixed array: skip.
			if err := sr.ReadFieldValue(reflect.Value{}); err != nil {
				return err
			}
		}
	}

	return nil
}

func setLong(v reflect.Value, val int64) error {
	switch v.Type().Kind() {
	case reflect.Uint, reflect.Uintptr, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64:
		v.SetUint(uint64(val))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64:
		v.SetInt(val)
	case reflect.Interface:
		v.Set(reflect.ValueOf(&val))
	default:
		return fmt.Errorf("invalid value type, expect integer: %v", v.Type().Kind())
	}
	return nil
}

// ReadFieldValue reads a fixed or variable length of bytes, decodes them and
// sets the result into the passed-in Value
func (sr *StructReader) ReadFieldValue(v reflect.Value) error {
	t, err := sr.reader.ReadByte()
	if err != nil {
		return err
	}

	// Handle nil values differently
	switch types.DbType(t) {
	case types.JSONNull, types.Null, types.Empty:
		if !v.IsValid() {
			return nil
		}
		v = indirect(v, true)
		v.SetZero()
		//switch v.Kind() {
		//case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice:
		//v.SetZero()
		// otherwise, ignore null for primitives/string (same as json library)
		//}
		return nil
	}

	if v.IsValid() {
		v = indirect(v, false)
	}

	switch types.DbType(t) {
	case types.Array:
		if !v.IsValid() {
			return sr.DiscardArray()
		}
		return sr.ReadArray(v)

	case types.Map:
		if !v.IsValid() {
			return sr.DiscardMap()
		}
		return sr.ReadMap(v)

	case types.Binary:
		val, err := sr.reader.ReadByteArray()
		if err != nil {
			return err
		}
		if !v.IsValid() {
			return nil
		}
		v.Set(reflect.ValueOf(val))

	case types.Boolean:
		val, err := sr.reader.ReadBoolean()
		if err != nil {
			return err
		}
		if !v.IsValid() {
			return nil
		}
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&val))
		} else {
			v.Set(reflect.ValueOf(val))
		}

	case types.Double:
		val, err := sr.reader.ReadDouble()
		if err != nil {
			return err
		}
		if !v.IsValid() {
			return nil
		}
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&val))
		} else {
			v.Set(reflect.ValueOf(val))
		}

	case types.Integer:
		val, err := sr.reader.ReadPackedInt()
		if err != nil {
			return err
		}
		if !v.IsValid() {
			return nil
		}
		return setLong(v, int64(val))

	case types.Long:
		val, err := sr.reader.ReadPackedLong()
		if err != nil {
			return err
		}
		if !v.IsValid() {
			return nil
		}
		return setLong(v, val)

	case types.String:
		s, err := sr.reader.ReadString()
		if err != nil {
			return err
		}
		if !v.IsValid() {
			return nil
		}
		if s == nil {
			v.SetZero()
		} else {
			if v.Type().Kind() == reflect.Interface {
				v.Set(reflect.ValueOf(s))
			} else {
				v.SetString(*s)
			}
		}

	case types.Timestamp:
		s, err := sr.reader.ReadString()
		if err != nil {
			return err
		}
		if s == nil {
			return errors.New("binary.StructReader: invalid Timestamp value")
		}
		if !v.IsValid() {
			return nil
		}
		val, err := types.ParseDateTime(*s)
		if err != nil {
			return err
		}
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&val))
		} else {
			v.Set(reflect.ValueOf(val))
		}

	case types.Number:
		s, err := sr.reader.ReadString()
		if err != nil {
			return err
		}
		if s == nil {
			return errors.New("binary.StructReader: invalid Number value")
		}
		if !v.IsValid() {
			return nil
		}
		number, ok := new(big.Rat).SetString(*s)
		if ok {
			v.Set(reflect.ValueOf(number))
		} else {
			// Return as a string.
			v.SetString(*s)
		}

	default:
		return fmt.Errorf("binary.StructReader: unsupported field value %v of type %[1]T", t)
	}
	return nil
}

func UnmarshalFromReader(v any, r *Reader) error {
	sr := &StructReader{reader: r}
	return sr.Unmarshal(v)
}

func (sr *StructReader) Unmarshal(v any) (err error) {
	// catch panics
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()
	// if already passed a reflect.Value, use that directly
	if rv, ok := v.(reflect.Value); ok {
		return sr.ReadFieldValue(rv)
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("invalid value type passed to Unmarshal: %v", reflect.TypeOf(v))
	}

	return sr.ReadFieldValue(rv)
}

// indirect walks down v allocating pointers as needed,
// until it gets to a non-pointer.
// If decodingNull is true, indirect stops at the first settable pointer so it
// can be set to nil.
func indirect(v reflect.Value, decodingNull bool) reflect.Value {
	// Issue #24153 indicates that it is generally not a guaranteed property
	// that you may round-trip a reflect.Value by calling Value.Addr().Elem()
	// and expect the value to still be settable for values derived from
	// unexported embedded struct fields.
	//
	// The logic below effectively does this when it first addresses the value
	// (to satisfy possible pointer methods) and continues to dereference
	// subsequent pointers as necessary.
	//
	// After the first round-trip, we set v back to the original value to
	// preserve the original RW flags contained in reflect.Value.
	v0 := v
	haveAddr := false

	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Pointer && v.Type().Name() != "" && v.CanAddr() {
		haveAddr = true
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Pointer && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Pointer) {
				haveAddr = false
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Pointer {
			break
		}

		if decodingNull && v.CanSet() {
			break
		}

		// Prevent infinite loop if v is an interface pointing to its own address:
		//     var v interface{}
		//     v = &v
		if v.Elem().Kind() == reflect.Interface && v.Elem().Elem() == v {
			v = v.Elem()
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}

		if haveAddr {
			v = v0 // restore original value after round-trip Value.Addr().Elem()
			haveAddr = false
		} else {
			v = v.Elem()
		}
	}
	return v
}
