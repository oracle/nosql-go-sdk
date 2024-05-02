//
// Copyright (c) 2019, 2024 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package binary

import (
	"fmt"
	"math/big"
	"os"
	"reflect"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

// structDecoder reads byte sequences from the underlying binary.Reader and decodes the
// bytes to construct native structs according to the Binary Protocol
// which defines the data exchange format between the Oracle NoSQL Database
// proxy and drivers.
type structDecoder struct {
	// The underlying MapValue being decoded
	mv *types.MapValue

	// TODO: more?
}

// decodeMap reads a structured byte sequences that represent the encoding of a
// Map value, and decodes the bytes into the passed-in field, which must
// be either a struct or a map[string]<specific type>
func (sr *structDecoder) decodeMap(m map[string]interface{}, v reflect.Value) error {

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

	for key, value := range m {

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
			f := fields.byExactName[key]
			if f == nil {
				f = fields.byFoldedName[string(foldName([]byte(key)))]
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
				fmt.Fprintf(os.Stdout, "nosql: unknown field '%s'\n", key)
			}
		}

		err := sr.decodeFieldValue(value, subv)
		if err != nil {
			return err
		}

		// Write value back to map;
		// if using struct, subv points into struct already.
		if v.Kind() == reflect.Map {
			kv := reflect.New(t.Key()).Elem()
			kv.SetString(key)
			if kv.IsValid() {
				v.SetMapIndex(kv, subv)
			}
		}
	}

	return nil
}

// decodeArray reads a structured byte sequences that represent the encoding of an
// array, decodes the bytes and puts the values into the given array.
func (sr *structDecoder) decodeArray(a []types.FieldValue, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		// ok
	default:
		return fmt.Errorf("invalid value type, expected array or slice: %v", v.Kind())
	}

	// The number of elements in the array.
	size := len(a)

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
			if err := sr.decodeFieldValue(a[i], v.Index(i)); err != nil {
				return err
			}
		} else {
			// Ran out of fixed array: skip.
		}
	}

	return nil
}

// decodeFieldValue reads a fixed or variable length of bytes, decodes them and
// sets the result into the passed-in Value
func (sr *structDecoder) decodeFieldValue(mv any, v reflect.Value) error {
	if !v.IsValid() {
		return nil
	}

	// Handle nil values differently
	if mv == nil {
		v = indirect(v, true)
		v.SetZero()
		return nil
	}
	switch mv.(type) {
	case *types.EmptyValue, *types.NullValue, *types.JSONNullValue:
		v = indirect(v, true)
		v.SetZero()
		return nil
	}

	//case []byte

	v = indirect(v, false)

	if m, ok := mv.(*types.MapValue); ok {
		return sr.decodeMap(m.Map(), v)
	}
	if m, ok := mv.(map[string]interface{}); ok {
		return sr.decodeMap(m, v)
	}

	//if a, ok := mv.([]interface{}); ok {
	//return sr.decodeArray(a, v)
	//}
	if a, ok := mv.([]types.FieldValue); ok {
		return sr.decodeArray(a, v)
	}

	if val, ok := mv.([]byte); ok {
		v.Set(reflect.ValueOf(val))
		return nil
	}

	if val, ok := mv.(bool); ok {
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&val))
		} else {
			v.Set(reflect.ValueOf(val))
		}
		return nil
	}

	if val, ok := mv.(float64); ok {
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&val))
		} else {
			v.Set(reflect.ValueOf(val))
		}
		return nil
	}

	if val, ok := mv.(int); ok {
		return setLong(v, int64(val))
	}

	if val, ok := mv.(int32); ok {
		return setLong(v, int64(val))
	}

	if val, ok := mv.(int64); ok {
		return setLong(v, val)
	}

	if s, ok := mv.(string); ok {
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&s))
		} else {
			v.SetString(s)
		}
		return nil
	}

	if val, ok := mv.(time.Time); ok {
		if v.Type().Kind() == reflect.Interface {
			v.Set(reflect.ValueOf(&val))
		} else {
			v.Set(reflect.ValueOf(val))
		}
		return nil
	}

	if val, ok := mv.(big.Rat); ok {
		v.Set(reflect.ValueOf(val))
		return nil
	}

	return fmt.Errorf("binary.structDecoder: unexpected map field value %v of type %[1]T", mv)
}

// DecodeMapValue decodes the data in an existing MapValue into the given native struct.
// v must be a pointer to a struct.
func DecodeMapValue(v any, mv *types.MapValue) error {
	sr := &structDecoder{mv: mv}
	return sr.decode(mv, v)
}

func (sr *structDecoder) decode(mv *types.MapValue, v any) (err error) {
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
	//if rv, ok := v.(reflect.Value); ok {
	//return sr.decodeFieldValue(mv, rv)
	//}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("invalid value type passed to decode: %v", reflect.TypeOf(v))
	}

	return sr.decodeFieldValue(mv, rv)
}
