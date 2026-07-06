//
// Copyright (c) 2019, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb/internal/proto/binary"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/require"
)

type v3NativeStructRow struct {
	ID   int    `nosql:"id"`
	Name string `nosql:"name"`
}

func readV3TableOpHeader(t *testing.T, r *binary.Reader) {
	t.Helper()

	_, err := r.ReadByte()
	require.NoError(t, err)
	_, err = r.ReadPackedInt()
	require.NoError(t, err)
	tableName, err := r.ReadString()
	require.NoError(t, err)
	require.NotNil(t, tableName)
}

func TestGetRequestSerializeV3UsesStructValueAsKey(t *testing.T) {
	req := &GetRequest{
		TableName:   "users",
		StructValue: &v3NativeStructRow{ID: 7, Name: "Ada"},
		Timeout:     time.Second,
		Consistency: types.Absolute,
		StructType:  reflect.TypeOf((*v3NativeStructRow)(nil)).Elem(),
	}
	w := binary.NewWriter()

	require.NoError(t, req.serializeV3(w, int16(3)))

	r := binary.NewReader(bytes.NewBuffer(w.Bytes()))
	readV3TableOpHeader(t, r)
	_, err := r.ReadByte()
	require.NoError(t, err)
	key, err := r.ReadFieldValue()
	require.NoError(t, err)
	mv, ok := key.(*types.MapValue)
	require.Truef(t, ok, "V3 Get key should serialize StructValue as MapValue, got %T", key)
	id, ok := mv.Get("id")
	require.True(t, ok)
	require.Equal(t, int64(7), id)
	name, ok := mv.Get("name")
	require.True(t, ok)
	require.Equal(t, "Ada", name)
}

func TestPutRequestSerializeV3UsesStructValueAsRow(t *testing.T) {
	req := &PutRequest{
		TableName:   "users",
		StructValue: &v3NativeStructRow{ID: 7, Name: "Ada"},
		Timeout:     time.Second,
	}
	w := binary.NewWriter()

	require.NoError(t, req.serializeV3(w, int16(3)))

	r := binary.NewReader(bytes.NewBuffer(w.Bytes()))
	readV3TableOpHeader(t, r)
	_, err := r.ReadBoolean()
	require.NoError(t, err)
	_, err = r.ReadByte()
	require.NoError(t, err)
	_, err = r.ReadBoolean()
	require.NoError(t, err)
	_, err = r.ReadPackedInt()
	require.NoError(t, err)
	value, err := r.ReadFieldValue()
	require.NoError(t, err)
	mv, ok := value.(*types.MapValue)
	require.Truef(t, ok, "V3 Put row should serialize StructValue as MapValue, got %T", value)
	id, ok := mv.Get("id")
	require.True(t, ok)
	require.Equal(t, int64(7), id)
	name, ok := mv.Get("name")
	require.True(t, ok)
	require.Equal(t, "Ada", name)
}

func TestGetRequestDeserializeV3PopulatesStructValue(t *testing.T) {
	req := &GetRequest{
		StructType: reflect.TypeOf((*v3NativeStructRow)(nil)).Elem(),
	}
	row := types.NewMapValue(map[string]interface{}{
		"id":   7,
		"name": "Ada",
	})
	w := binary.NewWriter()
	_, err := w.WritePackedInt(1)
	require.NoError(t, err)
	_, err = w.WritePackedInt(1)
	require.NoError(t, err)
	_, err = w.WritePackedInt(0)
	require.NoError(t, err)
	_, err = w.WriteBoolean(true)
	require.NoError(t, err)
	_, err = w.WriteFieldValue(row)
	require.NoError(t, err)
	_, err = w.WritePackedLong(0)
	require.NoError(t, err)
	_, err = w.WriteVersion(types.Version{1})
	require.NoError(t, err)
	_, err = w.WritePackedLong(123)
	require.NoError(t, err)

	res, err := req.deserializeV3(binary.NewReader(bytes.NewBuffer(w.Bytes())), int16(3))
	require.NoError(t, err)
	getRes := res.(*GetResult)
	require.Nil(t, getRes.Value)
	require.Equal(t, &v3NativeStructRow{ID: 7, Name: "Ada"}, getRes.StructValue)
}
