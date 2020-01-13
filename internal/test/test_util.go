//
// Copyright (C) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
//
// Please see LICENSE.txt file included in the top-level directory of the
// appropriate download for a copy of the license and additional information.
//

package test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/assert"
)

// AssertWriteKB checks if writeKB is the same as expectWriteKB.
func AssertWriteKB(assert *assert.Assertions, expectWriteKB, writeKB int) {
	if IsOnPrem() {
		expectWriteKB = 0
	}

	assert.Equalf(expectWriteKB, writeKB, "wrong writeKB")
}

// AssertReadKB checks if readKB is the same as expectReadKB.
func AssertReadKB(assert *assert.Assertions, expectReadKB, readKB, readUnits, prepCost int, isAbsolute bool) {
	if IsOnPrem() {
		assert.Equal(0, readKB, "wrong readKB")
	} else {
		actualReadKB := readKB - prepCost
		assert.Equal(expectReadKB, actualReadKB, "wrong readKB")
	}

	AssertReadUnits(assert, readKB, readUnits, prepCost, isAbsolute)
}

// AssertReadUnits checks if readUnits is as expected.
func AssertReadUnits(assert *assert.Assertions, readKB, readUnits, prepCost int, isAbsolute bool) {
	if IsOnPrem() {
		assert.Equal(0, readUnits, "wrong readUnits")
	} else {
		expectReadUnits := readKB - prepCost
		if isAbsolute {
			expectReadUnits <<= 1
		}
		actualReadUnits := readUnits - prepCost
		assert.Equal(expectReadUnits, actualReadUnits, "wrong readUnits")
	}
}

// ExecuteQueryStmt executes the specified query statement using the specified client.
// It returns query results as a slice of *types.MapValue.
func ExecuteQueryStmt(client *nosqldb.Client, stmt string) ([]*types.MapValue, error) {
	return ExecuteQueryRequest(client, &nosqldb.QueryRequest{Statement: stmt})
}

// ExecuteQueryRequest executes the specified query request using the specified client.
// It returns query results as a slice of *types.MapValue.
func ExecuteQueryRequest(client *nosqldb.Client, queryReq *nosqldb.QueryRequest) ([]*types.MapValue, error) {
	var err error
	var queryRes *nosqldb.QueryResult
	var res []*types.MapValue
	var results []*types.MapValue

	for {
		queryRes, err = client.Query(queryReq)
		if err != nil {
			return nil, err
		}

		res, err = queryRes.GetResults()
		if err != nil {
			return nil, err
		}

		if len(res) > 0 {
			if results == nil {
				results = make([]*types.MapValue, 0, 16)
			}

			results = append(results, res...)
		}

		if queryReq.IsDone() {
			break
		}
	}

	return results, nil
}

// DeleteTable deletes all rows from the specified table.
// The primary key column names must be provided in the pks arguments.
func DeleteTable(table string, pks ...string) (err error) {
	stmt := fmt.Sprintf("select %s from %s", strings.Join(pks, ","), table)
	results, err := ExecuteQueryStmt(client, stmt)
	if err != nil {
		return
	}

	var delRes *nosqldb.DeleteResult
	delReq := &nosqldb.DeleteRequest{
		TableName: table,
	}
	for _, v := range results {
		delReq.Key = v
		delRes, err = client.Delete(delReq)
		if err != nil {
			return
		}
		if !delRes.Success {
			return fmt.Errorf("failed to delete row from table(name=%s, pk=%v)", table, v)
		}
	}

	return nil
}

// TODO: support all database types.
var columnTypes = []string{"string", "integer", "long", "float", "double"}

// GenCreateTableStmt generates a "create table" statement with the specified
// table name and number of columns.
func GenCreateTableStmt(table string, numCols int, colNamePrefix string) string {
	if table == "" {
		panic("table name must be non-empty")
	}
	if numCols < 1 {
		panic("the number of columns must be a minimum of 1")
	}
	var sb strings.Builder
	sb.WriteString("create table " + table + " (")
	for i := 0; i < numCols; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		colType := columnTypes[rand.Intn(len(columnTypes))]
		sb.WriteString(fmt.Sprintf("%s%d %s", colNamePrefix, i+1, colType))
	}
	sb.WriteString(fmt.Sprintf(", primary key(%s%d))", colNamePrefix, 1))
	return sb.String()
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// GenString randomly generates a string that contains the specified number of characters.
func GenString(n int) string {
	if n <= 0 {
		return ""
	}

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// GenBytes generates n bytes.
func GenBytes(n int) []byte {
	buf := make([]byte, n)
	rand.Read(buf)
	return buf
}

// CompareMapValue compares underlying map values of m1 and m2, it also compares insertion
// order of the values if checkInsertionOrder is specified.
// It returns true if the values are equal, otherwise returns false.
func CompareMapValue(m1, m2 *types.MapValue, checkInsertionOrder bool) bool {
	if m1 == nil || m2 == nil {
		return m1 == m2
	}

	if m1.Len() != m2.Len() {
		return false
	}

	if checkInsertionOrder {
		for i := 1; i <= m1.Len(); i++ {
			k1, v1, _ := m1.GetByIndex(i)
			k2, v2, ok := m2.GetByIndex(i)
			if !ok || k1 != k2 {
				return false
			}

			if !valueEqual(v1, v2) {
				return false
			}
		}

		return true

	}

	// Compare un-ordered MapValue.
	for k1, v1 := range m1.Map() {
		v2, ok2 := m2.Get(k1)
		if !ok2 {
			return false
		}

		if !valueEqual(v1, v2) {
			return false
		}
	}

	return true
}

func valueEqual(v1, v2 interface{}) bool {
	if v1 == nil {
		return v2 == nil || v2 == types.JSONNullValueInstance ||
			v2 == types.NullValueInstance || v2 == types.EmptyValueInstance
	}

	if v2 == nil {
		return v1 == nil || v1 == types.JSONNullValueInstance ||
			v1 == types.NullValueInstance || v1 == types.EmptyValueInstance
	}

	if v1 == types.JSONNullValueInstance || v2 == types.JSONNullValueInstance {
		return v1 == v2
	}

	if v1 == types.NullValueInstance || v2 == types.NullValueInstance {
		return v1 == v2
	}

	if v1 == types.EmptyValueInstance || v2 == types.EmptyValueInstance {
		return v1 == v2
	}

	if n1, ok := v1.(json.Number); ok {
		return jsonNumberEqual(n1, v2, &nosqldb.Decimal32)
	}

	if n2, ok := v2.(json.Number); ok {
		return jsonNumberEqual(n2, v1, &nosqldb.Decimal32)
	}

	if t1, ok := v1.(time.Time); ok {
		return timeValueEqual(t1, v2)
	}

	if t2, ok := v2.(time.Time); ok {
		return timeValueEqual(t2, v1)
	}

	if b1, ok := v1.([]byte); ok {
		return byteSliceEqual(b1, v2)
	}

	if b2, ok := v2.([]byte); ok {
		return byteSliceEqual(b2, v1)
	}

	var mv1, mv2 *types.MapValue
	switch v := v1.(type) {
	case *types.MapValue:
		mv1 = v
	case map[string]interface{}:
		mv1 = types.NewMapValue(v)
	}

	switch v := v2.(type) {
	case *types.MapValue:
		mv2 = v
	case map[string]interface{}:
		mv2 = types.NewMapValue(v)
	}

	switch {
	case mv1 != nil && mv2 != nil:
		return CompareMapValue(mv1, mv2, false)

	case mv1 == nil && mv2 == nil:
		value1 := reflect.ValueOf(v1)
		value2 := reflect.ValueOf(v2)

		if !value1.IsValid() || !value2.IsValid() {
			return value1.IsValid() == value2.IsValid()
		}

		k1 := value1.Kind()
		k2 := value2.Kind()
		if k1 != k2 {
			return false
		}

		switch k1 {
		case reflect.Array, reflect.Slice:
			if value1.Len() != value2.Len() {
				return false
			}

			for i := 0; i < value1.Len(); i++ {
				if !valueEqual(value1.Index(i).Interface(), value2.Index(i).Interface()) {
					return false
				}
			}
			return true

		default:
			return reflect.DeepEqual(v1, v2)
		}

	default:
		return false
	}
}

func byteSliceEqual(b1 []byte, v2 interface{}) bool {
	switch v2 := v2.(type) {
	case []byte:
		return reflect.DeepEqual(b1, v2)

	case string:
		// The string may represent the base64 encoding of a slice of bytes.
		b2, err := base64.StdEncoding.DecodeString(v2)
		if err != nil {
			return false
		}
		return reflect.DeepEqual(b1, b2)

	default:
		return false
	}
}

// binaryPrecision returns the closest binary precision for the specified decimal precision.
func binaryPrecision(decimalPrec uint) (prec uint) {
	switch decimalPrec {
	case 7:
		return 24
	case 16:
		return 53
	case 34:
		return 113
	default:
		return 0
	}
}

func jsonNumberEqual(n1 json.Number, v2 interface{}, fpArithSpec *nosqldb.FPArithSpec) bool {

	switch v2 := v2.(type) {
	case int, int8, int16, int32, int64:
		i64, err := n1.Int64()
		if err != nil {
			return false
		}
		return i64 == reflect.ValueOf(v2).Int()

	case float32, float64:
		f1, err := n1.Float64()
		if err != nil {
			return false
		}

		f2 := reflect.ValueOf(v2).Float()
		if f1 == f2 {
			return true
		}

		prec := binaryPrecision(fpArithSpec.Precision)
		bf1 := big.NewFloat(f1).SetPrec(prec).SetMode(fpArithSpec.RoundingMode)
		bf2 := big.NewFloat(f2).SetPrec(prec).SetMode(fpArithSpec.RoundingMode)
		return bf1.Cmp(bf2) == 0

	case uint8, uint16, uint32, uint64:
		i64, err := n1.Int64()
		if err != nil {
			return false
		}

		return uint64(i64) == reflect.ValueOf(v2).Uint()

	case *big.Rat:
		rat1, ok := new(big.Rat).SetString(n1.String())
		if !ok {
			return false
		}
		return ratValueEqual(rat1, v2, &nosqldb.Decimal32)

	case json.Number:
		rat1, ok := new(big.Rat).SetString(n1.String())
		if !ok {
			return false
		}

		rat2, ok := new(big.Rat).SetString(v2.String())
		if !ok {
			return false
		}

		return ratValueEqual(rat1, rat2, &nosqldb.Decimal32)

	default:
		return false
	}
}

func showRat(rat *big.Rat) string {
	bf1, _, _ := big.ParseFloat(rat.RatString(), 10, 24, big.ToNearestEven)
	return bf1.Text('G', 7)
}

func ratValueEqual(rat1, rat2 *big.Rat, fpArithSpec *nosqldb.FPArithSpec) bool {
	if rat1.Cmp(rat2) == 0 {
		return true
	}

	var err error
	var bf1, bf2 *big.Float

	prec := binaryPrecision(fpArithSpec.Precision)

	bf1, _, err = big.ParseFloat(rat1.RatString(), 10, prec, fpArithSpec.RoundingMode)
	if err != nil {
		return false
	}

	bf2, _, err = big.ParseFloat(rat2.RatString(), 10, prec, fpArithSpec.RoundingMode)
	if err != nil {
		return false
	}

	if bf1.Cmp(bf2) == 0 {
		return true
	}

	decimalPrec := int(fpArithSpec.Precision)
	if bf1.Text('G', decimalPrec) == bf2.Text('G', decimalPrec) {
		return true
	}

	return false
}

func timeValueEqual(t1 time.Time, v2 interface{}) bool {
	switch v2 := v2.(type) {
	case time.Time:
		return t1.Equal(v2)

	case string:
		t2, err := time.Parse(types.ISO8601Layout, v2)
		if err != nil {
			return false
		}
		return t1.Equal(t2)

	default:
		return false
	}
}
