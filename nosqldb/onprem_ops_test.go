//
// Copyright (c) 2019, 2023 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

// +build onprem

package nosqldb_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// OnPremTestSuite tests operations that are supported for on-premise NoSQL database server.
type OnPremTestSuite struct {
	*test.NoSQLTestSuite
}

// TestNamespacesOp tests create, drop and show namespaces operations.
func (suite *OnPremTestSuite) TestNamespacesOp() {
	namespaces := []string{"Ns01", "Ns02", "Ns03"}
	testCases := []struct {
		stmt       string
		expErrCode nosqlerr.ErrorCode // Expected error code.
	}{
		{"drop namespace if exists %s cascade", nosqlerr.NoError},
		{"create namespace %s", nosqlerr.NoError},
		{"create namespace %s", nosqlerr.ResourceExists},
		{"create namespace if not exists %s", nosqlerr.NoError},
		// Call ListNamespaces() after all namespaces are created.
		{"show namespaces", nosqlerr.NoError},
		{"drop namespace %s", nosqlerr.NoError},
		{"drop namespace %s", nosqlerr.ResourceNotFound},
		{"drop namespace if exists %s", nosqlerr.NoError},
	}

	var err error
	var nsResults []string

	for _, r := range testCases {
		// Show namespaces.
		if strings.HasPrefix(r.stmt, "show") {
			nsResults, err = suite.Client.ListNamespaces()
			if !suite.NoErrorf(err, "ListNamespaces() failed, got error: %v", r.stmt, err) {
				continue
			}
			for _, ns := range namespaces {
				suite.Containsf(nsResults, ns,
					"ListNamespaces(): namespace \"%s\" should have been included in the list.", ns)
			}

			continue
		}

		// Create/drop namespaces.
		for _, ns := range namespaces {
			ddl := fmt.Sprintf(r.stmt, ns)
			suite.doDDLTest(ddl, r.expErrCode)
		}
	}

	// Show namespaces again.
	nsResults, err = suite.Client.ListNamespaces()
	if suite.NoErrorf(err, "ListNamespaces() failed, got error: %v", err) {
		for _, ns := range namespaces {
			suite.NotContainsf(nsResults, ns,
				"ListNamespaces(): namespace \"%s\" should not be included in the list.", ns)
		}
	}

	// Verify namespace names are case-insensitive.
	suite.doDDLTest("drop namespace if exists NsABC", nosqlerr.NoError)
	suite.doDDLTest("create namespace NSabC", nosqlerr.NoError)
	suite.doDDLTest("create namespace nsABc", nosqlerr.ResourceExists)
	suite.doDDLTest("drop namespace NSABC", nosqlerr.NoError)
}

// TestUserRolesOp tests create/drop/show/list users, create/drop/show/list roles
// and grant/revoke roles operations.
//
// This test can only run with an on-premise secure kvstore.
func (suite *OnPremTestSuite) TestUserRolesOp() {
	if !test.IsOnPremSecureStore() {
		suite.T().Skip("TestUserRolesOp() test can only run with an on-premise secure store")
	}

	var err error
	var stmt string
	var roles []string
	var users []string

	roleNames := []string{"adminRole01", "Role01", "Role02", "Role03"}
	usernames := []string{"Admin01", "User01", "User02", "User03"}
	password := "NoSql00__123456"
	// Create users.
	for i, name := range usernames {
		if i == 0 {
			stmt = fmt.Sprintf("create user %s identified by %q admin", name, password)
		} else {
			stmt = fmt.Sprintf("create user %s identified by %q", name, password)
		}
		suite.doDDLTest(stmt, nosqlerr.NoError)

		// Select a user to test and verify usernames are case-sensitive.
		if i == len(usernames)-1 {
			stmt = "show user " + name
			suite.doDDLTest(stmt, nosqlerr.NoError)
			stmt = "show as json user " + name
			suite.doDDLTest(stmt, nosqlerr.NoError)
			stmt = "show user " + strings.ToUpper(name)
			suite.doDDLTest(stmt, nosqlerr.ResourceNotFound)
			stmt = "show as json user " + strings.ToLower(name)
			suite.doDDLTest(stmt, nosqlerr.ResourceNotFound)
		}
	}

	// List users.
	users = suite.getUserNames()
	for _, name := range usernames {
		suite.Containsf(users, name,
			"ListUsers(): user \"%s\" should have been included in the list.", name)
	}

	// Create and grant roles to users.
	for i, role := range roleNames {
		stmt = "create role " + role
		suite.doDDLTest(stmt, nosqlerr.NoError)
		stmt = fmt.Sprintf("grant %s to user %s", role, usernames[i])
		suite.doDDLTest(stmt, nosqlerr.NoError)

		// Select a role to test and verify role names are case-insensitive.
		if i == len(roleNames)-1 {
			stmt = "show role " + role
			suite.doDDLTest(stmt, nosqlerr.NoError)
			stmt = "show as json role " + role
			suite.doDDLTest(stmt, nosqlerr.NoError)
			stmt = "show role " + strings.ToUpper(role)
			suite.doDDLTest(stmt, nosqlerr.NoError)
			stmt = "show as json role " + strings.ToLower(role)
			suite.doDDLTest(stmt, nosqlerr.NoError)
		}
	}

	// List roles.
	// ListRoles() returns a list of names that are in lowercase.
	roles, err = suite.Client.ListRoles()
	if suite.NoErrorf(err, "ListRoles() failed, got error: %v", err) {
		for _, name := range roleNames {
			name = strings.ToLower(name)
			suite.Containsf(roles, name,
				"ListRoles(): role \"%s\" should have been included in the list.", name)
		}
	}

	// Revoke roles from users and drop roles.
	for i, role := range roleNames {
		stmt = fmt.Sprintf("revoke %s from user %s", role, usernames[i])
		suite.doDDLTest(stmt, nosqlerr.NoError)
		stmt = "drop role " + role
		suite.doDDLTest(stmt, nosqlerr.NoError)
	}

	// List roles.
	roles, err = suite.Client.ListRoles()
	if suite.NoErrorf(err, "ListRoles() failed, got error: %v", err) {
		for _, name := range roleNames {
			name = strings.ToLower(name)
			suite.NotContainsf(roles, name,
				"ListRoles(): role \"%s\" should not be included in the list.", name)
		}
	}

	// Drop users.
	for _, name := range usernames {
		stmt = fmt.Sprintf("drop user %s", name)
		suite.doDDLTest(stmt, nosqlerr.NoError)
	}

	// List users.
	users = suite.getUserNames()
	for _, name := range usernames {
		suite.NotContainsf(users, name,
			"ListUsers(): user \"%s\" should not be included in the list.", name)
	}
}

// TestChildTable tests create/drop, put/get/delete operations on child tables.
func (suite *OnPremTestSuite) TestChildTable() {
	suite.doChildTableTest("")
	// Test child tables in the specified namespace.
	suite.doChildTableTest("Ns001")
}

func (suite *OnPremTestSuite) doChildTableTest(ns string) {
	var stmt string
	var err error

	if len(ns) > 0 {
		stmt = "CREATE NAMESPACE IF NOT EXISTS " + ns
		suite.ExecuteDDL(stmt)
	}

	parentTable := suite.GetNsTableName(ns, "Parent")
	stmt = "CREATE TABLE IF NOT EXISTS " + parentTable +
		" (id INTEGER, pin INTEGER, name STRING, PRIMARY KEY(SHARD(pin), id))"
	suite.CreateTable(stmt, nil)

	childTable := parentTable + ".Child"
	stmt = "CREATE TABLE IF NOT EXISTS " + childTable +
		" (childId INTEGER, childName STRING, PRIMARY KEY(childId))"
	suite.CreateTable(stmt, nil)

	// Put a row.
	value := &types.MapValue{}
	value.Put("id", 1).Put("pin", 123456).Put("name", "test1")
	value.Put("childId", 1).Put("childName", "cname")

	putReq := &nosqldb.PutRequest{
		TableName: childTable,
		Value:     value,
	}
	putRes, err := suite.Client.Put(putReq)
	if suite.NoErrorf(err, "Put(table=%s) failed: %v", childTable, err) {
		suite.NotNilf(putRes.Version, "Put should have returned a non-nil Version")
	}

	// Get the row.
	key := &types.MapValue{}
	key.Put("id", 1).Put("pin", 123456).Put("childId", 1)

	getReq := &nosqldb.GetRequest{
		TableName: childTable,
		Key:       key,
	}
	getRes, err := suite.Client.Get(getReq)
	if suite.NoErrorf(err, "Get(table=%s, key=%v) failed: %v", childTable, key, err) {
		suite.Truef(getRes.RowExists(), "Get(table=%s, key=%v) failed: %v", childTable, key, err)
	}

	// Put with JSON.
	jsonStr := `{"id": 2, "pin": 13579, "name": "test2", "childId": 2, "childName": "cname2"}`
	value, err = types.NewMapValueFromJSON(jsonStr)
	suite.NoErrorf(err, "NewMapValueFromJSON(jsonStr=%q): %v", jsonStr, err)
	putReq = &nosqldb.PutRequest{
		TableName: childTable,
		Value:     value,
	}
	putRes, err = suite.Client.Put(putReq)
	if suite.NoErrorf(err, "Put(table=%s) failed: %v", childTable, err) {
		suite.NotNilf(putRes.Version, "Put should have returned a non-nil Version")
	}

	jsonStr = `{"id": 2, "pin": 13579, "childId": 2}`
	key, err = types.NewMapValueFromJSON(jsonStr)
	if suite.NoErrorf(err, "NewMapValueFromJSON(jsonStr=%q): %v", jsonStr, err) {
		getReq = &nosqldb.GetRequest{
			TableName: childTable,
			Key:       key,
		}
		getRes, err = suite.Client.Get(getReq)
		if suite.NoErrorf(err, "Get(table=%s, key=%v) failed: %v", childTable, key, err) {
			suite.Truef(getRes.RowExists(), "Get(table=%s, key=%v) failed: %v", childTable, key, err)
		}
	}

	// Query
	stmt = fmt.Sprintf("SELECT * FROM %s WHERE childName='%s'", childTable, "cname2")
	qReq := &nosqldb.QueryRequest{
		Statement: stmt,
	}

	results, err := suite.ExecuteQueryRequest(qReq)
	if suite.NoErrorf(err, "Query(stmt=%s) failed: %v", stmt, err) {
		if suite.Equalf(1, len(results), "unexpected number of results returned") {
			id, ok := results[0].GetInt("id")
			if suite.Truef(ok, "cannot find int value of \"id\" column") {
				suite.Equalf(2, id, "unexpected value of \"id\" column")
			}
		}
	}

	// Create an index on "childName" field.
	stmt = fmt.Sprintf("CREATE INDEX %s ON %s(%s)", "idx1", childTable, "childName")
	suite.ExecuteTableDDL(stmt)

	// Query by index field.
	stmt = fmt.Sprintf("SELECT * FROM %s WHERE childName='%s'", childTable, "cname2")
	qReq = &nosqldb.QueryRequest{
		Statement: stmt,
	}
	results, err = suite.ExecuteQueryRequest(qReq)
	if suite.NoErrorf(err, "Query(stmt=%s) failed: %v", stmt, err) {
		if suite.Equalf(1, len(results), "unexpected number of results returned") {
			id, ok := results[0].GetInt("id")
			if suite.Truef(ok, "cannot find int value of \"id\" column") {
				suite.Equalf(2, id, "unexpected value of \"id\" column")
			}
		}
	}

	// Delete a row.
	key = &types.MapValue{}
	key.Put("id", 1).Put("pin", 123456).Put("childId", 1)

	delReq := &nosqldb.DeleteRequest{
		TableName: childTable,
		Key:       key,
	}
	delRes, err := suite.Client.Delete(delReq)
	if suite.NoErrorf(err, "Delete(table=%s, key=%v) failed: %v", childTable, key, err) {
		suite.Truef(delRes.Success, "Delete(table=%s, key=%v) should have succeeded", childTable, key)
	}

	// Drop child table and then parent table.
	stmt = "DROP TABLE IF EXISTS " + childTable
	suite.ExecuteTableDDL(stmt)
	stmt = "DROP TABLE IF EXISTS " + parentTable
	suite.ExecuteTableDDL(stmt)
}

func (suite *OnPremTestSuite) doDDLTest(ddl string, expErrCode nosqlerr.ErrorCode) {
	ddlRes, err := suite.Client.DoSystemRequestAndWait(ddl, 30*time.Second, time.Second)
	switch expErrCode {
	case nosqlerr.NoError:
		if suite.NoErrorf(err, "\"%s\" should have succeeded, got error: %v", ddl, err) {
			suite.Equalf(types.Complete, ddlRes.State, "unexpected operation state.")
		}
		// Check ResultString and OperationID of DDLResult for "show ..." operations.
		if strings.HasPrefix(ddl, "show") {
			suite.NotEmptyf(ddlRes.ResultString, "\"%s\": DDLResult.ResultString should not be empty.", ddl)
			suite.Emptyf(ddlRes.OperationID, "\"%s\": DDLResult.OperationID should be empty.", ddl)
		}

	default:
		suite.Truef(nosqlerr.Is(err, expErrCode),
			"\"%s\" should have failed with error %v, got error: %v", ddl, expErrCode, err)
	}
}

func (suite *OnPremTestSuite) getUserNames() []string {
	users, err := suite.Client.ListUsers()
	if suite.NoErrorf(err, "ListUsers() failed, got error: %v", err) {
		names := make([]string, 0, len(users))
		for _, u := range users {
			names = append(names, u.Name)
		}
		return names
	}
	return nil
}

func TestOnPremOperations(t *testing.T) {
	test := &OnPremTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
