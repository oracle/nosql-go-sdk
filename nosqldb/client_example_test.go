//
// Copyright (c) 2019, 2020 Oracle and/or its affiliates.  All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//

package nosqldb_test

import (
	"fmt"
	"time"

	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/httputil"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
)

func ExampleNewClient_cloud() {
	// Specify configurations for the client that connects to the NoSQL cloud service.
	//
	// This assumes the required user credentials are specified in the
	// default OCI configuration file ~/.oci/config.
	//
	// [DEFAULT]
	// tenancy=<your-tenancy-id>
	// user=<your-user-id>
	// fingerprint=<fingerprint-of-your-public-key>
	// key_file=<path-to-your-private-key-file>
	// pass_phrase=<optional-passphrase>
	//
	cfg := nosqldb.Config{
		Region: "us-phoenix-1",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()
	// Perform database operations using client APIs.
	// ...
}

func ExampleNewClient_cloudSim() {
	// Specify configurations for the client that connects to the NoSQL cloud simulator.
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()
	// Perform database operations using client APIs.
	// ...
}

func ExampleNewClient_onPremise() {
	// Specify configurations for the client that connects to on-premise NoSQL servers.
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()
	// Perform database operations using client APIs.
	// ...
}

func ExampleClient_Get() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 100
	deptID := 90
	pk := &types.MapValue{}
	pk.Put("employee_id", empID).Put("department_id", deptID)

	req := &nosqldb.GetRequest{
		TableName: "employees",
		Key:       pk,
		Timeout:   3 * time.Second,
	}

	res, err := client.Get(req)
	if err != nil {
		fmt.Printf("Get() failed: %v\n", err)
		return
	}

	if res.RowExists() {
		fmt.Printf("got the row: %s\n", res.ValueAsJSON())
	} else {
		fmt.Printf("the requested row(employee_id=%d, department_id=%d) does not exist.\n", empID, deptID)
	}
}

func ExampleClient_Put() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 170
	deptID := 80
	value := &types.MapValue{}
	value.Put("employee_id", empID).Put("department_id", deptID)
	value.Put("first_name", "Tayler").Put("last_name", "Fox")
	value.Put("email", "TFOX@example.com").Put("phone_number", "011.44.1343.729268")
	value.Put("hire_date", "2006-01-24").Put("salary", 17000)

	req := &nosqldb.PutRequest{
		TableName: "employees",
		Value:     value,
		Timeout:   5 * time.Second,
	}

	res, err := client.Put(req)
	if err != nil {
		fmt.Printf("Put() failed: %v\n", err)
		return
	}

	if res.Success() {
		fmt.Printf("put row(employee_id=%d, department_id=%d) succeeded.\n", empID, deptID)
	} else {
		fmt.Printf("put row(employee_id=%d, department_id=%d) failed.\n", empID, deptID)
	}
}

func ExampleClient_Put_putIfPresent() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 170
	deptID := 80
	value := &types.MapValue{}
	value.Put("employee_id", empID).Put("department_id", deptID)
	value.Put("first_name", "Tayler").Put("last_name", "Fox")
	value.Put("email", "TFOX@example.com").Put("phone_number", "011.44.1343.729268")
	value.Put("hire_date", "2006-01-24").Put("salary", 19000)

	req := &nosqldb.PutRequest{
		TableName: "employees",
		Value:     value,
		Timeout:   5 * time.Second,
		PutOption: types.PutIfPresent,
	}

	res, err := client.Put(req)
	if err != nil {
		fmt.Printf("Put() failed: %v\n", err)
		return
	}

	if res.Success() {
		fmt.Printf("put row(option=PutIfPresent, employee_id=%d, department_id=%d) succeeded.\n", empID, deptID)
	} else {
		fmt.Printf("put row(option=PutIfPresent, employee_id=%d, department_id=%d) failed.\n", empID, deptID)
	}
}

func ExampleClient_Put_putIfAbsent() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 171
	deptID := 80
	value := &types.MapValue{}
	value.Put("employee_id", empID).Put("department_id", deptID)
	value.Put("first_name", "William").Put("last_name", "Smith")
	value.Put("email", "WSMITH@example.com").Put("phone_number", "011.44.1343.629268")
	value.Put("hire_date", "2007-02-23").Put("salary", 14000)

	req := &nosqldb.PutRequest{
		TableName: "employees",
		Value:     value,
		Timeout:   5 * time.Second,
		PutOption: types.PutIfAbsent,
		ReturnRow: true,
	}

	res, err := client.Put(req)
	if err != nil {
		fmt.Printf("Put() failed: %v\n", err)
		return
	}

	if res.Success() {
		fmt.Printf("put row(option=PutIfAbsent, employee_id=%d, department_id=%d) succeeded.\n", empID, deptID)
	} else {
		fmt.Printf("put row(option=PutIfAbsent, employee_id=%d, department_id=%d) failed.\n", empID, deptID)
	}
}

func ExampleClient_Put_putIfVersion() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 172
	deptID := 80
	pk := &types.MapValue{}
	pk.Put("employee_id", empID).Put("department_id", deptID)

	getReq := &nosqldb.GetRequest{
		TableName: "employees",
		Key:       pk,
		Timeout:   5 * time.Second,
	}

	getRes, err := client.Get(getReq)
	if err != nil {
		fmt.Printf("Get() failed: %v\n", err)
		return
	}

	currVersion := getRes.Version

	value := &types.MapValue{}
	value.Put("employee_id", empID).Put("department_id", deptID)
	value.Put("first_name", "Elizabeth").Put("last_name", "Bates")
	value.Put("email", "EBATES@example.com").Put("phone_number", "011.44.1343.529268")
	value.Put("hire_date", "2007-03-24").Put("salary", 15300)

	req := &nosqldb.PutRequest{
		TableName:    "employees",
		Value:        value,
		Timeout:      5 * time.Second,
		PutOption:    types.PutIfVersion,
		ReturnRow:    true,
		MatchVersion: currVersion,
	}

	res, err := client.Put(req)
	if err != nil {
		fmt.Printf("Put() failed: %v\n", err)
		return
	}

	if res.Success() {
		fmt.Printf("put row(option=PutIfVersion, employee_id=%d, department_id=%d) succeeded.\n", empID, deptID)
	} else {
		fmt.Printf("put row(option=PutIfVersion, employee_id=%d, department_id=%d) failed.\n", empID, deptID)
	}
}

func ExampleClient_Delete() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 100
	deptID := 90
	pk := &types.MapValue{}
	pk.Put("employee_id", empID).Put("department_id", deptID)

	req := &nosqldb.DeleteRequest{
		TableName: "employees",
		Key:       pk,
		Timeout:   5 * time.Second,
	}

	res, err := client.Delete(req)
	if err != nil {
		fmt.Printf("Delete() failed: %v\n", err)
		return
	}

	if res.Success {
		fmt.Printf("delete row(employee_id=%d, department_id=%d) succeeded.\n", empID, deptID)
	} else {
		fmt.Printf("delete row(employee_id=%d, department_id=%d) failed.\n", empID, deptID)
	}
}

func ExampleClient_Delete_deleteIfVersion() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	empID := 100
	deptID := 90
	pk := &types.MapValue{}
	pk.Put("employee_id", empID).Put("department_id", deptID)

	getReq := &nosqldb.GetRequest{
		TableName: "employees",
		Key:       pk,
		Timeout:   5 * time.Second,
	}

	getRes, err := client.Get(getReq)
	if err != nil {
		fmt.Printf("Get() failed: %v\n", err)
		return
	}

	currVersion := getRes.Version

	req := &nosqldb.DeleteRequest{
		TableName:    "employees",
		Key:          pk,
		Timeout:      5 * time.Second,
		MatchVersion: currVersion,
	}

	res, err := client.Delete(req)
	if err != nil {
		fmt.Printf("Delete() failed: %v\n", err)
		return
	}

	if res.Success {
		fmt.Printf("delete row(option=DeleteIfVersion, employee_id=%d, department_id=%d) succeeded.\n", empID, deptID)
	} else {
		fmt.Printf("delete row(option=DeleteIfVersion, employee_id=%d, department_id=%d) failed.\n", empID, deptID)
	}
}

func ExampleClient_Query() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	stmt := "declare $dept_id integer; " +
		"SELECT employee_id, first_name, last_name FROM employees WHERE department_id = $dept_id"
	prepReq := &nosqldb.PrepareRequest{
		Statement: stmt,
	}

	prepRes, err := client.Prepare(prepReq)
	if err != nil {
		fmt.Printf("Prepare(%s) failed: %v\n", stmt, err)
		return
	}

	queryReq := &nosqldb.QueryRequest{
		PreparedStatement: &prepRes.PreparedStatement,
	}
	queryReq.PreparedStatement.SetVariable("$dept_id", 90)

	var results []*types.MapValue
	for {
		queryRes, err := client.Query(queryReq)
		if err != nil {
			fmt.Printf("Query(%s) failed: %v\n", stmt, err)
			return
		}

		res, err := queryRes.GetResults()
		if err != nil {
			fmt.Printf("GetResults() failed: %v\n", err)
			return
		}

		results = append(results, res...)

		if queryReq.IsDone() {
			break
		}
	}

	fmt.Printf("got %d results\n", len(results))
	for _, r := range results {
		empID, _ := r.GetInt("employee_id")
		firstName, _ := r.GetString("firstName")
		lastName, _ := r.GetString("lastName")
		fmt.Printf("%d: %s %s\n", empID, firstName, lastName)
	}
}

func ExampleClient_MultiDelete() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	deptID := 90
	sk := &types.MapValue{}
	sk.Put("department_id", deptID)

	req := &nosqldb.MultiDeleteRequest{
		TableName: "employees",
		Key:       sk,
		Timeout:   5 * time.Second,
	}

	res, err := client.MultiDelete(req)
	if err != nil {
		fmt.Printf("MultiDelete() failed: %v\n", err)
		return
	}

	fmt.Printf("number of rows deleted: %d\n", res.NumDeleted)
}

func ExampleClient_WriteMultiple() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	// Sample table:
	//
	// CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING,
	// email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE,
	// department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))
	type emp struct {
		empID       int
		firstName   string
		lastName    string
		email       string
		phoneNumber string
		hireDate    time.Time
		salary      float64
		deptID      int
	}

	formerEmployees := []*emp{
		&emp{empID: 101, deptID: 80},
		&emp{empID: 107, deptID: 80},
		&emp{empID: 109, deptID: 80},
	}

	newEmployees := []*emp{
		&emp{
			empID:       211,
			deptID:      80,
			firstName:   "Tayler",
			lastName:    "Fox",
			email:       "TFOX@example.com",
			phoneNumber: "011.44.1343.729268",
			hireDate:    time.Date(2019, time.October, 1, 10, 0, 0, 0, time.UTC),
			salary:      17000,
		},
		&emp{
			empID:       212,
			deptID:      80,
			firstName:   "William",
			lastName:    "Smith",
			email:       "WSMITH@example.com",
			phoneNumber: "011.44.1343.629268",
			hireDate:    time.Date(2019, time.October, 3, 10, 0, 0, 0, time.UTC),
			salary:      17000,
		},
	}

	wmReq := &nosqldb.WriteMultipleRequest{
		TableName: "employees",
		Timeout:   10 * time.Second,
	}

	for _, e := range formerEmployees {
		pk := &types.MapValue{}
		pk.Put("employee_id", e.empID).Put("department_id", e.deptID)
		delReq := &nosqldb.DeleteRequest{
			TableName: "employees",
			Key:       pk,
		}

		wmReq.AddDeleteRequest(delReq, true)
	}

	for _, e := range newEmployees {
		value := &types.MapValue{}
		value.Put("employee_id", e.empID).Put("department_id", e.deptID)
		value.Put("first_name", e.firstName).Put("last_name", e.lastName)
		value.Put("email", e.email).Put("phone_number", e.phoneNumber)
		value.Put("hire_date", e.hireDate).Put("salary", e.salary)
		putReq := &nosqldb.PutRequest{
			TableName: "employees",
			Value:     value,
			PutOption: types.PutIfAbsent,
			ReturnRow: true,
		}

		wmReq.AddPutRequest(putReq, true)
	}

	wmRes, err := client.WriteMultiple(wmReq)
	if err != nil {
		fmt.Printf("WriteMultiple() failed: %v\n", err)
		return
	}

	if wmRes.IsSuccess() {
		fmt.Printf("WriteMultiple() succeeded\n")
	} else {
		fmt.Printf("WriteMultiple() failed\n")
	}
}

func ExampleClient_DoTableRequest() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	stmt := "CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING," +
		"email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE," +
		"department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))"

	req := &nosqldb.TableRequest{
		Statement: stmt,
		TableLimits: &nosqldb.TableLimits{
			ReadUnits:  200,
			WriteUnits: 200,
			StorageGB:  2,
		},
		Timeout: 5 * time.Second,
	}

	res, err := client.DoTableRequest(req)
	if err != nil {
		fmt.Printf("DoTableRequest() failed: %v\n", err)
		return
	}

	res, err = res.WaitForCompletion(client, 10*time.Second, 2*time.Second)
	if err != nil {
		fmt.Printf("failed to create table \"employees\": %v\n", err)
		return
	}

	fmt.Printf("table \"employees\" has been created. Table state: %s\n", res.State)
}

func ExampleClient_DoTableRequestAndWait_createTable() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	stmt := "CREATE TABLE employees (employee_id INTEGER, first_name STRING, last_name STRING," +
		"email STRING, phone_number STRING, hire_date TIMESTAMP(0), salary DOUBLE," +
		"department_id INTEGER, PRIMARY KEY(shard(department_id), employee_id))"

	req := &nosqldb.TableRequest{
		Statement: stmt,
		TableLimits: &nosqldb.TableLimits{
			ReadUnits:  200,
			WriteUnits: 200,
			StorageGB:  2,
		},
		Timeout: 3 * time.Second,
	}

	res, err := client.DoTableRequestAndWait(req, 10*time.Second, 2*time.Second)
	if err != nil {
		fmt.Printf("DoTableRequestAndWait(): failed to create table \"employees\": %v\n", err)
		return
	}

	fmt.Printf("table \"employees\" has been created. Table state: %s\n", res.State)
}

func ExampleClient_DoTableRequestAndWait_changeTableLimits() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	req := &nosqldb.TableRequest{
		TableName: "employees",
		TableLimits: &nosqldb.TableLimits{
			ReadUnits:  400,
			WriteUnits: 400,
			StorageGB:  2,
		},
		Timeout: 3 * time.Second,
	}

	_, err = client.DoTableRequestAndWait(req, 10*time.Second, 2*time.Second)
	if err != nil {
		fmt.Printf("DoTableRequestAndWait(): failed to change table limits for table \"employees\": %v\n", err)
		return
	}

	fmt.Println("table limits for \"employees\" has been changed.")
}

func ExampleClient_DoTableRequestAndWait_dropTable() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	stmt := "DROP TABLE IF EXISTS employees"

	req := &nosqldb.TableRequest{
		Statement: stmt,
		Timeout:   3 * time.Second,
	}

	_, err = client.DoTableRequestAndWait(req, 10*time.Second, 2*time.Second)
	if err != nil {
		fmt.Printf("DoTableRequestAndWait(): failed to drop table \"employees\": %v\n", err)
		return
	}

	fmt.Println("table \"employees\" has been dropped.")
}

func ExampleClient_GetTable() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	req := &nosqldb.GetTableRequest{
		TableName: "employees",
		Timeout:   3 * time.Second,
	}

	res, err := client.GetTable(req)
	if err != nil {
		fmt.Printf("GetTable() failed: %v\n", err)
		return
	}

	fmt.Printf("got table info for \"employees\": state=%s, limits=%v\n", res.State, res.Limits)
}

func ExampleClient_GetIndexes() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	req := &nosqldb.GetIndexesRequest{
		TableName: "employees",
		Timeout:   3 * time.Second,
	}

	res, err := client.GetIndexes(req)
	if err != nil {
		fmt.Printf("GetIndexes(): failed to get indexes on table \"employees\": %v\n", err)
		return
	}

	fmt.Printf("number of indexes on table \"employees\": %d\n", len(res.Indexes))
	for i, idx := range res.Indexes {
		fmt.Printf("%d: index name: %s\n", i+1, idx.IndexName)
	}
}

func ExampleClient_ListTables() {
	cfg := nosqldb.Config{
		Mode:     "cloudsim",
		Endpoint: "http://localhost:8080",
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	req := &nosqldb.ListTablesRequest{
		Timeout: 3 * time.Second,
	}

	res, err := client.ListTables(req)
	if err != nil {
		fmt.Printf("ListTables() failed: %v\n", err)
		return
	}

	fmt.Printf("number of tables available: %d\n", len(res.Tables))
	for i, table := range res.Tables {
		fmt.Printf("%d: table name: %s\n", i+1, table)
	}
}

func ExampleClient_DoSystemRequest() {
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	req := &nosqldb.SystemRequest{
		Statement: "CREATE NAMESPACE ns01",
		Timeout:   3 * time.Second,
	}

	res, err := client.DoSystemRequest(req)
	if err != nil {
		fmt.Printf("DoSystemRequest(): failed to create namespace \"ns01\": %v\n", err)
		return
	}

	_, err = res.WaitForCompletion(client, 10*time.Second, 2*time.Second)
	if err != nil {
		fmt.Printf("failed to create namespace \"ns01\": %v\n", err)
		return
	}

	fmt.Printf("namespace \"ns01\" has been created\n")
}

func ExampleClient_DoSystemRequestAndWait() {
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	stmt := "CREATE USER user01 IDENTIFIED BY \"F;0s2M0;-Tdr\""
	_, err = client.DoSystemRequestAndWait(stmt, 10*time.Second, 2*time.Second)
	if err != nil {
		fmt.Printf("DoSystemRequest(): failed to create user \"user01\": %v\n", err)
		return
	}

	fmt.Printf("user \"user01\" has been created\n")
}

func ExampleClient_GetSystemStatus() {
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	sysReq := &nosqldb.SystemRequest{
		Statement: "CREATE ROLE role01",
		Timeout:   3 * time.Second,
	}

	sysRes, err := client.DoSystemRequest(sysReq)
	if err != nil {
		fmt.Printf("DoSystemRequest(): failed to create role \"role01\": %v\n", err)
		return
	}

	req := &nosqldb.SystemStatusRequest{
		OperationID: sysRes.OperationID,
		Timeout:     3 * time.Second,
	}

	sysRes, err = client.GetSystemStatus(req)
	if err != nil {
		fmt.Printf("GetSystemStatus() failed: %v\n", err)
		return
	}

	fmt.Printf("current state of system request for the \"CREATE ROLE role01\" operation: %s\n", sysRes.State)
}

func ExampleClient_ListNamespaces() {
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	namespaces, err := client.ListNamespaces()
	if err != nil {
		fmt.Printf("ListNamespaces() failed: %v\n", err)
		return
	}

	fmt.Printf("number of namespaces available: %d\n", len(namespaces))
	for i, ns := range namespaces {
		fmt.Printf("%d: namespace name: %s\n", i+1, ns)
	}
}

func ExampleClient_ListRoles() {
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	roles, err := client.ListRoles()
	if err != nil {
		fmt.Printf("ListRoles() failed: %v\n", err)
		return
	}

	fmt.Printf("number of roles available: %d\n", len(roles))
	for i, ns := range roles {
		fmt.Printf("%d: role name: %s\n", i+1, ns)
	}
}

func ExampleClient_ListUsers() {
	cfg := nosqldb.Config{
		Endpoint: "https://localhost:8080",
		Mode:     "onprem",
		Username: "testUser",
		Password: []byte("F;0s2M0;-Tdr"),
		// Specify InsecureSkipVerify to skip verification for server certificate.
		// This is used for testing, not recommended for production use.
		HTTPConfig: httputil.HTTPConfig{
			InsecureSkipVerify: true,
		},
	}

	client, err := nosqldb.NewClient(cfg)
	if err != nil {
		fmt.Printf("failed to create a NoSQL client: %v\n", err)
		return
	}
	defer client.Close()

	users, err := client.ListUsers()
	if err != nil {
		fmt.Printf("ListUsers() failed: %v\n", err)
		return
	}

	fmt.Printf("number of users available: %d\n", len(users))
	for i, u := range users {
		fmt.Printf("%d: username: %s\n", i+1, u.Name)
	}
}
