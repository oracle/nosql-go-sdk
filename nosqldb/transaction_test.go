package nosqldb_test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/oracle/nosql-go-sdk/internal/test"
	"github.com/oracle/nosql-go-sdk/nosqldb"
	"github.com/oracle/nosql-go-sdk/nosqldb/nosqlerr"
	"github.com/oracle/nosql-go-sdk/nosqldb/types"
	"github.com/stretchr/testify/suite"
)

// TransactionTestSuite contains tests for transaction APIs.

type TransactionTestSuite struct {
	*test.NoSQLTestSuite
	table      string
	childTable string
	fooTable   string
	queryAll   string
	queryBySid string
	verbose    bool
}

func (suite *TransactionTestSuite) SetupSuite() {
	suite.NoSQLTestSuite.SetupSuite()
	suite.table = suite.GetTableName("TransactionTest")
	// Create a test table.
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"sid INTEGER, id INTEGER, i0 INTEGER, i1 INTEGER, s STRING, "+
		"PRIMARY KEY(SHARD(sid), id))", suite.table)
	limits := &nosqldb.TableLimits{
		ReadUnits:  200,
		WriteUnits: 200,
		StorageGB:  1,
	}
	suite.CreateTable(stmt, limits)

	// Create a child table
	suite.childTable = suite.GetTableName("TransactionTest.child")
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"cid INTEGER, ci0 INTEGER, ci1 INTEGER, "+
		"PRIMARY KEY(cid))", suite.childTable)
	suite.CreateTable(stmt, nil)

	// Create table foo, not belongs to table heriarchy
	suite.fooTable = suite.GetTableName("foo")
	stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"id INTEGER, s STRING, PRIMARY KEY(id))", suite.fooTable)
	suite.CreateTable(stmt, limits)

	// Common queries
	suite.queryAll = fmt.Sprintf("select * from %s", suite.table)
	suite.queryBySid = "select * from %s where sid = %d"

	// flag to turn on the verbose output
	suite.verbose = true
}

func (s *TransactionTestSuite) TearDownTest() {
	s.deleteAll()
}

// Test Put/Get/Delete APIs with different operation options.
func (suite *TransactionTestSuite) TestBasic() {
	sid := 0
	queryBySid := suite.stmtQueryBySid(sid)

	operations := []Operation{
		&Put{
			value:         suite.row(sid, 0),
			shouldSucceed: true,
		},
		&Get{
			key:       suite.key(sid, 0),
			keyExists: true,
		},
		&Delete{
			key:       suite.key(sid, 1),
			keyExists: false,
		},
		&Put{
			value:         suite.row(sid, 0),
			shouldSucceed: true,
		},
		&Put{
			value:         suite.row(sid, 1),
			shouldSucceed: true,
		},
		&MultiDelete{
			key:        suite.shardKey(sid),
			numDeleted: 2,
		},
		&Get{
			key:       suite.key(sid, 1),
			keyExists: false,
		},
	}
	suite.executeAndCommit(operations)

	// delete all
	suite.deleteTable(suite.table)

	// abort transaction
	operations = []Operation{
		&Put{
			value:         suite.row(sid, 0),
			shouldSucceed: true,
		},
		&Put{
			value:         suite.row(sid, 1),
			shouldSucceed: true,
		},
		&Query{
			statement: queryBySid,
			numRows:   2,
		},
	}
	suite.executeAndAbort(operations)

	suite.query(queryBySid, 0)
}

func (suite *TransactionTestSuite) TestPutGetDeletes() {
	numIdsPerSid := 3
	sid := 0

	//
	// Put
	//

	// concurrent puts and commit the transaction
	operations := []Operation{}
	for id := 0; id < numIdsPerSid; id++ {
		put := &Put{
			value:         suite.row(sid, id),
			shouldSucceed: true,
		}
		operations = append(operations, put)
	}

	for id := numIdsPerSid; id < numIdsPerSid+2; id++ {
		put := &Put{
			value:         suite.row(sid, id),
			shouldSucceed: false,
			option:        types.PutIfPresent,
		}
		operations = append(operations, put)
	}

	suite.executeOperations(operations, true, true)
	suite.query(suite.stmtQueryBySid(sid), numIdsPerSid)

	// concurrent puts and abort the transaction
	suite.deleteTable(suite.table)
	suite.executeOperations(operations, true, false)
	suite.query(suite.stmtQueryBySid(sid), 0)

	//
	// Delete
	//

	suite.loadRows(sid, numIdsPerSid)
	suite.loadRows(sid+1, numIdsPerSid)

	operations = nil
	// delete rows with sid = 1
	for id := 0; id < numIdsPerSid; id++ {
		delete := &Delete{
			key:       suite.key(sid, id),
			keyExists: true,
		}
		operations = append(operations, delete)
	}

	// delete rows that do not exists
	for id := numIdsPerSid; id < numIdsPerSid+2; id++ {
		delete := &Delete{
			key:       suite.key(sid, id),
			keyExists: false,
		}
		operations = append(operations, delete)
	}

	// run deletes concurrently and commit
	suite.executeOperations(operations, true, true)
	suite.query(suite.stmtQueryBySid(sid), 0)

	// run deletes concurrently and rollback
	suite.loadRows(sid, numIdsPerSid)
	suite.executeOperations(operations, true, false)
	suite.query(suite.stmtQueryBySid(sid), numIdsPerSid)

	//
	// Get
	//
	operations = nil
	for id := 0; id < numIdsPerSid; id++ {
		get := &Get{
			key:       suite.key(sid, id),
			keyExists: true,
		}
		operations = append(operations, get)
	}
	suite.executeOperations(operations, true, true)

	// Mixed put, get and delete
	suite.deleteTable(suite.table)
	operations = nil
	key0 := suite.key(sid, 0)
	row0 := suite.row(sid, 0)
	row1 := suite.row(sid, 1)

	operations = []Operation{
		&Get{
			key:       key0,
			keyExists: false,
		},
		&Put{
			value:         row0,
			shouldSucceed: true,
		},
		&Put{
			value:         row1,
			shouldSucceed: true,
		},
		&Get{
			key:       key0,
			keyExists: true,
		},
		&Delete{
			key:       key0,
			keyExists: true,
		},
		&Get{
			key:       key0,
			keyExists: false,
		},
		&Put{
			value:         row0,
			shouldSucceed: false,
			option:        types.PutIfPresent,
		},
		&Get{
			key:       key0,
			keyExists: false,
		},
		&Put{
			value:         row0,
			shouldSucceed: true,
			option:        types.PutIfAbsent,
		},
		&Get{
			key:       key0,
			keyExists: true,
		},
		&Delete{
			key:       key0,
			keyExists: true,
		},
		&Get{
			key:       key0,
			keyExists: false,
		},
	}

	suite.executeAndCommit(operations)
	suite.query(suite.stmtQueryBySid(sid), 1)

	suite.deleteTable(suite.table)
	suite.executeAndAbort(operations)
	suite.query(suite.stmtQueryBySid(sid), 0)
}

func (suite *TransactionTestSuite) TestConcurrentOps() {
	sid := 0
	numRows := 100

	operations := []Operation{}
	for i := 0; i < numRows; i++ {
		put := &Put{
			value:         suite.row(sid, i),
			shouldSucceed: true,
		}
		operations = append(operations, put)
	}
	suite.executeOperations(operations, true, true)
	suite.query(suite.stmtQueryBySid(sid), numRows)
}

func (suite *TransactionTestSuite) TestMultiDelete() {
	sid := 0
	numIdsPerSid := 3

	suite.loadRows(0, numIdsPerSid)
	suite.loadRows(1, numIdsPerSid)

	operations := []Operation{
		&MultiDelete{
			key:        suite.shardKey(sid),
			numDeleted: numIdsPerSid,
		},
		&Query{
			statement: suite.stmtQueryBySid(sid),
			numRows:   0,
		},
	}

	// Run operations and commit the transaction
	suite.executeAndCommit(operations)
	suite.query(suite.stmtQueryBySid(sid), 0)

	suite.loadRows(0, numIdsPerSid)

	// Run operations and commit the transaction
	suite.executeAndAbort(operations)
	suite.query(suite.stmtQueryBySid(sid), numIdsPerSid)
}

func (suite *TransactionTestSuite) TestWriteMultiple() {
	sid := 0
	numIdsPerSid := 3
	numCidsPerId := 1

	suite.loadRows(sid, numIdsPerSid)
	suite.loadRows(sid+1, numIdsPerSid)
	suite.loadChildRows(sid, numIdsPerSid, numCidsPerId)

	// Run operations and commit the transaction
	operations := []Operation{
		&WriteMultiple{
			operations: []Operation{
				&Put{
					value:         suite.row(sid, 0),
					shouldSucceed: true,
				},
				&Put{
					value:         suite.row(sid, 1),
					shouldSucceed: false,
					option:        types.PutIfAbsent,
				},
				&Delete{
					key:       suite.key(sid, 2),
					keyExists: true,
				},
				&Put{
					table:         suite.childTable,
					value:         suite.childRow(sid, 0, 1),
					option:        types.PutIfAbsent,
					shouldSucceed: true,
				},
				&Delete{
					table:     suite.childTable,
					key:       suite.childKey(sid, 0, 0),
					keyExists: true,
				},
				&Delete{
					table:     suite.childTable,
					key:       suite.childKey(sid, 1, 0),
					keyExists: true,
				},
			},
		},
		&Get{
			key:       suite.key(sid, 0),
			keyExists: true,
		},
		&Get{
			key:       suite.key(sid, 2),
			keyExists: false,
		},
		&Get{
			table:     suite.childTable,
			key:       suite.childKey(sid, 0, 1),
			keyExists: true,
		},
		&Get{
			table:     suite.childTable,
			key:       suite.childKey(sid, 0, 0),
			keyExists: false,
		},
		&Get{
			table:     suite.childTable,
			key:       suite.childKey(sid, 1, 0),
			keyExists: false,
		},
	}
	suite.executeAndCommit(operations)
	suite.query(suite.stmtQueryBySid(sid), 2)
	suite.query(suite.stmtQueryBySidTable(suite.childTable, sid), 2)

	// Run operations and abort the transaction
	suite.deleteAll()
	suite.loadRows(sid, numIdsPerSid)
	suite.loadRows(sid+1, numIdsPerSid)
	suite.loadChildRows(sid, numIdsPerSid, numCidsPerId)

	suite.executeAndAbort(operations)
	suite.query(suite.stmtQueryBySid(sid), numIdsPerSid)
	suite.query(suite.stmtQueryBySidTable(suite.childTable, sid), numIdsPerSid)
}

func (suite *TransactionTestSuite) TestQuery() {
	sid := 0
	numIdsPerSid := 3
	newId := 10

	selectBySid := suite.stmtQueryBySid(sid)
	selectOne := "declare $sid integer; $id integer; " +
		"select * from " + suite.table + " where sid = $sid and id = $id"
	insert := "declare $sid integer; $id integer; $i0 integer; $i1 integer; $s string; " +
		"insert into " + suite.table + " values($sid, $id, $i0, $i1, $s)"
	update := "update " + suite.table + " set i0 = i1 + 100 where sid = " + strconv.Itoa(sid)
	delete := "declare $sid integer; $id integer; delete from " + suite.table +
		" where sid = $sid and id = $id"

	suite.loadRows(sid, numIdsPerSid)
	suite.loadRows(sid+1, numIdsPerSid)

	operations := []Operation{
		&Query{
			statement: selectBySid,
			numRows:   numIdsPerSid,
		},
		&Query{
			statement: insert,
			numRows:   1,
			bindVars: map[string]interface{}{
				"$sid": sid,
				"$id":  newId,
				"$i0":  newId,
				"$i1":  newId,
				"$s":   "s" + strconv.Itoa(newId),
			},
		},
		&Query{
			statement: selectOne,
			numRows:   1,
			bindVars: map[string]interface{}{
				"$sid": sid,
				"$id":  newId,
			},
		},
		&Query{
			statement: update,
			numRows:   4,
		},
		&Query{
			statement: delete,
			numRows:   1,
			bindVars: map[string]interface{}{
				"$sid": 0,
				"$id":  0,
			},
		},
		&Query{
			statement: selectBySid,
			numRows:   numIdsPerSid,
		},
		&Query{
			statement: delete,
			numRows:   1,
			bindVars: map[string]interface{}{
				"$sid": 0,
				"$id":  1,
			},
		},
	}
	/* Run ops and commit */
	suite.executeAndCommit(operations)
	suite.query(selectBySid, numIdsPerSid-1)

	/* Run ops and abort */
	suite.deleteTable(suite.table)
	suite.loadRows(sid, numIdsPerSid)
	suite.loadRows(sid+1, numIdsPerSid)

	suite.executeAndAbort(operations)
	suite.query(selectBySid, numIdsPerSid)
}

// Tests the prepare operation within a transaction.
//
// This test case covers various scenarios:
// - Fail cases
//  1. Prepare a query on a table different from the transaction's table.
//  2. Prepare a query without shard key in where clause
//  3. Prepare a query using a shard key different from the transaction’s
//     shard key.
//
// - Prepare won't be the binding operation
func (suite *TransactionTestSuite) TestPrepare() {
	sid := 0
	numIdsPerSid := 3
	selectBySid100 := suite.stmtQueryBySid(100)

	suite.loadRows(sid, numIdsPerSid)
	suite.loadRows(sid+1, numIdsPerSid)

	txn := suite.beginTransaction()

	var prep *Prepare
	var query *Query
	// Prepare a query on different table, it should fail.
	prep = &Prepare{
		statement: "select * from " + suite.fooTable,
		errCode:   nosqlerr.IllegalArgument,
	}
	prep.perform(suite, txn)

	// Prepare a query without shard key specified in where clause, it
	// should fail.
	prep = &Prepare{
		statement: "select * from " + suite.table,
		errCode:   nosqlerr.IllegalArgument,
	}
	prep.perform(suite, txn)

	// Prepare a query with shard key [sid=100].
	// Operation succeeds, but it won't be the binding operation
	prep = &Prepare{
		statement: selectBySid100,
	}
	prep.perform(suite, txn)

	// Get operation, shard key [sid=0].
	// Operation succeeds, this is the binding operation of the transaction
	get := &Get{
		key:       suite.key(sid, 0),
		keyExists: true,
	}
	get.perform(suite, txn)

	// Execute the query prepared after the binding operation
	// Operation should fail because its shard key does not match the
	// transaction’s shard key.
	query = &Query{
		statement: selectBySid100,
		errCode:   nosqlerr.IllegalArgument,
	}
	query.perform(suite, txn)

	//  Prepare the query with shard key[sid=100]
	//  Operation should fail due to a shard-key mismatch between the query
	//  and the transaction.
	prep = &Prepare{
		statement: selectBySid100,
		errCode:   nosqlerr.IllegalArgument,
	}
	prep.perform(suite, txn)

	// Query with shard key[sid=0]
	query = &Query{
		statement: suite.stmtQueryBySid(sid),
		numRows:   numIdsPerSid,
	}
	query.perform(suite, txn)

	suite.abortTransaction(txn)
}

// Tests the execution of various invalid queries within a transaction.
// The test case covers different scenarios where queries are expected to
// fail due to various reasons.
func (suite *TransactionTestSuite) TestInvalidQueryInTxn() {
	sid := 0
	queries := []string{
		// missing shard key in the WHERE clause
		"select * from " + suite.table,
		// shard key using operators other than '='
		"select * from " + suite.table + " where sid > 0",
		"select * from " + suite.table + " where sid != 1",
		// missing shard key in the WHERE clause
		"select * from " + suite.table + " $t where partition($t) = 1",
		// missing shard key in the WHERE clause
		"select count(*) from " + suite.table + " group by sid",

		// missing shard key in the WHERE clause
		"delete from " + suite.table + " where id = 0",
		// multiple shard keys */
		"update " + suite.table + " set s = 'update' || s " +
			"where sid = 1 or sid = 2",

		// inner join, missing shard key of 1st table
		"select * from " + suite.table + " p, " + suite.childTable + " c " +
			"where p.sid = c.sid and c.sid = 1",

		// nested table, missing shard key of the target table
		"select * from nested tables(" +
			suite.table + " p descendants(" + suite.childTable + " c)) " +
			"where c.sid = 1",

		// left outer join, missing shard key of the target table
		"select * from " +
			suite.table + " p left outer join " + suite.childTable + " c " +
			"on p.sid = c.sid and p.id = c.id " +
			"where c.sid = 1",

		// query on a table different from transaction's table
		"select * from " + suite.fooTable + " where id = 0",
	}

	operations := []Operation{
		&Query{
			statement: suite.stmtQueryBySid(sid), // binding op
			numRows:   0,
		},
	}
	for _, query := range queries {
		operations = append(operations,
			&Query{
				statement: query,
				errCode:   nosqlerr.IllegalArgument,
			},
		)
	}
	suite.executeAndCommit(operations)
}

// Tests the execution of various invalid operations within a transaction:
//   - The Request table is not in the hierarchy of transaction table
//   - The shard key of this request is different from the transaction’s
//     shard key.
func (suite *TransactionTestSuite) TestInvalidOpInTxn() {
	key0 := suite.key(0, 0)
	row0 := suite.row(0, 0)
	key1 := suite.key(1, 0)
	row1 := suite.row(1, 0)
	queryFooById := "select * from " + suite.fooTable + " where id = 0"
	queryBySid1 := suite.stmtQueryBySid(1)

	expErrCode := nosqlerr.IllegalArgument

	operations := []Operation{
		// The Request table is not in the hierarchy of transaction table
		&Put{
			value:         row0,
			option:        types.PutIfAbsent,
			shouldSucceed: true,
		},
		&Get{
			table:   suite.fooTable,
			key:     key0,
			errCode: expErrCode,
		},
		&Put{
			table:   suite.fooTable,
			value:   row0,
			errCode: expErrCode,
		},
		&Delete{
			table:   suite.fooTable,
			key:     key0,
			errCode: expErrCode,
		},
		&MultiDelete{
			table:   suite.fooTable,
			key:     key0,
			errCode: expErrCode,
		},
		&WriteMultiple{
			table: suite.fooTable,
			operations: []Operation{
				&Put{
					table: suite.fooTable,
					value: row0,
				},
				&Delete{
					table: suite.fooTable,
					key:   key0,
				},
			},
			errCode: expErrCode,
		},
		&Prepare{
			statement: queryFooById,
			errCode:   expErrCode,
		},
		&Query{
			statement: queryFooById,
			errCode:   expErrCode,
		},
		&Query{
			table:     suite.fooTable,
			statement: queryFooById,
			errCode:   expErrCode,
		},

		// The shard key of this request is different from the transaction’s
		// shard key.
		&Get{
			key:     key1,
			errCode: expErrCode,
		},
		&Put{
			value:   row1,
			errCode: expErrCode,
		},
		&Delete{
			key:     key1,
			errCode: expErrCode,
		},
		&MultiDelete{
			key:     key1,
			errCode: expErrCode,
		},
		&WriteMultiple{
			operations: []Operation{
				&Put{
					value: row1,
				},
				&Delete{
					key: key1,
				},
			},
			errCode: expErrCode,
		},
		&Prepare{
			statement: queryBySid1,
			errCode:   expErrCode,
		},
		&Query{
			statement: queryBySid1,
			errCode:   expErrCode,
		},
	}

	suite.executeAndCommit(operations)
}

func (suite *TransactionTestSuite) TestBindToTxn() {
	sid := 1
	txn := suite.beginTransaction()

	requests := []nosqldb.Request{}
	requests = append(requests, &nosqldb.PutRequest{
		Value:       suite.row(sid, 0),
		TableName:   suite.table,
		Transaction: txn,
	})
	requests = append(requests, &nosqldb.GetRequest{
		Key:         suite.key(sid, 0),
		TableName:   suite.table,
		Transaction: txn,
	})
	requests = append(requests, &nosqldb.DeleteRequest{
		Key:         suite.key(sid, 0),
		TableName:   suite.table,
		Transaction: txn,
	})
	requests = append(requests, &nosqldb.MultiDeleteRequest{
		Key:         suite.shardKey(sid),
		TableName:   suite.table,
		Transaction: txn,
	})
	wrReq := &nosqldb.WriteMultipleRequest{
		TableName:   suite.table,
		Transaction: txn,
	}
	wrReq.AddPutRequest(
		&nosqldb.PutRequest{
			Value:     suite.row(sid, 0),
			TableName: suite.table,
		},
		false,
	)

	requests = append(requests, wrReq)
	requests = append(requests, &nosqldb.PrepareRequest{
		Statement:   suite.stmtQueryBySid(sid),
		Transaction: txn,
	})
	requests = append(requests, &nosqldb.QueryRequest{
		Statement:   suite.stmtQueryBySid(sid),
		Transaction: txn,
	})

	var counter int32
	var bindOp *nosqldb.TransactionalRequest
	var wg sync.WaitGroup
	for _, op := range requests {
		wg.Add(1)
		go func(op nosqldb.Request) {
			defer wg.Done()
			treq, _ := op.(nosqldb.TransactionalRequest)
			if nosqldb.TryBindWithTransaction(treq) {
				atomic.AddInt32(&counter, 1)
				bindOp = &treq
			}
		}(op)
	}
	wg.Wait()

	suite.Truef(counter == 1, "Only one request could be bound with txn, but got %d", counter)
	suite.Truef(nosqldb.TransactionBoundWithOp(txn), "Transaction should have been bound with a op")

	nosqldb.UnbindFromTransaction(*bindOp)
	suite.Falsef(nosqldb.TransactionBoundWithOp(txn), "Transaction should have been unbound from the op")
}

// Tests the behavior of a transaction when a binding operation fails.
//
// If the binding operation fails, it is unbound from the transaction and
// the next eligible operation becomes the binding operation.
//
// In this test, a put operation with an invalid row is used, which is
// expected to fail. The test is executed multiple times to ensure that
// the invalid put operation can be selected as the binding operation.
func (suite *TransactionTestSuite) TestBindOperationFailed() {
	sid := 0
	badRow := suite.row(sid+1, 0)
	badRow.Delete("id")

	operations := []Operation{
		&Put{
			value:   badRow,
			errCode: nosqlerr.IllegalArgument,
		},
		&Put{
			value:         suite.row(sid, 0),
			shouldSucceed: true,
		},
		&Put{
			value:         suite.row(sid, 1),
			shouldSucceed: true,
		},
		&Put{
			value:         suite.row(sid, 2),
			shouldSucceed: true,
		},
	}

	for i := 0; i < 20; i++ {
		suite.executeOperations(operations, true, true)
	}
}

func (suite *TransactionTestSuite) stmtQueryBySid(sid int) string {
	return suite.stmtQueryBySidTable(suite.table, sid)
}

func (suite *TransactionTestSuite) stmtQueryBySidTable(table string, sid int) string {
	return fmt.Sprintf(suite.queryBySid, table, sid)
}

func (suite *TransactionTestSuite) loadRows(sid int, numIdsPerSid int) {
	op := new(Put)
	op.shouldSucceed = true
	for i := 0; i < numIdsPerSid; i++ {
		op.value = suite.row(sid, i)
		op.perform(suite, nil)
	}
}

func (suite *TransactionTestSuite) loadChildRows(sid int, numIdsPerSid int, numCidsPerId int) {
	op := new(Put)
	op.table = suite.childTable
	op.shouldSucceed = true
	for id := 0; id < numIdsPerSid; id++ {
		for cid := 0; cid < numCidsPerId; cid++ {
			op.value = suite.childRow(sid, id, 0)
			op.perform(suite, nil)
		}
	}
}

func (suite *TransactionTestSuite) deleteAll() {
	stmt := fmt.Sprintf("delete from %s", suite.childTable)
	suite.query(stmt, -1)

	stmt = fmt.Sprintf("delete from %s", suite.table)
	suite.query(stmt, -1)
}

func (suite *TransactionTestSuite) deleteTable(tableName string) {
	stmt := fmt.Sprintf("delete from %s", tableName)
	suite.query(stmt, -1)
}

func (suite *TransactionTestSuite) query(stmt string, numAffectedRows int) {
	op := &Query{
		statement: stmt,
		numRows:   numAffectedRows,
	}

	op.perform(suite, nil)
}

func (suite *TransactionTestSuite) executeAndCommit(operations []Operation) {
	suite.executeOperations(operations, false, true)
}

func (suite *TransactionTestSuite) executeAndAbort(operations []Operation) {
	suite.executeOperations(operations, false, false)
}

func (suite *TransactionTestSuite) executeOperations(operations []Operation, concurrent bool, commit bool) {
	txn := suite.beginTransaction()
	defer func() {
		if commit {
			suite.commitTransaction(txn)
		} else {
			suite.abortTransaction(txn)
		}
	}()

	if concurrent {
		suite.runConcurrentOps(operations, txn)
	} else {
		for _, op := range operations {
			op.perform(suite, txn)
		}
	}
}

func (suite *TransactionTestSuite) runConcurrentOps(operations []Operation, txn *nosqldb.Transaction) {
	var wg sync.WaitGroup
	for _, op := range operations {
		wg.Add(1)
		go func(op Operation) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					suite.T().Errorf(">>> error in op %T: %v", op, r)
				}
			}()
			op.perform(suite, txn)
		}(op)
	}
	wg.Wait()
}

func (suite *TransactionTestSuite) shardKey(sid int) *types.MapValue {
	key := &types.MapValue{}
	key.Put("sid", sid)
	return key
}

func (suite *TransactionTestSuite) key(sid int, id int) *types.MapValue {
	key := suite.shardKey(sid)
	key.Put("id", id)
	return key
}

func (suite *TransactionTestSuite) row(sid int, id int) *types.MapValue {
	value := suite.key(sid, id)
	value.Put("i0", id)
	value.Put("i1", id)
	value.Put("s", "s"+strconv.Itoa(id))
	return value
}

func (suite *TransactionTestSuite) childKey(sid int, id int, cid int) *types.MapValue {
	key := suite.key(sid, id)
	key.Put("cid", cid)
	return key
}

func (suite *TransactionTestSuite) childRow(sid int, id int, cid int) *types.MapValue {
	value := suite.childKey(sid, id, cid)
	value.Put("ci0", cid)
	value.Put("ci1", cid)
	return value
}

func (suite *TransactionTestSuite) beginTransaction() *nosqldb.Transaction {
	var req *nosqldb.BeginTransactionRequest
	var res *nosqldb.TransactionResult
	var err error

	req = &nosqldb.BeginTransactionRequest{
		TableName: suite.table,
	}
	res, err = suite.Client.BeginTransaction(req)
	suite.debugf("[BeginTransaction] res=%v\n", res)
	if suite.NoErrorf(err, "BeginTransaction() failed, got error: %v", err) {
		suite.assertTransactionOpCost(res.Capacity)
		return res.Transaction
	}
	return nil
}

func (suite *TransactionTestSuite) commitTransaction(txn *nosqldb.Transaction) {
	var req *nosqldb.CommitTransactionRequest
	var res *nosqldb.TransactionResult
	var err error

	req = &nosqldb.CommitTransactionRequest{
		EndTransactionRequest: nosqldb.EndTransactionRequest{
			Transaction: txn,
		},
	}
	res, err = suite.Client.CommitTransaction(req)
	suite.debugf("[CommitTransaction] res=%v\n", res)

	suite.NoErrorf(err, "CommitTransaction() failed, got error: %v", err)
	suite.assertTransactionOpCost(res.Capacity)
}

func (suite *TransactionTestSuite) abortTransaction(txn *nosqldb.Transaction) {
	var req *nosqldb.AbortTransactionRequest
	var res *nosqldb.TransactionResult
	var err error

	req = &nosqldb.AbortTransactionRequest{
		EndTransactionRequest: nosqldb.EndTransactionRequest{
			Transaction: txn,
		},
	}
	res, err = suite.Client.AbortTransaction(req)
	suite.debugf("[AbortTransaction] res=%v\n", res)

	suite.NoErrorf(err, "AbortTransaction() failed, got error: %v", err)
	suite.assertTransactionOpCost(res.Capacity)
}

func (suite *TransactionTestSuite) assertTransactionOpCost(capacity nosqldb.Capacity) {
	suite.Equalf(0, capacity.ReadKB, "assertTransactionOpCost: readKB exp-0, act-%d", capacity.ReadKB)
	suite.Equalf(1, capacity.WriteKB, "assertTransactionOpCost: writeKB exp-1, act-%d", capacity.WriteKB)
}

type Operation interface {
	perform(suite *TransactionTestSuite, txn *nosqldb.Transaction)
}

type Put struct {
	value         *types.MapValue
	table         string
	shouldSucceed bool
	errCode       nosqlerr.ErrorCode
	option        types.PutOption
	matchVersion  types.Version
}

func (op *Put) createRequest(suite *TransactionTestSuite, txn *nosqldb.Transaction) *nosqldb.PutRequest {
	req := &nosqldb.PutRequest{
		Value:        op.value,
		TableName:    op.table,
		Transaction:  txn,
		PutOption:    op.option,
		MatchVersion: op.matchVersion,
	}
	if req.TableName == "" {
		req.TableName = suite.table
	}
	return req
}

func (op *Put) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {
	req := op.createRequest(suite, txn)

	res, err := suite.Client.Put(req)
	checkError(suite, "Put", err, op.errCode)
	if err != nil {
		return
	}

	suite.debugf("\n[Put] res=%v, isBindOp=%t\n", res, nosqldb.IsTxnBindingOp(req))
	if op.shouldSucceed {
		suite.NotNilf(res.Version, "Put failed")
	} else {
		suite.Nilf(res.Version, "Put should not succeed")
	}
}

type Get struct {
	key       *types.MapValue
	table     string
	keyExists bool
	errCode   nosqlerr.ErrorCode
}

func (op *Get) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {
	req := &nosqldb.GetRequest{
		Key:         op.key,
		TableName:   op.table,
		Transaction: txn,
	}
	if req.TableName == "" {
		req.TableName = suite.table
	}

	res, err := suite.Client.Get(req)
	checkError(suite, "Get", err, op.errCode)
	if err != nil {
		return
	}

	suite.debugf("\n[Get] res=%v, isBindOp=%t\n", res, nosqldb.IsTxnBindingOp(req))
	if op.keyExists {
		suite.NotNilf(res.Value, "Key not found")
	} else {
		suite.Nilf(res.Value, "Key should not exist")
	}
}

type Delete struct {
	key          *types.MapValue
	table        string
	keyExists    bool
	errCode      nosqlerr.ErrorCode
	matchVersion types.Version
}

func (op *Delete) createRequest(suite *TransactionTestSuite, txn *nosqldb.Transaction) *nosqldb.DeleteRequest {
	req := &nosqldb.DeleteRequest{
		Key:          op.key,
		TableName:    op.table,
		Transaction:  txn,
		MatchVersion: op.matchVersion,
	}
	if req.TableName == "" {
		req.TableName = suite.table
	}
	return req
}

func (op *Delete) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {
	req := op.createRequest(suite, txn)

	res, err := suite.Client.Delete(req)
	checkError(suite, "Delete", err, op.errCode)
	if err != nil {
		return
	}

	suite.debugf("\n[Delete] res=%v, isBindOp=%t\n", res, nosqldb.IsTxnBindingOp(req))
	if op.keyExists {
		suite.Truef(res.Success, "Key not found")
	} else {
		suite.Falsef(res.Success, "Key should not exist")
	}
}

type MultiDelete struct {
	key        *types.MapValue
	table      string
	numDeleted int
	errCode    nosqlerr.ErrorCode
}

func (op *MultiDelete) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {
	req := &nosqldb.MultiDeleteRequest{
		Key:         op.key,
		TableName:   op.table,
		Transaction: txn,
	}

	if req.TableName == "" {
		req.TableName = suite.table
	}

	res, err := suite.Client.MultiDelete(req)
	checkError(suite, "MultiDelete", err, op.errCode)
	if err != nil {
		return
	}

	suite.debugf("\n[MultiDelete] res=%v, isBindOp=%t\n", res, nosqldb.IsTxnBindingOp(req))
	if op.numDeleted >= 0 {
		suite.Equalf(op.numDeleted, res.NumDeleted,
			"MultiDelete: unexpected number of rows deleted, exp-%d, act-%d", op.numDeleted, res.NumDeleted)
	}
}

type WriteMultiple struct {
	operations  []Operation
	table       string
	abortIfFail bool
	errCode     nosqlerr.ErrorCode
}

func (op *WriteMultiple) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {
	req := &nosqldb.WriteMultipleRequest{
		TableName:   op.table,
		Transaction: txn,
	}

	if req.TableName == "" {
		req.TableName = suite.table
	}

	failedIndex := -1
	var err error
	for i, subOp := range op.operations {
		switch wop := subOp.(type) {
		case *Put:
			putReq := wop.createRequest(suite, nil)
			err = req.AddPutRequest(putReq, op.abortIfFail)
			suite.NoErrorf(err, "WriteMultiple failed to add PutRequest, got error: %v", err)
			if op.abortIfFail && failedIndex < 0 && !wop.shouldSucceed {
				failedIndex = i
			}

		case *Delete:
			delReq := wop.createRequest(suite, nil)
			err = req.AddDeleteRequest(delReq, op.abortIfFail)
			suite.NoErrorf(err, "WriteMultiple failed to add DeleteRequest, got error: %v", err)
			if op.abortIfFail && failedIndex < 0 && !wop.keyExists {
				failedIndex = i
			}

		default:
			suite.T().Errorf("unsupported request type for WriteMultipleRequest, "+
				"expect *Put or *Delete, got %T", req)
		}
	}

	res, err := suite.Client.WriteMultiple(req)
	checkError(suite, "WriteMultiple", err, op.errCode)
	if err != nil {
		return
	}

	suite.debugf("[WriteMultiple] numRequests=%d, isBindOp=%t, res=%v\n",
		len(op.operations), nosqldb.IsTxnBindingOp(req), res)

	if op.abortIfFail && failedIndex >= 0 {
		suite.Equalf(failedIndex, res.FailedOperationIndex,
			"WriteMultiple, FailedOperationIndex expect=%d, actual=%d", failedIndex, res.FailedOperationIndex)
		return
	}

	var opRet *nosqldb.OperationResult
	for i, subOp := range op.operations {
		opRet = &res.ResultSet[i]
		switch wop := subOp.(type) {
		case *Put:
			op.checkPutResult(suite, opRet, wop)
		case *Delete:
			op.checkDeleteResult(suite, opRet, wop)
		default:
			suite.T().Errorf("unsupported request type for WriteMultipleRequest, "+
				"expect *Put or *Delete, got %T", req)
		}
	}
}

func (op *WriteMultiple) checkPutResult(suite *TransactionTestSuite, opRet *nosqldb.OperationResult, put *Put) {
	if put.shouldSucceed {
		suite.NotNilf(opRet.Version, "put should succeed but not, row=%v", put.value)
	} else {
		suite.Nilf(opRet.Version, "put should not succeed, row=%v", put.value)
	}
}

func (op *WriteMultiple) checkDeleteResult(suite *TransactionTestSuite, opRet *nosqldb.OperationResult, delete *Delete) {
	suite.Equalf(delete.keyExists, opRet.Success,
		"unexpected delete result, exp-deleted=%t, act-deleted=%t", delete.keyExists, opRet.Success)
}

type Prepare struct {
	statement string
	errCode   nosqlerr.ErrorCode
}

func (op *Prepare) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {
	req := &nosqldb.PrepareRequest{
		Statement:   op.statement,
		Transaction: txn,
	}

	res, err := suite.Client.Prepare(req)
	checkError(suite, "Prepare", err, op.errCode)
	if err != nil {
		return
	}

	suite.debugf("[Prepare] stmt=%s, isBindOp=%t, res=%v\n", req.Statement, nosqldb.IsTxnBindingOp(req), res)
	suite.NoErrorf(err, "Prepare failed unxpectedly: %v", err)
	suite.NotNilf(res.PreparedStatement, "Prepare PreparedStatement should not be null")
}

type Query struct {
	statement string
	table     string
	numRows   int
	bindVars  map[string]interface{}
	errCode   nosqlerr.ErrorCode
}

func (op *Query) perform(suite *TransactionTestSuite, txn *nosqldb.Transaction) {

	var req *nosqldb.QueryRequest
	var res []*types.MapValue
	var err error = nil
	if op.bindVars != nil {
		prep := &nosqldb.PrepareRequest{
			Statement:   op.statement,
			Transaction: txn,
		}
		prepRet, err := suite.Client.Prepare(prep)
		suite.debugf("[Query] Prepare stmt=%s, isBindOp=%t, results:\n%v\n",
			op.statement, nosqldb.IsTxnBindingOp(prep), prepRet)
		checkError(suite, "Query", err, op.errCode)
		if err != nil {
			return
		}

		pStmt := prepRet.PreparedStatement
		for k, v := range op.bindVars {
			err = pStmt.SetVariable(k, v)
			suite.NoErrorf(err, "[Query] PreparedStatement.SetVariable(k=%s) got error %v", k, err)
		}
		req = &nosqldb.QueryRequest{
			PreparedStatement: &pStmt,
			TableName:         op.table,
			Transaction:       txn,
		}
	} else {
		req = &nosqldb.QueryRequest{
			Statement:   op.statement,
			TableName:   op.table,
			Transaction: txn,
		}
	}

	res, err = suite.ExecuteQueryRequest(req)
	checkError(suite, "Query", err, op.errCode)
	if err != nil {
		return
	}

	selectQuery := strings.Contains(strings.ToLower(op.statement), "select ")
	suite.debugf("[Query] stmt=%s, isSelect=%t isBindOp=%t, results:\n%s\n",
		op.statement, selectQuery, nosqldb.IsTxnBindingOp(req), op.dispResults(res))
	if op.numRows >= 0 {
		if selectQuery {
			numRows := len(res)
			suite.Equalf(op.numRows, numRows,
				"Wrong number of query results, exp-%d, act-%d", op.numRows, numRows)
		} else {
			suite.Equalf(1, len(res), "Insert/Update/Delete query should return single record, but actual %d", len(res))

			var numAffectedRows int
			_, val, _ := (res[0]).GetByIndex(1)
			if v, ok := val.(int); ok {
				numAffectedRows = v
			} else if v, ok := val.(int64); ok {
				fmt.Println("int64:", v)
				numAffectedRows = int(v)
			} else {
				suite.Fail("Unexpected return value of insert/update/delete query: type=%T value=%v", v, v)
			}
			suite.Equalf(op.numRows, numAffectedRows, "Unexpected query result")
		}
	}
}

func checkError(suite *TransactionTestSuite, operation string, err error, expErrCode nosqlerr.ErrorCode) {
	if err != nil {
		suite.debugf("\n%s failed: %v\n", operation, err)
	}
	if expErrCode == nosqlerr.NoError {
		suite.NoErrorf(err, "%s failed, got error: %v", operation, err)
	} else {
		suite.Truef(nosqlerr.Is(err, expErrCode), "%s should have failed with error code: %d ", operation, expErrCode)
	}
}

func (op *Query) dispResults(results []*types.MapValue) string {
	if results == nil {
		return ""
	}
	var sb strings.Builder
	var b []byte

	for i, v := range results {
		b, _ = v.MarshalJSON()
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString("-")
		sb.WriteString(string(b))
		sb.WriteByte('\n')
	}
	sb.WriteString(strconv.Itoa(len(results)))
	sb.WriteString(" rows returned\n")
	return sb.String()
}

func (suite *TransactionTestSuite) debugf(format string, args ...any) {
	if suite.verbose {
		fmt.Printf(format, args...)
	}
}

func TestTransaction(t *testing.T) {
	test := &TransactionTestSuite{
		NoSQLTestSuite: test.NewNoSQLTestSuite(),
	}
	suite.Run(t, test)
}
