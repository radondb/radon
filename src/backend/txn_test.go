/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"xcontext"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"

	"github.com/xelabs/go-mysqlstack/sqldb"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestTxnNormalExecute(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "select * from node3", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result1)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)
	fakedb.AddQueryDelay(querys[2].Query, result2, 110)

	// normal execute.
	{
		rctx := &xcontext.RequestContext{
			Querys: querys,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result1)
		want.AppendResult(result2)
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}

	// single execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqSingle,
			RawQuery: querys[0].Query,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		assert.Equal(t, result1, got)
	}

	// scatter execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqScatter,
			RawQuery: querys[0].Query,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result1)
		want.AppendResult(result1)
		assert.Equal(t, want, got)
	}
}

func TestTxnExecuteStreamFetch(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "select * from node3", Backend: addrs[1]},
	}

	result11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}
	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		}
		result11.Rows = append(result11.Rows, row)
	}

	result12 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 9; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("22")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("2nice name")),
		}
		result12.Rows = append(result12.Rows, row)
	}

	// normal execute.
	{
		fakedb.AddQueryStream(querys[0].Query, result11)
		fakedb.AddQueryStream(querys[1].Query, result12)
		fakedb.AddQueryStream(querys[2].Query, result12)

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		rctx := &xcontext.RequestContext{
			Querys: querys,
		}

		callbackQr := &sqltypes.Result{}
		err = txn.ExecuteStreamFetch(rctx, func(qr *sqltypes.Result) error {
			callbackQr.AppendResult(qr)
			return nil
		}, 1024*1024)
		assert.Nil(t, err)

		want := len(result11.Rows) + 2*len(result12.Rows)
		got := len(callbackQr.Rows)
		assert.Equal(t, want, got)
	}

	// execute error.
	{
		fakedb.AddQueryError(querys[0].Query, errors.New("mock.stream.query.error"))
		fakedb.AddQueryStream(querys[1].Query, result12)
		fakedb.AddQueryStream(querys[2].Query, result12)

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		rctx := &xcontext.RequestContext{
			Querys: querys,
		}

		callbackQr := &sqltypes.Result{}
		err = txn.ExecuteStreamFetch(rctx, func(qr *sqltypes.Result) error {
			callbackQr.AppendResult(qr)
			return nil
		}, 1024*1024)
		want := "mock.stream.query.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestTxnNormalError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 3)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[1]},
	}

	// execute error.
	{
		fakedb.AddQueryError("select * from node1", errors.New("mock.execute.error"))
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqSingle,
			RawQuery: querys[0].Query,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		_, err = txn.Execute(rctx)
		assert.NotNil(t, err)
	}
}

func TestTxnErrorBackendNotExists(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, _, cleanup := MockTxnMgr(log, 3)
	defer cleanup()

	fakedb.AddQuery("select * from node1", result1)
	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: "xx"},
		xcontext.QueryTuple{Query: "select * from node2", Backend: "xx"},
	}

	// Normal connection error.
	{
		rctx := &xcontext.RequestContext{
			Querys: querys,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		_, err = txn.Execute(rctx)
		want := "txn.can.not.get.normal.connection.by.backend[xx].from.pool"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestTxnExecuteSingle(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
	}
	fakedb.AddQuery(querys[0].Query, result1)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// single execute.
	{
		got, err := txn.ExecuteSingle(querys[0].Query)
		assert.Nil(t, err)
		assert.Equal(t, result1, got)
	}
}

func TestTxnExecuteScatter(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
	}
	fakedb.AddQuery(querys[0].Query, result1)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// scatter execute.
	{
		qr, err := txn.ExecuteScatter(querys[0].Query)
		assert.Nil(t, err)
		got := fmt.Sprintf("%+v", qr.Rows)
		want := "[[11 1nice name] [12 12nice name] [11 1nice name] [12 12nice name]]"
		assert.Equal(t, want, got)
	}
}

func TestTxnExecuteOnThisBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()
	query := "select from node2"
	backend := addrs[1]
	fakedb.AddQuery(query, result1)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	{
		got, err := txn.ExecuteOnThisBackend(backend, query)
		assert.Nil(t, err)
		assert.Equal(t, result1, got)
	}
}

func TestTxnSetting(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, _, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	query := "select * from node1"
	fakedb.AddQueryDelay(query, result1, 1000)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// timeout
	{
		txn.SetTimeout(50)
		// scatter execute.
		{
			_, err := txn.ExecuteScatter(query)
			assert.NotNil(t, err)
			/*
				got := err.Error()
				want := "Query execution was interrupted, timeout[50ms] exceeded"
				assert.Equal(t, want, got)
			*/
		}
	}

	// max result size.
	{
		txn.SetTimeout(0)
		txn.SetMaxResult(10)
		// scatter execute.
		{
			_, err := txn.ExecuteScatter(query)
			got := err.Error()
			want := "Query execution was interrupted, max memory usage[10 bytes] exceeded"
			assert.Equal(t, want, got)
		}
	}
}

/*****************************************************************/
/************************XA TESTS START***************************/
/*****************************************************************/
func TestTxnAbort(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 3)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "update node2", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "update node3", Backend: addrs[2]},
	}

	fakedb.AddQueryDelay(querys[0].Query, result2, 2000)
	fakedb.AddQueryDelay(querys[1].Query, result2, 2000)
	fakedb.AddQueryDelay(querys[2].Query, result2, 2000)
	fakedb.AddQueryPattern("XA .*", result1)

	// Normal abort.
	{
		var wg sync.WaitGroup
		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		// normal execute with long time.
		{
			wg.Add(1)
			go func() {
				defer wg.Done()
				rctx := &xcontext.RequestContext{
					TxnMode: xcontext.TxnWrite,
					Querys:  querys,
				}
				txn.Execute(rctx)
			}()
		}

		// abort
		{
			time.Sleep(time.Second)
			err := txn.Abort()
			assert.Nil(t, err)
		}
		wg.Wait()
	}

	// Twopc abort.
	{
		var wg sync.WaitGroup
		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		err = txn.Begin()
		assert.Nil(t, err)

		// normal execute with long time.
		{
			wg.Add(1)
			go func() {
				defer wg.Done()
				rctx := &xcontext.RequestContext{
					TxnMode: xcontext.TxnWrite,
					Querys:  querys,
				}
				txn.Execute(rctx)
			}()
		}

		// abort
		{
			time.Sleep(time.Second)
			err := txn.Abort()
			assert.Nil(t, err)
		}
		wg.Wait()
	}
}

func TestTxnTwoPCExecute(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "select * from node3", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)
	fakedb.AddQueryDelay(querys[2].Query, result2, 150)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Set 2PC conds.
	{
		fakedb.AddQueryPattern("XA .*", result1)
	}

	// Begin.
	{
		err := txn.Begin()
		assert.Nil(t, err)
	}

	// normal execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnRead,
			Querys:  querys,
		}
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result2)
		want.AppendResult(result2)
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}

	// single execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqSingle,
			RawQuery: querys[0].Query,
		}
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		assert.Equal(t, result2, got)
	}

	// scatter execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqScatter,
			TxnMode:  xcontext.TxnWrite,
			RawQuery: querys[0].Query,
		}
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result2)
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}

	// 2PC Commit.
	{
		txn.Commit()
	}
}

func TestTxnTwoPCExecuteNormalOnOneBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	// All in one backends.
	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node3", Backend: addrs[0]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)
	fakedb.AddQueryDelay(querys[2].Query, result2, 150)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Begin.
	{
		err := txn.Begin()
		assert.Nil(t, err)
	}

	// normal execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:   xcontext.ReqNormal,
			Querys: querys,
		}
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result2)
		want.AppendResult(result2)
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}

	// 2PC Commit.
	{
		txn.Commit()
	}
}

func TestTxnTwoPCExecuteWrite(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "insert", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)
	fakedb.AddQueryDelay(querys[2].Query, result2, 150)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Set 2PC conds.
	{
		fakedb.AddQueryPattern("XA .*", result1)
	}

	// Begin.
	{
		err := txn.Begin()
		assert.Nil(t, err)
	}

	// normal execute.
	{
		rctx := &xcontext.RequestContext{
			TxnMode: xcontext.TxnWrite,
			Querys:  querys,
		}
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result2)
		want.AppendResult(result2)
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}

	// 2PC Commit.
	{
		txn.Commit()
	}
}

func TestTxnTwoPCExecuteScatterOnOneBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 1)
	defer cleanup()

	// All in one backends.
	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
	}
	fakedb.AddQuery(querys[0].Query, result2)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Begin.
	{
		err := txn.Begin()
		assert.Nil(t, err)
	}

	// scatter execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqScatter,
			RawQuery: querys[0].Query,
		}
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := result2
		assert.Equal(t, want, got)
	}

	// 2PC Commit.
	{
		txn.Commit()
	}
}

func TestTxnTwoPCExecuteError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "update", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "update", Backend: addrs[1]},
	}
	fakedb.AddQuery(querys[0].Query, result1)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)

	// Set 2PC conds.
	resetFunc := func(txn *Txn) {
		fakedb.ResetAll()
		fakedb.AddQuery(querys[0].Query, result1)
		fakedb.AddQueryDelay(querys[1].Query, result2, 100)
		fakedb.AddQueryPattern("XA .*", result1)
	}

	// Begin never failed.
	{
		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		resetFunc(txn)

		err = txn.Begin()
		assert.Nil(t, err)
	}

	// Execute, xa start error.
	{
		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		resetFunc(txn)
		fakedb.AddQueryErrorPattern("XA START .*", errors.New("mock.xa.start.error"))

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys,
		}
		_, err = txn.Execute(rctx)
		assert.NotNil(t, err)
		txn.Rollback()
	}

	// Commit error.
	{
		// XA END error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))

			err = txn.Begin()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			txn.Commit()
		}

		// XA PREPARE error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA PREPARE .*", errors.New("mock.xa.prepare.error"))

			err = txn.Begin()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			txn.Commit()
		}

		// XA PREPARE and ROLLBACK error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA PREPARE .*", errors.New("mock.xa.prepare.error"))
			fakedb.AddQueryErrorPattern("XA ROLLBACK .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))

			err = txn.Begin()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			txn.Commit()
		}

		// XA COMMIT error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA COMMIT .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))

			err = txn.Begin()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			txn.Commit()
		}
	}
}

/*****************************************************************/
/*************************XA TESTS END****************************/
/*****************************************************************/
