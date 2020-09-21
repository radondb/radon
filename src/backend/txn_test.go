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
	"testing"

	"xcontext"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestTxnNormalExecute(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
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

	// loadbalance=1.
	{
		rctx := &xcontext.RequestContext{
			Querys:  querys,
			TxnMode: xcontext.TxnRead,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		txn.SetIsExecOnRep(true)
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result1)
		want.AppendResult(result2)
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}
}

func TestTxnNormalExecuteWithAttach(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgrWithAttach(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "select * from node3", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result1)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)
	fakedb.AddQueryDelay(querys[2].Query, result2, 110)

	// single execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqSingle,
			RawQuery: querys[1].Query,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		assert.Equal(t, result2, got)
	}

	// scatter execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqScatter,
			RawQuery: querys[1].Query,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result2)
		assert.Equal(t, want, got)
	}
}

func TestTxnNormalExecuteWithReplica(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgrWithReplica(log, 2)
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
			Querys:  querys,
			TxnMode: xcontext.TxnRead,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		txn.SetIsExecOnRep(true)
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
			TxnMode:  xcontext.TxnRead,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		txn.SetIsExecOnRep(true)
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		assert.Equal(t, result1, got)
	}

	// scatter execute.
	{
		rctx := &xcontext.RequestContext{
			Mode:     xcontext.ReqScatter,
			RawQuery: querys[0].Query,
			TxnMode:  xcontext.TxnRead,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		txn.SetIsExecOnRep(true)
		got, err := txn.Execute(rctx)
		assert.Nil(t, err)

		want := &sqltypes.Result{}
		want.AppendResult(result1)
		want.AppendResult(result1)
		assert.Equal(t, want, got)
	}
}

func TestTxnExecuteReplicaError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, cleanup := MockTxnMgrWithReplica(log, 2)
	defer cleanup()

	fakedb.AddQuery("select * from node1", result1)
	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: "xx"},
		xcontext.QueryTuple{Query: "select * from node2", Backend: "xx"},
	}

	// Fetch replica connection fail, retry fetch normal connection error.
	{
		rctx := &xcontext.RequestContext{
			Querys:  querys,
			TxnMode: xcontext.TxnRead,
		}

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()

		txn.SetIsExecOnRep(true)
		_, err = txn.Execute(rctx)
		want := "txn.can.not.get.normal.connection.by.backend[xx].from.pool"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestTxnExecuteStreamFetch(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgrWithReplica(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		{Query: "select * from node1", Backend: addrs[0]},
		{Query: "select * from node2", Backend: addrs[1]},
		{Query: "select * from node3", Backend: addrs[1]},
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

	// loadbalance=1.
	{
		fakedb.AddQueryStream(querys[0].Query, result11)
		fakedb.AddQueryStream(querys[1].Query, result12)
		fakedb.AddQueryStream(querys[2].Query, result12)

		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		txn.SetIsExecOnRep(true)

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
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 3)
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
	fakedb, txnMgr, backends, _, cleanup := MockTxnMgr(log, 3)
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
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
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
	fakedb, txnMgr, backends, _, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := "select * from node1"
	fakedb.AddQuery(querys, result1)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// scatter execute.
	{
		qr, err := txn.ExecuteScatter(querys)
		assert.Nil(t, err)
		got := fmt.Sprintf("%+v", qr.Rows)
		want := "[[11 1nice name] [12 12nice name] [11 1nice name] [12 12nice name]]"
		assert.Equal(t, want, got)
	}
}

func TestTxnExecuteOnThisBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
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
	fakedb, txnMgr, backends, _, cleanup := MockTxnMgr(log, 2)
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

	{
		txn.SetSessionID(1)
	}
}

func TestTxnExecuteTwopc(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	query := "select * from node1"
	fakedb.AddQuery(query, result1)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	txn.twopc = true
	// scatter execute.
	{
		qr, err := txn.ExecuteScatter(query)
		assert.Nil(t, err)
		got := fmt.Sprintf("%+v", qr.Rows)
		want := "[[11 1nice name] [12 12nice name] [11 1nice name] [12 12nice name]]"
		assert.Equal(t, want, got)
	}
}
