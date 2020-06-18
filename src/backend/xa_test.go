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
	"strings"
	"sync"
	"testing"
	"time"

	"xcontext"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestTxnXAAbort(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 3)
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

func TestTxnXAExecute(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
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
		err := txn.Commit()
		assert.Nil(t, err)
	}
}

func TestTxnXARollback(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "insert", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQuery(querys[1].Query, result2)
	fakedb.AddQuery(querys[2].Query, result2)

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

	// 2PC Rollback.
	{
		err = txn.Rollback()
		assert.Nil(t, err)
	}
}

func TestTxnXARollbackError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// commit err and rollback err will WriteXaCommitErrLog, need the scatter
	fakedb, txnMgr, backends, addrs, scatter, cleanup := MockTxnMgrScatter(log, 2)
	defer cleanup()
	err := scatter.Init(MockScatterDefault(log))
	assert.Nil(t, err)

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

	// Rollback error.
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
			err = txn.Rollback()
			assert.NotNil(t, err)
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
			err = txn.Rollback()
			assert.NotNil(t, err)
		}

		// ROLLBACK error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
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
			err = txn.Rollback()
			assert.Nil(t, err)
		}

		// ROLLBACK nothing for read-txn.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			err = txn.Begin()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnRead,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			err = txn.Rollback()
			assert.Nil(t, err)
		}
	}
}

func TestTxnXAExecuteNormalOnOneBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
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
		err := txn.Commit()
		assert.Nil(t, err)
	}
}

func TestTxnXAExecuteWrite(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
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
		err := txn.Commit()
		assert.Nil(t, err)
	}
}

func TestTxnXAExecuteScatterOnOneBackend(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 1)
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
		err := txn.Commit()
		assert.Nil(t, err)
	}
}

func TestTxnXAExecuteError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// commit err and rollback err will WriteXaCommitErrLog, need the scatter
	fakedb, txnMgr, backends, addrs, scatter, cleanup := MockTxnMgrScatter(log, 2)
	defer cleanup()
	err := scatter.Init(MockScatterDefault(log))
	assert.Nil(t, err)

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

	resetFunc1 := func(txn *Txn) {
		fakedb.ResetAll()
		fakedb.AddQueryError(querys[0].Query, errors.New("mock.xa.start.error"))
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
	}

	// Execute error, RollbackPhaseOne.
	{
		txn, err := txnMgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
		resetFunc1(txn)

		err = txn.Begin()
		assert.Nil(t, err)

		rctx := &xcontext.RequestContext{
			Mode:    xcontext.ReqNormal,
			TxnMode: xcontext.TxnWrite,
			Querys:  querys,
		}
		_, err = txn.Execute(rctx)
		assert.NotNil(t, err)
		err = txn.RollbackPhaseOne()
		assert.Nil(t, err)
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
			err = txn.Commit()
			assert.NotNil(t, err)
		}

		// XA END error when RollbackPhaseOne.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA START .*", errors.New("mock.xa.start.error"))
			fakedb.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))

			err = txn.Begin()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.NotNil(t, err)
			err = txn.RollbackPhaseOne()
			assert.NotNil(t, err)
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
			err = txn.Commit()
			assert.NotNil(t, err)
		}

		// ROLLBACK error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
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
			err = txn.Rollback()
			assert.Nil(t, err)
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
			err = txn.Commit()
			assert.Nil(t, err)
		}
	}
}

func TestTxnBeginScatter(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "insert", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQuery(querys[1].Query, result2)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Set 2PC conds.
	{
		fakedb.AddQueryPattern("XA .*", result1)
	}

	// Begin.
	{
		err := txn.BeginScatter()
		assert.Nil(t, err)
	}
}

func TestTxnCheckXidPrefix(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, _, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Set 2PC conds.
	{
		fakedb.AddQueryPattern("XA .*", result1)
	}

	// check the prefix of xid in the single statement txn is RXID.
	{
		err := txn.BeginScatter()
		assert.Nil(t, err)
		ss := strings.Split(txn.xid, "-")
		assert.EqualValues(t, "RXID", ss[0])
	}

	// check the prefix of xid in the multiple statement txn is MULTRXID.
	{
		txn.SetMultiStmtTxn()
		err := txn.BeginScatter()
		assert.Nil(t, err)
		ss := strings.Split(txn.xid, "-")
		assert.EqualValues(t, "MULTRXID", ss[0])
	}
}

func TestTxnBeginScatterError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "insert", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQuery(querys[1].Query, result2)

	txn, err := txnMgr.CreateTxn(backends)
	assert.Nil(t, err)
	defer txn.Finish()

	// Set 2PC conds.
	{
		fakedb.AddQueryErrorPattern("XA START .*", errors.New("mock.xa.start.error"))
	}

	// Begin.
	{
		err := txn.BeginScatter()
		assert.NotNil(t, err)
	}
}

func TestTxnCommitScatter(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "insert", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQuery(querys[1].Query, result2)

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
		assert.Equal(t, want, got)
	}

	// 2PC CommitScatter.
	{
		err = txn.CommitScatter()
		assert.Nil(t, err)
	}
}

func TestTxnCommitScatterError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// commit err and rollback err will WriteXaCommitErrLog, need the scatter
	fakedb, txnMgr, backends, addrs, scatter, cleanup := MockTxnMgrScatter(log, 2)
	defer cleanup()
	err := scatter.Init(MockScatterDefault(log))
	assert.Nil(t, err)

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

	// CommitScatter error.
	{
		// XA END error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))

			err = txn.BeginScatter()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			err = txn.CommitScatter()
			assert.NotNil(t, err)
		}

		// XA PREPARE error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA PREPARE .*", errors.New("mock.xa.prepare.error"))

			err = txn.BeginScatter()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			err = txn.CommitScatter()
			assert.NotNil(t, err)
		}

		// COMMIT error.
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
			err = txn.CommitScatter()
			assert.Nil(t, err)
		}
	}
}

func TestTxnRollbackScatter(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, backends, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "insert", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "insert", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result2)
	fakedb.AddQuery(querys[1].Query, result2)

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
		assert.Equal(t, want, got)
	}

	// 2PC Rollback.
	{
		err = txn.RollbackScatter()
		assert.Nil(t, err)
	}
}

func TestTxnRollbackScatterError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// commit err and rollback err will WriteXaCommitErrLog, need the scatter
	fakedb, txnMgr, backends, addrs, scatter, cleanup := MockTxnMgrScatter(log, 2)
	defer cleanup()
	err := scatter.Init(MockScatterDefault(log))
	assert.Nil(t, err)

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

	// RollbackScatter error.
	{
		// XA END error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
			fakedb.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))

			err = txn.BeginScatter()
			assert.Nil(t, err)

			rctx := &xcontext.RequestContext{
				Mode:    xcontext.ReqNormal,
				TxnMode: xcontext.TxnWrite,
				Querys:  querys,
			}
			_, err = txn.Execute(rctx)
			assert.Nil(t, err)
			err = txn.RollbackScatter()
			assert.NotNil(t, err)
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
			err = txn.RollbackScatter()
			assert.NotNil(t, err)
		}

		// ROLLBACK error.
		{
			txn, err := txnMgr.CreateTxn(backends)
			assert.Nil(t, err)
			defer txn.Finish()

			resetFunc(txn)
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
			err = txn.RollbackScatter()
			assert.Nil(t, err)
		}
	}
}
