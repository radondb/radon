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
	"sync"
	"testing"
	"time"
	"xcontext"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestBackupTxnExecute(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, _, backup, addrs, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	querys := []xcontext.QueryTuple{
		xcontext.QueryTuple{Query: "select * from node1", Backend: addrs[0]},
		xcontext.QueryTuple{Query: "select * from node2", Backend: addrs[1]},
		xcontext.QueryTuple{Query: "select * from node3", Backend: addrs[1]},
	}

	fakedb.AddQuery(querys[0].Query, result1)
	fakedb.AddQueryDelay(querys[1].Query, result2, 100)
	fakedb.AddQueryDelay(querys[2].Query, result2, 110)
	fakedb.AddQuery("select * from backup", result1)

	// backup execute.
	{
		txn, err := txnMgr.CreateBackupTxn(backup)
		assert.Nil(t, err)

		got, err := txn.Execute("", "select * from backup")
		assert.Nil(t, err)
		assert.Equal(t, result1, got)
		txn.Finish()
	}

	// backup execute error.
	{
		fakedb.ResetAll()
		fakedb.AddQueryError("select * from backup", errors.New("mock.backup.select.error"))

		txn, err := txnMgr.CreateBackupTxn(backup)
		assert.Nil(t, err)
		_, err = txn.Execute("", "select * from backup")
		assert.NotNil(t, err)
		txn.Finish()
	}
}

func TestBackupTxnSetting(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, _, backup, _, cleanup := MockTxnMgr(log, 2)
	defer cleanup()

	query := "select * from node1"
	fakedb.AddQueryDelay(query, result1, 100)

	txn, err := txnMgr.CreateBackupTxn(backup)
	assert.Nil(t, err)
	defer txn.Finish()

	// timeout
	{
		txn.SetTimeout(10)
		_, err := txn.Execute("", query)
		assert.NotNil(t, err)
	}

	// max result size.
	{
		txn.SetTimeout(0)
		txn.SetMaxResult(10)
		_, err := txn.Execute("", query)
		got := err.Error()
		want := "Query execution was interrupted, max memory usage[10 bytes] exceeded"
		assert.Equal(t, want, got)
	}
}

func TestBackupTxnAbort(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb, txnMgr, _, backup, _, cleanup := MockTxnMgr(log, 3)
	defer cleanup()

	fakedb.AddQueryDelay("update backup", result2, 2000)

	{
		var wg sync.WaitGroup
		txn, err := txnMgr.CreateBackupTxn(backup)
		assert.Nil(t, err)
		defer txn.Finish()

		// execute with long time.
		{
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn.Execute("", "update backup")
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
