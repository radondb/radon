/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"testing"

	"fakedb"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestTxnManager(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	backends := make(map[string]*Pool)
	addrs := fakedb.Addrs()
	for _, addr := range addrs {
		conf := MockBackendConfigDefault(addr, addr)
		pool := NewPool(log, conf)
		backends[addr] = pool
	}
	txnmgr := NewTxnManager(log)

	{
		txn, err := txnmgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
	}

	{
		backTxn, err := txnmgr.CreateBackupTxn(backends[addrs[0]])
		assert.Nil(t, err)
		defer backTxn.Finish()
	}
}

func TestTxnManagerBackendsNull(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	backends := make(map[string]*Pool)
	txnmgr := NewTxnManager(log)

	{
		_, err := txnmgr.CreateTxn(backends)
		want := "backends.is.NULL"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		_, err := txnmgr.CreateBackupTxn(nil)
		want := "backup.is.NULL"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}
