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
	backends := make(map[string]*Poolz)
	addrs := fakedb.Addrs()
	for _, addr := range addrs {
		conf := MockBackendConfigDefault(addr, addr)
		poolz := NewPoolz(log, conf)
		backends[addr] = poolz
	}
	txnmgr := NewTxnManager(log)

	{
		txn, err := txnmgr.CreateTxn(backends)
		assert.Nil(t, err)
		defer txn.Finish()
	}
}
