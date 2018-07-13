/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"sync"
	"time"

	"xbase/sync2"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	backupTxnCounterCreate          = "#backuptxn.create"
	backupTxnCounterConnectionError = "#get.backup.connection.error"
	backupTxnCounterTxnFinish       = "#backuptxn.finish"
	backupTxnCounterTxnAbort        = "#backuptxn.abort"
)

// BackupTxn tuple.
type BackupTxn struct {
	log        *xlog.Log
	id         uint64
	mu         sync.Mutex
	mgr        *TxnManager
	txnd       *TxnDetail
	start      time.Time
	state      sync2.AtomicInt32
	backup     *Pool
	timeout    int
	maxResult  int
	errors     int
	connMu     sync.RWMutex
	connection Connection
}

// NewBackupTxn creates the new BackupTxn.
func NewBackupTxn(log *xlog.Log, txid uint64, mgr *TxnManager, backup *Pool) (*BackupTxn, error) {
	txn := &BackupTxn{
		log:    log,
		id:     txid,
		mgr:    mgr,
		backup: backup,
		start:  time.Now(),
	}
	txnd := NewTxnDetail(txn)
	txn.txnd = txnd
	tz.Add(txnd)
	txnCounters.Add(backupTxnCounterCreate, 1)
	return txn, nil
}

// SetTimeout used to set the txn timeout.
func (txn *BackupTxn) SetTimeout(timeout int) {
	txn.timeout = timeout
}

// SetMaxResult used to set the txn max result.
func (txn *BackupTxn) SetMaxResult(max int) {
	txn.maxResult = max
}

// TxID returns txn id.
func (txn *BackupTxn) TxID() uint64 {
	return txn.id
}

// XID returns empty.
func (txn *BackupTxn) XID() string {
	return ""
}

// State returns txn.state.
func (txn *BackupTxn) State() int32 {
	return txn.state.Get()
}

// XaState returns txn xastate.
func (txn *BackupTxn) XaState() int32 {
	return -1
}

func (txn *BackupTxn) incErrors() {
	txn.errors++
}

func (txn *BackupTxn) fetchBackupConnection() (Connection, error) {
	pool := txn.backup
	if pool == nil {
		return nil, errors.New("txn.backup.node.is.nil")
	}

	conn, err := pool.Get()
	if err != nil {
		return nil, err
	}

	txn.connMu.Lock()
	txn.connection = conn
	txn.connMu.Unlock()
	return conn, nil
}

// Execute used to execute the query to the backup node.
// If the backup node is not exists, fetchBackupConnection will return with an error.
func (txn *BackupTxn) Execute(database string, query string) (*sqltypes.Result, error) {
	log := txn.log
	conn, err := txn.fetchBackupConnection()
	if err != nil {
		log.Error("backtxn.execute.fetch.connection[db:%s, query:%s].error:%+v", database, query, err)
		txnCounters.Add(backupTxnCounterConnectionError, 1)
		txn.incErrors()
		return nil, err
	}
	if err := conn.UseDB(database); err != nil {
		log.Error("backuptxn.execute.usedb[db:%s, query:%s].on[%v].error:%+v", database, query, conn.Address(), err)
		txn.incErrors()
		return nil, err
	}

	qr, err := conn.ExecuteWithLimits(query, txn.timeout, txn.maxResult)
	if err != nil {
		log.Error("backuptxn.execute.db:%s, query:%s].on[%v].error:%+v", database, query, conn.Address(), err)
		txn.incErrors()
		return nil, err
	}
	return qr, nil
}

// Finish used to finish a transaction.
// If the lastErr is nil, we will recycle all the twopc connections to the pool for reuse,
// otherwise we wil close all of the them.
func (txn *BackupTxn) Finish() error {
	txnCounters.Add(backupTxnCounterTxnFinish, 1)

	txn.mu.Lock()
	defer txn.mu.Unlock()

	defer tz.Remove(txn.txnd)

	txn.state.Set(int32(txnStateFinshing))
	// backup node connection.
	if txn.connection != nil {
		if txn.errors > 0 {
			txn.connection.Close()
		} else {
			txn.connection.Recycle()
		}
	}
	txn.mgr.Remove()
	return nil
}

// Abort used to abort all txn connections.
func (txn *BackupTxn) Abort() error {
	txnCounters.Add(backupTxnCounterTxnAbort, 1)

	txn.mu.Lock()
	defer txn.mu.Unlock()

	// If the txn has finished, we won't do abort.
	if txn.state.Get() == int32(txnStateFinshing) {
		return nil
	}
	txn.state.Set(int32(txnStateAborting))

	// backup node connection.
	txn.connMu.RLock()
	if txn.connection != nil {
		txn.connection.Kill("txn.abort")
	}
	txn.connMu.RUnlock()
	txn.mgr.Remove()
	return nil
}
