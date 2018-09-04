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
	"sync/atomic"

	"config"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// TxnManager tuple.
type TxnManager struct {
	log        *xlog.Log
	xaCheck    *XaCheck
	txnid      uint64
	txnNums    int64
	commitLock sync.RWMutex
}

// NewTxnManager creates new TxnManager.
func NewTxnManager(log *xlog.Log) *TxnManager {
	return &TxnManager{
		log:   log,
		txnid: 0,
	}
}

// Init is used to init the async worker.
func (mgr *TxnManager) Init(scatter *Scatter, XaCheckConf *config.XaCheckConfig) error {
	xaChecker := NewXaCheck(scatter, XaCheckConf)
	if err := xaChecker.Init(); err != nil {
		return err
	}
	mgr.xaCheck = xaChecker

	return nil
}

// GetID returns a new txnid.
func (mgr *TxnManager) GetID() uint64 {
	return atomic.AddUint64(&mgr.txnid, 1)
}

// Add used to add a txn to mgr.
func (mgr *TxnManager) Add() error {
	atomic.AddInt64(&mgr.txnNums, 1)
	return nil
}

// Remove used to remove a txn from mgr.
func (mgr *TxnManager) Remove() error {
	atomic.AddInt64(&mgr.txnNums, -1)
	return nil
}

// CreateTxn creates new txn.
func (mgr *TxnManager) CreateTxn(backends map[string]*Pool) (*Txn, error) {
	if len(backends) == 0 {
		return nil, errors.New("backends.is.NULL")
	}

	txid := mgr.GetID()
	txn, err := NewTxn(mgr.log, txid, mgr, backends)
	if err != nil {
		return nil, err
	}
	mgr.Add()
	return txn, nil
}

// CreateBackupTxn creates new backup txn.
func (mgr *TxnManager) CreateBackupTxn(backup *Pool) (*BackupTxn, error) {
	if backup == nil {
		return nil, errors.New("backup.is.NULL")
	}
	txid := mgr.GetID()
	txn, err := NewBackupTxn(mgr.log, txid, mgr, backup)
	if err != nil {
		return nil, err
	}
	mgr.Add()
	return txn, nil
}

// CommitLock used to acquire the commit.
func (mgr *TxnManager) CommitLock() {
	mgr.commitLock.Lock()
}

// CommitUnlock used to release the commit.
func (mgr *TxnManager) CommitUnlock() {
	mgr.commitLock.Unlock()
}

// CommitRLock used to acquire the read lock of commit.
func (mgr *TxnManager) CommitRLock() {
	mgr.commitLock.RLock()
}

// CommitRUnlock used to release the read lock of commit.
func (mgr *TxnManager) CommitRUnlock() {
	mgr.commitLock.RUnlock()
}
