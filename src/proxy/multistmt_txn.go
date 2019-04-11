/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func (spanner *Spanner) handleMultiStmtTxn(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	log := spanner.log
	snode := node.(*sqlparser.Transaction)
	switch snode.Action {
	case sqlparser.StartTxnStr:
		qr, err = spanner.handleStartTransaction(session, snode.Action, node)
	case sqlparser.BeginTxnStr:
		qr, err = spanner.handleBegin(session, snode.Action, node)
	case sqlparser.RollbackTxnStr:
		qr, err = spanner.handleRollback(session, snode.Action, node)
	case sqlparser.CommitTxnStr:
		qr, err = spanner.handleCommit(session, snode.Action, node)
	}
	if err != nil {
		log.Error("proxy.query.multistmt.txn.[%s].error:%s", query, err)
	}
	return qr, err
}

// handleStartTransaction used to handle Multi-statement transaction "start transaction"
func (spanner *Spanner) handleStartTransaction(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteBegin(session, query, node)
}

// handleBegin used to handle Multi-statement transaction "begin"
func (spanner *Spanner) handleBegin(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteBegin(session, query, node)
}

// handleRollback used to handle Multi-statement transaction "rollback"
func (spanner *Spanner) handleRollback(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteRollback(session, query, node)
}

// handleCommit used to handle Multi-statement transaction "commit"
func (spanner *Spanner) handleCommit(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteCommit(session, query, node)
}

// ExecuteBegin used to execute "start transaction" or "begin".
func (spanner *Spanner) ExecuteBegin(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	conf := spanner.conf
	sessions := spanner.sessions
	scatter := spanner.scatter
	var err error

	if !spanner.isTwoPC() {
		log.Error("spanner.execute.2pc.disable")
		return nil, errors.Errorf("spanner.query.execute.multistmt.txn.error[twopc-disable]")
	}

	txn := sessions.getSessionTxn(session)
	//1. If the previous cmd is not in transaction, and the autocommit = 0, the begin will implicit commit in mysql,
	// the case is not supported. https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	//2. If txn is not nil, it isn't supported. e.g., begin;sql1;sql2;.. begin;(return err, and the txn isn't free);
	if txn != nil {
		// the last txn isn't free
		log.Error("spanner.execute.multistmt.begin.nestedTxn.unsupported.")
		return nil, errors.Errorf("ExecuteMultiStatBegin.nestedTxn.unsupported")
	}

	txn, err = scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return nil, err
	}
	txn.SetTimeout(conf.Proxy.QueryTimeout)
	txn.SetMaxResult(conf.Proxy.MaxResultSize)
	txn.SetMultiStmtTxn()

	sessions.MultiStmtTxnBinding(session, txn, node, query)
	if err := txn.BeginScatter(); err != nil {
		txn.Finish()
		sessions.MultiStmtTxnUnBinding(session, true)
		log.Error("spanner.execute.multistmt.txn.begin.scatter.error:[%v]", err)
		return nil, err
	}

	qr := &sqltypes.Result{}
	return qr, nil
}

// ExecuteRollback used to execute multiple-statement transaction sql:"rollback"
func (spanner *Spanner) ExecuteRollback(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	sessions := spanner.sessions

	if !spanner.isTwoPC() {
		log.Error("spanner.execute.multistmt.txn.rollback.2pc.disable")
		qr := &sqltypes.Result{Warnings: 1}
		return qr, errors.Errorf("spanner.execute.multistmt.txn.rollback.error[twopc-disable]")
	}

	txn := sessions.getSessionTxn(session)
	// return err if query is "rollback" without begin a multi-transaction.
	if txn == nil {
		log.Error("spanner.execute.multistmt.txn.rollback.error.txn.not.begin")
		qr := &sqltypes.Result{}
		return qr, errors.Errorf("unsupported: rollback.without.txn.begin")
	}

	sessions.MultiStmtTxnBinding(session, nil, node, query)
	if err := txn.RollbackScatter(); err != nil {
		log.Error("spanner.execute.multistmt.txn.rollback.scattr.error:[%v]", err)
		return nil, err
	}

	sessions.MultiStmtTxnUnBinding(session, true)
	txn.Finish()
	qr := &sqltypes.Result{}
	return qr, nil
}

// ExecuteCommit used to execute multiple-statement transaction: "commit"
func (spanner *Spanner) ExecuteCommit(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	sessions := spanner.sessions

	if !spanner.isTwoPC() {
		log.Error("spanner.execute.multistmt.txn.commit.error.2pc.disable")
		qr := &sqltypes.Result{Warnings: 1}
		return qr, errors.Errorf("spanner.execute.multistmt.txn.commit.error:[twopc-disable]")
	}

	txn := sessions.getSessionTxn(session)
	// return err if "commit" was sent without begin a multi-transaction.
	if txn == nil {
		log.Error("spanner.execute.multistmt.txn.commit.error.txn.not.begin")
		qr := &sqltypes.Result{}
		return qr, errors.Errorf("unsupported: commit.without.txn.begin")
	}

	sessions.MultiStmtTxnBinding(session, nil, node, query)
	if err := txn.CommitScatter(); err != nil {
		log.Error("spanner.execute.multistmt.txn.commit.scattr.error:[%v]", err)
		return nil, err
	}

	sessions.MultiStmtTxnUnBinding(session, true)
	txn.Finish()
	qr := &sqltypes.Result{}
	return qr, nil
}
