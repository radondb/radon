/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func (spanner *Spanner) handleMultiStateTransaction(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var err error
	log := spanner.log
	snode := node.(*sqlparser.Transaction)
	switch snode.Action {
	case "start transaction":
		_, err = spanner.handleStart(session, query, node)
	case "begin":
		_, err = spanner.handleBegin(session, query, node)
	case "rollback":
		_, err = spanner.handleRollback(session, query, node)
	case "commit":
		_, err = spanner.handleCommit(session, query, node)
	}

	if err != nil {
		log.Error("proxy.query.transaction[%s].error:%s", query, err)
	}
	return nil, err
}

// TODO: handleStart used to handle Multi-statement transaction "start transaction"
func (spanner *Spanner) handleStart(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
	return nil, nil
}

// TODO: handleBegin used to handle Multi-statement transaction "begin"
func (spanner *Spanner) handleBegin(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
	return nil, nil
}

// TODO: handleRollback used to handle Multi-statement transaction "rollback"
func (spanner *Spanner) handleRollback(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
	return nil, nil
}

// TODO: handleCommit used to handle Multi-statement transaction "commit"
func (spanner *Spanner) handleCommit(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
	return nil, nil
}
