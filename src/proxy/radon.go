/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleRadon used to handle the command: radon attach/detach/attachlist.
func (spanner *Spanner) handleRadon(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	log := spanner.log
	attach := NewAttach(log, spanner.scatter, spanner.router, spanner)

	snode := node.(*sqlparser.Radon)
	row := snode.Row
	var attachName string

	if row != nil {
		if len(row) != DetachParamsCount && len(row) != AttachParamsCount {
			return nil, errors.Errorf("spanner.query.execute.radon.%s.error,", snode.Action)
		}

		if len(row) == DetachParamsCount {
			val, _ := row[0].(*sqlparser.SQLVal)
			attachName = common.BytesToString(val.Val)
		}
	}

	switch snode.Action {
	case sqlparser.AttachStr:
		qr, err = attach.Attach(snode)
	case sqlparser.DetachStr:
		qr, err = attach.Detach(attachName)
	case sqlparser.AttachListStr:
		qr, err = attach.ListAttach()
	case sqlparser.ReshardStr:
		table := snode.Table.Name.String()
		database := session.Schema()
		if !snode.Table.Qualifier.IsEmpty() {
			database = snode.Table.Qualifier.String()
		}

		newTable := snode.NewName.Name.String()
		newDatabase := session.Schema()
		if !snode.NewName.Qualifier.IsEmpty() {
			newDatabase = snode.NewName.Qualifier.String()
		}

		log.Error("proxy.radon.unsupported.%s: [%s.%s->%s.%s]", snode.Action, database, table, newDatabase, newTable)
		err = sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query.%s: [%s.%s->%s.%s]", snode.Action,
			database, table, newDatabase, newTable)
	default:
		log.Error("proxy.radon.unsupported[%s]", query)
		err = sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "unsupported.query: %v", query)
	}
	if err != nil {
		log.Error("proxy.query.radon.[%s].error:%s", query, err)
		return nil, err
	}
	return qr, err
}
