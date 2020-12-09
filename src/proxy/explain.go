/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"optimizer"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleExplain used to handle the EXPLAIN command.
func (spanner *Spanner) handleExplain(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	database := session.Schema()
	router := spanner.router
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "EXPLAIN", Type: querypb.Type_VARCHAR},
	}

	explainableStmt := node.(*sqlparser.Explain).Statement
	privilegePlug := spanner.plugins.PlugPrivilege()
	if err := privilegePlug.Check(database, session.User(), explainableStmt); err != nil {
		return nil, err
	}

	// Explain only supports DML.
	// see: https://dev.mysql.com/doc/refman/8.0/en/explain.html
	switch explainableStmt.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
	case *sqlparser.Delete:
	case *sqlparser.Insert:
		autoincPlug := spanner.plugins.PlugAutoIncrement()
		if err := autoincPlug.Process(database, explainableStmt.(*sqlparser.Insert)); err != nil {
			return nil, err
		}
	case *sqlparser.Update:
	default:
		return nil, sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, "explain only supports SELECT/DELETE/INSERT/UNION")
	}

	simOptimizer := optimizer.NewSimpleOptimizer(log, database, query, explainableStmt, router)
	planTree, err := simOptimizer.BuildPlanTree()
	if err != nil {
		log.Error("proxy.explain.error:%+v", err)
		return nil, err
	}

	if len(planTree.Plans()) > 0 {
		msg := planTree.Plans()[0].JSON()
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(msg)),
		}
		qr.Rows = append(qr.Rows, row)
		return qr, nil
	}
	return qr, nil
}
