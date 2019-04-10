/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"strings"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

const (
	var_mysql_autocommit      = "autocommit"
	var_radon_streaming_fetch = "radon_streaming_fetch"
)

// handleSet used to handle the SET command.
func (spanner *Spanner) handleSet(session *driver.Session, query string, node *sqlparser.Set) (*sqltypes.Result, error) {
	log := spanner.log
	txSession := spanner.sessions.getTxnSession(session)

	for _, expr := range node.Exprs {
		name := expr.Name.Lowered()
		if strings.HasPrefix(name, "@@session.") {
			name = strings.TrimPrefix(name, "@@session.")
		}

		switch name {
		case var_radon_streaming_fetch:
			switch expr := expr.Expr.(type) {
			case *sqlparser.SQLVal:
				switch expr.Type {
				case sqlparser.StrVal:
					val := strings.ToLower(string(expr.Val))
					switch val {
					case "on":
						txSession.setStreamingFetchVar(true)
					case "off":
						txSession.setStreamingFetchVar(false)
					}
				default:
					return nil, fmt.Errorf("Invalid value type: %v", sqlparser.String(expr))
				}
			case sqlparser.BoolVal:
				if expr {
					txSession.setStreamingFetchVar(true)
				} else {
					txSession.setStreamingFetchVar(false)
				}
			}
		case var_mysql_autocommit:
			var autocommit = true

			switch expr := expr.Expr.(type) {
			case *sqlparser.SQLVal:
				switch expr.Type {
				case sqlparser.IntVal:
					if expr.Val[0]=='0' {
						autocommit = false
					}
				}
			}
			if !autocommit {
				query := "begin"
				node := &sqlparser.Transaction{
					Action: "begin",
				}
				qr, err := spanner.handleMultiStmtTxn(session, query, node)
				if err != nil {
					log.Error("proxy.transaction[%s](by.autocommit).from.session[%v].error:%+v", query, session.ID(), err)
					return nil, err
				}
				return qr, nil
			}
		default:
			log.Warning("unhandle.set[%v]:%v", name, query)
		}
	}
	qr := &sqltypes.Result{Warnings: 1}
	return qr, nil
}
