/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"strings"
	"xbase"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/driver"

	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func returnQuery(qr *sqltypes.Result, callback func(qr *sqltypes.Result) error, err error) error {
	if err != nil {
		return err
	}
	callback(qr)
	return nil
}

// ComQuery impl.
// Supports statements are:
// 1. DDL
// 2. DML
// 3. USE DB
func (spanner *Spanner) ComQuery(session *driver.Session, query string, callback func(qr *sqltypes.Result) error) error {
	var qr *sqltypes.Result
	log := spanner.log
	throttle := spanner.throttle
	diskChecker := spanner.diskChecker
	hasBackup := spanner.scatter.HasBackup()

	// Throttle.
	throttle.Acquire()
	defer throttle.Release()

	// Disk usage check.
	if diskChecker.HighWater() {
		return sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "%s", "no space left on device")
	}

	// Support for JDBC driver.
	if strings.HasPrefix(query, "/*") {
		qr, err := spanner.handleJDBCShows(session, query, nil)
		qr.Warnings = 1
		return returnQuery(qr, callback, err)
	}
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	node, err := sqlparser.Parse(query)
	if err != nil {
		log.Error("query[%v].parser.error: %v", query, err)
		return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, "", err.Error())
	}

	// Readonly check.
	if spanner.ReadOnly() {
		// DML Write denied.
		if spanner.IsDMLWrite(node) {
			return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "", "--read-only")
		}
		// DDL denied.
		if spanner.IsDDL(node) {
			return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "", "--read-only")
		}
	}

	switch node.(type) {
	case *sqlparser.Use:
		if qr, err = spanner.handleUseDB(session, query, node); err != nil {
			log.Error("proxy.usedb[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		spanner.auditLog(session, R, xbase.USEDB, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.DDL:
		if qr, err = spanner.handleDDL(session, query, node); err != nil {
			log.Error("proxy.DDL[%s].from.session[%v].error:%+v", query, session.ID(), err)
		} else {
			// Binlog.
			spanner.logEvent(session, xbase.DDL, query)
		}
		spanner.auditLog(session, W, xbase.DDL, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Show:
		show := node.(*sqlparser.Show)
		switch show.Type {
		case sqlparser.ShowDatabasesStr:
			if qr, err = spanner.handleShowDatabases(session, query, node); err != nil {
				log.Error("proxy.show.databases[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowStatusStr:
			if qr, err = spanner.handleShowStatus(session, query, node); err != nil {
				log.Error("proxy.show.status[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowVersionsStr:
			if qr, err = spanner.handleShowVersions(session, query, node); err != nil {
				log.Error("proxy.show.verions[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowEnginesStr:
			if qr, err = spanner.handleShowEngines(session, query, node); err != nil {
				log.Error("proxy.show.engines[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowTablesStr:
			if qr, err = spanner.handleShowTables(session, query, node); err != nil {
				log.Error("proxy.show.tables[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowCreateTableStr:
			if qr, err = spanner.handleShowCreateTable(session, query, node); err != nil {
				log.Error("proxy.show.create.table[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowProcesslistStr:
			if qr, err = spanner.handleShowProcesslist(session, query, node); err != nil {
				log.Error("proxy.show.processlist[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowQueryzStr:
			if qr, err = spanner.handleShowQueryz(session, query, node); err != nil {
				log.Error("proxy.show.queryz[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowTxnzStr:
			if qr, err = spanner.handleShowTxnz(session, query, node); err != nil {
				log.Error("proxy.show.txnz[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowCreateDatabaseStr:
			// Support for myloader.
			if qr, err = spanner.handleShowCreateDatabase(session, query, node); err != nil {
				log.Error("proxy.show.create.database[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowWarningsStr, sqlparser.ShowVariablesStr:
			// Support for JDBC.
			if qr, err = spanner.handleJDBCShows(session, query, node); err != nil {
				log.Error("proxy.JDBC.shows[%s].from.session[%v].error:%+v", query, session.ID(), err)
			}
		case sqlparser.ShowBinlogEventsStr:
			if qr, err = spanner.handleShowBinlogEvents(session, query, node); err != nil {
				log.Error("proxy.show.binlogevents[%s].error:%+v", query, err)
			}
		default:
			log.Error("proxy.show.unsupported[%s].from.session[%v]", query, session.ID())
			err = sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
		}
		spanner.auditLog(session, R, xbase.SHOW, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Insert:
		if qr, err = spanner.handleInsert(session, query, node); err != nil {
			log.Error("proxy.insert[%s].from.session[%v].error:%+v", xbase.TruncateQuery(query, 256), session.ID(), err)
		} else {
			// Binlog.
			spanner.logEvent(session, xbase.INSERT, query)
		}
		inode := node.(*sqlparser.Insert)
		switch inode.Action {
		case sqlparser.InsertStr:
			spanner.auditLog(session, W, xbase.INSERT, query, qr)
		case sqlparser.ReplaceStr:
			spanner.auditLog(session, W, xbase.REPLACE, query, qr)
		}
		return returnQuery(qr, callback, err)
	case *sqlparser.Delete:
		if qr, err = spanner.handleDelete(session, query, node); err != nil {
			log.Error("proxy.delete[%s].from.session[%v].error:%+v", query, session.ID(), err)
		} else {
			// Binlog.
			spanner.logEvent(session, xbase.DELETE, query)
		}
		spanner.auditLog(session, W, xbase.DELETE, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Update:
		if qr, err = spanner.handleUpdate(session, query, node); err != nil {
			log.Error("proxy.update[%s].from.session[%v].error:%+v", xbase.TruncateQuery(query, 256), session.ID(), err)
		} else {
			// Binlog.
			spanner.logEvent(session, xbase.UPDATE, query)
		}
		spanner.auditLog(session, W, xbase.UPDATE, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Select:
		typ := ""
		backupType := "/*backup*/"

		snode := node.(*sqlparser.Select)
		if len(snode.Comments) > 0 {
			if common.BytesToString(snode.Comments[0]) == backupType {
				typ = backupType
			}
		}

		switch typ {
		case backupType:
			log.Warning("proxy.select.for.backup:[%s].prepare", query)
			if err = spanner.handleSelectStream(session, query, node, callback); err != nil {
				log.Error("proxy.select.for.backup:[%s].error:%+v", xbase.TruncateQuery(query, 256), err)
				return err
			}
			log.Warning("proxy.select.for.backup:[%s].done", query)
			return nil
		default:
			switch snode.From[0].(type) {
			case *sqlparser.AliasedTableExpr:
				aliasTableExpr := snode.From[0].(*sqlparser.AliasedTableExpr)
				tb := aliasTableExpr.Expr.(sqlparser.TableName)
				table := tb.Name.String()
				if table == "dual" {
					if qr, err = spanner.handleDual(session, query, node); err != nil {
						log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
						// Send to AP node if we have.
						if hasBackup {
							if qr, err = spanner.handleBackupQuery(session, query, node); err != nil {
								log.Error("proxy.backup.select[%s].error:%+v", xbase.TruncateQuery(query, 256), err)
							}
						}
					}
				} else { // e.g.: select a from table [as] aliasTable;
					if qr, err = spanner.handleSelect(session, query, node); err != nil {
						log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
						// Send to AP node if we have.
						if hasBackup {
							if qr, err = spanner.handleBackupQuery(session, query, node); err != nil {
								log.Error("proxy.backup.select[%s].error:%+v", xbase.TruncateQuery(query, 256), err)
							}
						}
					}
				}
				spanner.auditLog(session, R, xbase.SELECT, query, qr)
				return returnQuery(qr, callback, err)
			default: // ParenTableExpr, JoinTableExpr
				if qr, err = spanner.handleSelect(session, query, node); err != nil {
					log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
					// Send to AP node if we have.
					if hasBackup {
						if qr, err = spanner.handleBackupQuery(session, query, node); err != nil {
							log.Error("proxy.backup.select[%s].error:%+v", xbase.TruncateQuery(query, 256), err)
						}
					}
				}
				spanner.auditLog(session, R, xbase.SELECT, query, qr)
				return returnQuery(qr, callback, err)
			}
		}
	case *sqlparser.Kill:
		if qr, err = spanner.handleKill(session, query, node); err != nil {
			log.Error("proxy.kill[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		spanner.auditLog(session, R, xbase.KILL, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Explain:
		if qr, err = spanner.handleExplain(session, query, node); err != nil {
			log.Error("proxy.explain[%s].from.session[%v].error:%+v", query, session.ID(), err)
		}
		spanner.auditLog(session, R, xbase.EXPLAIN, query, qr)
		return returnQuery(qr, callback, err)
	case *sqlparser.Transaction:
		// Support for myloader.
		log.Warning("proxy.query.transaction.query:%s", query)
		spanner.auditLog(session, R, xbase.TRANSACTION, query, qr)
		qr = &sqltypes.Result{Warnings: 1}
		return returnQuery(qr, callback, nil)
	case *sqlparser.Set:
		// Support for JDBC/myloader.
		log.Warning("proxy.query.set.query:%s", query)
		qr = &sqltypes.Result{Warnings: 1}
		spanner.auditLog(session, R, xbase.SET, query, qr)
		return returnQuery(qr, callback, nil)
	default:
		log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
		spanner.auditLog(session, R, xbase.UNSUPPORT, query, qr)
		return sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "unsupported.query:%v", query)
	}
}

// IsDML returns the DML query or not.
func (spanner *Spanner) IsDML(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.Select, *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update:
		return true
	}
	return false
}

// IsDMLWrite returns the DML write or not.
func (spanner *Spanner) IsDMLWrite(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update:
		return true
	}
	return false
}

// IsDDL returns the DDL query or not.
func (spanner *Spanner) IsDDL(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.DDL:
		return true
	}
	return false
}
