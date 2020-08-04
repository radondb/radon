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
	"time"

	"monitor"
	"xbase"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
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
// 3. USE DB: MySQL client use 'database' won't pass here, FIXME.
func (spanner *Spanner) ComQuery(session *driver.Session, query string, bindVariables map[string]*querypb.BindVariable, callback func(qr *sqltypes.Result) error) error {
	var qr *sqltypes.Result
	log := spanner.log
	throttle := spanner.throttle
	diskChecker := spanner.diskChecker
	timeStart := time.Now()
	slowQueryTime := time.Duration(spanner.conf.Proxy.LongQueryTime) * time.Second

	// Throttle.
	throttle.Acquire()
	defer throttle.Release()

	// Disk usage check.
	if diskChecker.HighWater() {
		return sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "%s", "no space left on device")
	}

	// Support for JDBC/Others driver.
	if spanner.isConnectorFilter(query) {
		qr, err := spanner.handleJDBCShows(session, query, nil)
		if err == nil {
			qr.Warnings = 1
		}
		return returnQuery(qr, callback, err)
	}

	// Trim space and ';'.
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	node, err := sqlparser.Parse(query)
	if err != nil {
		log.Error("query[%v].parser.error: %v", query, err)
		return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, err.Error())
	}

	// Bind variables.
	if bindVariables != nil {
		parsedQuery := sqlparser.NewParsedQuery(node)
		query, err = parsedQuery.GenerateQuery(bindVariables, nil)
		if err != nil {
			log.Error("query[%v].parsed.GenerateQuery.error: %v, bind:%+v", query, err, bindVariables)
			return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, err.Error())
		}

		// This sucks.
		node, err = sqlparser.Parse(query)
		if err != nil {
			log.Error("query[%v].parser.error: %v", query, err)
			return sqldb.NewSQLError(sqldb.ER_SYNTAX_ERROR, err.Error())
		}
	}
	log.Debug("query:%v", query)

	// Readonly check.
	if spanner.ReadOnly() {
		// DML Write denied.
		if spanner.IsDMLWrite(node) {
			return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "--read-only")
		}
		// DDL denied.
		if spanner.IsDDL(node) {
			return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "--read-only")
		}
		// Admin command denied.
		if spanner.IsAdminCmd(node) {
			return sqldb.NewSQLError(sqldb.ER_OPTION_PREVENTS_STATEMENT, "--read-only")
		}
	}

	defer func() {
		queryStat(node, timeStart, slowQueryTime, err)
	}()
	// The status of the execution result, zero for success and non-zero for failure.
	status := uint16(0)
	switch node := node.(type) {
	case *sqlparser.Use:
		if qr, err = spanner.handleUseDB(session, query, node); err != nil {
			log.Error("proxy.usedb[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.USEDB, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.DDL:
		if qr, err = spanner.handleDDL(session, query, node); err != nil {
			log.Error("proxy.DDL[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, W, xbase.DDL, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Show:
		show := node
		switch show.Type {
		case sqlparser.ShowDatabasesStr:
			if qr, err = spanner.handleShowDatabases(session, query, node); err != nil {
				log.Error("proxy.show.databases[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowStatusStr:
			if qr, err = spanner.handleShowStatus(session, query, node); err != nil {
				log.Error("proxy.show.status[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowVersionsStr:
			if qr, err = spanner.handleShowVersions(session, query, node); err != nil {
				log.Error("proxy.show.verions[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowEnginesStr:
			if qr, err = spanner.handleShowEngines(session, query, node); err != nil {
				log.Error("proxy.show.engines[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowTablesStr:
			// Support for SHOW FULL TBALES which can be parsed used by Navicat
			// TODO: need to support: SHOW [FULL] TABLES [FROM db_name] [like_or_where]
			if qr, err = spanner.handleShowTables(session, query, node); err != nil {
				log.Error("proxy.show.tables[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowCreateTableStr:
			if qr, err = spanner.handleShowCreateTable(session, query, node); err != nil {
				log.Error("proxy.show.create.table[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowColumnsStr:
			if qr, err = spanner.handleShowColumns(session, query, node); err != nil {
				log.Error("proxy.show.colomns[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowProcesslistStr:
			if qr, err = spanner.handleShowProcesslist(session, query, node); err != nil {
				log.Error("proxy.show.processlist[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowQueryzStr:
			if qr, err = spanner.handleShowQueryz(session, query, node); err != nil {
				log.Error("proxy.show.queryz[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowTxnzStr:
			if qr, err = spanner.handleShowTxnz(session, query, node); err != nil {
				log.Error("proxy.show.txnz[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowCreateDatabaseStr:
			// Support for myloader.
			if qr, err = spanner.handleShowCreateDatabase(session, query, node); err != nil {
				log.Error("proxy.show.create.database[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowTableStatusStr:
			// Support for Navicat.
			if qr, err = spanner.handleShowTableStatus(session, query, node); err != nil {
				log.Error("proxy.show.table.status[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		case sqlparser.ShowWarningsStr, sqlparser.ShowVariablesStr:
			// Support for JDBC.
			if qr, err = spanner.handleJDBCShows(session, query, node); err != nil {
				log.Error("proxy.JDBC.shows[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
		default:
			log.Error("proxy.show.unsupported[%s].from.session[%v]", query, session.ID())
			status = sqldb.ER_UNKNOWN_ERROR
			err = sqldb.NewSQLErrorf(status, "unsupported.query:%v", query)
		}
		spanner.auditLog(session, R, xbase.SHOW, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Insert:
		if qr, err = spanner.handleInsert(session, query, node); err != nil {
			log.Error("proxy.insert[%s].from.session[%v].error:%+v", xbase.TruncateQuery(query, 256), session.ID(), err)
			status = 1
		}
		switch node.Action {
		case sqlparser.InsertStr:
			spanner.auditLog(session, W, xbase.INSERT, query, qr, status)
		case sqlparser.ReplaceStr:
			spanner.auditLog(session, W, xbase.REPLACE, query, qr, status)
		}
		return returnQuery(qr, callback, err)
	case *sqlparser.Delete:
		if qr, err = spanner.handleDelete(session, query, node); err != nil {
			log.Error("proxy.delete[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, W, xbase.DELETE, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Update:
		if qr, err = spanner.handleUpdate(session, query, node); err != nil {
			log.Error("proxy.update[%s].from.session[%v].error:%+v", xbase.TruncateQuery(query, 256), session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, W, xbase.UPDATE, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Select:
		streamingFetch := false
		txSession := spanner.sessions.getTxnSession(session)
		if txSession.getStreamingFetchVar() {
			streamingFetch = true
		} else {
			if len(node.Comments) > 0 {
				comment := strings.Replace(common.BytesToString(node.Comments[0]), " ", "", -1)
				if comment == "/*+streaming*/" {
					streamingFetch = true
				}
			}
		}

		if streamingFetch {
			if err = spanner.handleSelectStream(session, query, node, callback); err != nil {
				log.Error("proxy.select.for.backup:[%s].error:%+v", xbase.TruncateQuery(query, 256), err)
				return err
			}
			return nil
		}

		switch node.From[0].(type) {
		case *sqlparser.AliasedTableExpr:
			aliasTableExpr := node.From[0].(*sqlparser.AliasedTableExpr)
			tb, ok := aliasTableExpr.Expr.(sqlparser.TableName)
			if !ok {
				// Subquery.
				if qr, err = spanner.handleSelect(session, query, node); err != nil {
					log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
					status = 1
				}
			} else {
				if tb.Name.String() == "dual" {
					// Select 1.
					if qr, err = spanner.ExecuteSingle(query); err != nil {
						log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
						status = 1
					}
				} else if spanner.router.IsSystemDB(tb.Qualifier.String()) {
					// System database select.
					if qr, err = spanner.handleSelectSystem(session, query, node); err != nil {
						log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
						status = 1
					}
				} else {
					// Normal select.
					if qr, err = spanner.handleSelect(session, query, node); err != nil {
						log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
						status = 1
					}
				}
			}
			spanner.auditLog(session, R, xbase.SELECT, query, qr, status)
			return returnQuery(qr, callback, err)
		default: // ParenTableExpr, JoinTableExpr
			if qr, err = spanner.handleSelect(session, query, node); err != nil {
				log.Error("proxy.select[%s].from.session[%v].error:%+v", query, session.ID(), err)
				status = 1
			}
			spanner.auditLog(session, R, xbase.SELECT, query, qr, status)
			return returnQuery(qr, callback, err)
		}
	case *sqlparser.Union:
		if qr, err = spanner.handleSelect(session, query, node); err != nil {
			log.Error("proxy.union[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, W, xbase.UPDATE, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Kill:
		if qr, err = spanner.handleKill(session, query, node); err != nil {
			log.Error("proxy.kill[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.KILL, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Explain:
		if qr, err = spanner.handleExplain(session, query, node); err != nil {
			log.Error("proxy.explain[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.EXPLAIN, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Transaction:
		// Support for myloader.
		// Support Multiple-statement Transaction
		if qr, err = spanner.handleMultiStmtTxn(session, query, node); err != nil {
			log.Error("proxy.transaction[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.TRANSACTION, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Radon:
		if qr, err = spanner.handleRadon(session, query, node); err != nil {
			log.Error("proxy.admin[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.RADON, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Set:
		log.Warning("proxy.query.set.query:%s", query)
		if qr, err = spanner.handleSet(session, query, node); err != nil {
			log.Error("proxy.set[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.SET, query, qr, status)
		return returnQuery(qr, callback, err)
	case *sqlparser.Checksum:
		log.Warning("proxy.query.checksum.query:%s", query)
		if qr, err = spanner.handleChecksumTable(session, query, node); err != nil {
			log.Error("proxy.checksum[%s].from.session[%v].error:%+v", query, session.ID(), err)
			status = 1
		}
		spanner.auditLog(session, R, xbase.CHECKSUM, query, qr, status)
		return returnQuery(qr, callback, err)
	default:
		log.Error("proxy.unsupported[%s].from.session[%v]", query, session.ID())
		status = sqldb.ER_UNKNOWN_ERROR
		err = sqldb.NewSQLErrorf(status, "unsupported.query:%v", query)
		spanner.auditLog(session, R, xbase.UNSUPPORT, query, qr, status)
		return err
	}
}

// IsDML returns the DML query or not.
func (spanner *Spanner) IsDML(node sqlparser.Statement) bool {
	switch node.(type) {
	case *sqlparser.Select, *sqlparser.Union, *sqlparser.Insert, *sqlparser.Delete, *sqlparser.Update:
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

// IsAdminCmd returns the Admin query or not.
// Some of admin commands are prohibited when radon is read-only.
func (spanner *Spanner) IsAdminCmd(node sqlparser.Statement) bool {
	if node, ok := node.(*sqlparser.Radon); ok {
		switch node.Action {
		case sqlparser.AttachStr, sqlparser.DetachStr, sqlparser.ReshardStr, sqlparser.CleanupStr,
			sqlparser.XACommitStr, sqlparser.XARollbackStr, sqlparser.RebalanceStr:
			return true
		}
	}
	return false
}

func queryStat(node sqlparser.Statement, timeStart time.Time, slowQueryTime time.Duration, err error) {
	var command string
	switch node.(type) {
	case *sqlparser.Use:
		command = "Use"
	case *sqlparser.DDL:
		command = "DDL"
	case *sqlparser.Show:
		command = "Show"
	case *sqlparser.Insert:
		command = "Insert"
	case *sqlparser.Delete:
		command = "Delete"
	case *sqlparser.Update:
		command = "Update"
	case *sqlparser.Select:
		command = "Select"
	case *sqlparser.Union:
		command = "Union"
	case *sqlparser.Kill:
		command = "Kill"
	case *sqlparser.Explain:
		command = "Explain"
	case *sqlparser.Transaction:
		command = "Transaction"
	case *sqlparser.Set:
		command = "Set"
	default:
		command = "Unsupport"
	}
	queryTime := time.Since(timeStart)
	if err != nil {
		if queryTime > slowQueryTime {
			monitor.SlowQueryTotalCounterInc(command, "Error")
		}
		monitor.QueryTotalCounterInc(command, "Error")
	} else {
		if queryTime > slowQueryTime {
			monitor.SlowQueryTotalCounterInc(command, "OK")
		}
		monitor.QueryTotalCounterInc(command, "OK")
	}
}
