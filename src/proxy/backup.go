/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleBackupQuery used to execute read query to the backup node.
func (spanner *Spanner) handleBackupQuery(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	timeout := spanner.conf.Proxy.QueryTimeout
	return spanner.queryBackupWithTimeout(session, query, node, timeout)
}

func (spanner *Spanner) queryBackupWithTimeout(session *driver.Session, query string, node sqlparser.Statement, timeout int) (*sqltypes.Result, error) {
	var qr *sqltypes.Result

	log := spanner.log
	conf := spanner.conf
	scatter := spanner.scatter
	sessions := spanner.sessions

	// Make sure we have the backup node.
	if scatter.HasBackup() {
		txn, err := scatter.CreateBackupTransaction()
		if err != nil {
			log.Error("spanner.backup.read[%s].txn.create.error:[%v]", query, err)
			return nil, err
		}
		defer txn.Finish()

		// txn limits.
		txn.SetTimeout(timeout)
		txn.SetMaxResult(conf.Proxy.MaxResultSize)

		// binding.
		sessions.TxnBinding(session, txn, node, query)
		defer sessions.TxnUnBinding(session)
		if qr, err = txn.Execute(session.Schema(), query); err != nil {
			log.Error("spanner.backup.read[%s].error:[%v]", query, err)
		}
		return qr, err
	}
	return nil, errors.New("we.do.not.have.the.backup.node")
}

func (spanner *Spanner) handleBackupWrite(db string, query string) (*sqltypes.Result, error) {
	timeout := spanner.conf.Proxy.QueryTimeout
	return spanner.writeBackupWithTimeout(db, query, timeout)
}

func (spanner *Spanner) changeBackupEngine(ddl *sqlparser.DDL) {
	if ddl.TableSpec == nil {
		ddl.TableSpec = &sqlparser.TableSpec{}
	}

	defaultEngine := spanner.conf.Proxy.BackupDefaultEngine
	if defaultEngine != "" {
		ddl.TableSpec.Options.Engine = defaultEngine
	}
}

func (spanner *Spanner) handleBackupDDL(db string, query string) (*sqltypes.Result, error) {
	log := spanner.log
	node, err := sqlparser.Parse(query)
	if err != nil {
		log.Error("spaner.backup.parser.ddl[%v].error:%v", query, err)
		return nil, err
	}
	// We only rewrite the 'CREATE TABLE' query.
	ddl := node.(*sqlparser.DDL)
	if ddl.Action == sqlparser.CreateTableStr {
		spanner.changeBackupEngine(ddl)
		query = sqlparser.String(ddl)
	}
	spanner.log.Warning("spanner.handle.backup.ddl.rewrite.query:%s", query)
	timeout := spanner.conf.Proxy.DDLTimeout
	return spanner.writeBackupWithTimeout(db, query, timeout)
}

func (spanner *Spanner) writeBackupWithTimeout(db string, query string, timeout int) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	log := spanner.log
	scatter := spanner.scatter

	// Make sure we have the backup node.
	if scatter.HasBackup() {
		txn, err := scatter.CreateBackupTransaction()
		if err != nil {
			log.Error("spanner.backup.write[%s].txn.create.error:[%v]", query, err)
			return nil, err
		}
		defer txn.Finish()

		// txn limits.
		txn.SetTimeout(timeout)
		if qr, err = txn.Execute(db, query); err != nil {
			log.Error("spanner.backup.wirte[%s].execute.error:[%v]", query, err)
		}
		return qr, err
	}
	return nil, nil
}
