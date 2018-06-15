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
	"executor"
	"optimizer"
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ExecuteTwoPC allows multi-shards transactions with 2pc commit.
func (spanner *Spanner) ExecuteTwoPC(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	conf := spanner.conf
	router := spanner.router
	scatter := spanner.scatter
	sessions := spanner.sessions

	// transaction.
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()

	// txn limits.
	txn.SetTimeout(conf.Proxy.QueryTimeout)
	txn.SetMaxResult(conf.Proxy.MaxResultSize)

	// binding.
	sessions.TxnBinding(session, txn, node, query)
	defer sessions.TxnUnBinding(session)

	// Transaction begin.
	if err := txn.Begin(); err != nil {
		log.Error("spanner.execute.2pc.txn.begin.error:[%v]", err)
		return nil, err
	}

	// Transaction execute.
	plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	if err != nil {
		return nil, err
	}

	executors := executor.NewTree(log, plans, txn)
	qr, err := executors.Execute()
	if err != nil {
		if x := txn.Rollback(); x != nil {
			log.Error("spanner.execute.2pc.error.to.rollback.still.error:[%v]", x)
		}
		return nil, err
	}
	if err := txn.Commit(); err != nil {
		log.Error("spanner.execute.2pc.txn.commit.error:[%v]", err)
		return nil, err
	}
	return qr, nil
}

// ExecuteNormal used to execute non-2pc querys to shards with QueryTimeout limits.
func (spanner *Spanner) ExecuteNormal(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	timeout := spanner.conf.Proxy.QueryTimeout
	return spanner.executeWithTimeout(session, database, query, node, timeout)
}

// ExecuteDDL used to execute ddl querys to the shards with DDLTimeout limits, used for create/drop index long time operation.
func (spanner *Spanner) ExecuteDDL(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	spanner.log.Info("spanner.execute.ddl.query:%s", query)
	timeout := spanner.conf.Proxy.DDLTimeout
	return spanner.executeWithTimeout(session, database, query, node, timeout)
}

// ExecuteNormal used to execute non-2pc querys to shards with timeout limits.
// timeout:
//    0x01. if timeout <= 0, no limits.
//    0x02. if timeout > 0, the query will be interrupted if the timeout(in millisecond) is exceeded.
func (spanner *Spanner) executeWithTimeout(session *driver.Session, database string, query string, node sqlparser.Statement, timeout int) (*sqltypes.Result, error) {
	log := spanner.log
	conf := spanner.conf
	router := spanner.router
	scatter := spanner.scatter
	sessions := spanner.sessions

	// transaction.
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()

	// txn limits.
	txn.SetTimeout(timeout)
	txn.SetMaxResult(conf.Proxy.MaxResultSize)

	// binding.
	sessions.TxnBinding(session, txn, node, query)
	defer sessions.TxnUnBinding(session)

	plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	if err != nil {
		return nil, err
	}
	executors := executor.NewTree(log, plans, txn)
	qr, err := executors.Execute()
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// ExecuteStreamFetch used to execute a stream fetch query.
func (spanner *Spanner) ExecuteStreamFetch(session *driver.Session, database string, query string, node sqlparser.Statement, callback func(qr *sqltypes.Result) error, streamBufferSize int) error {
	log := spanner.log
	router := spanner.router
	scatter := spanner.scatter
	sessions := spanner.sessions

	// transaction.
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return err
	}
	defer txn.Finish()

	// binding.
	sessions.TxnBinding(session, txn, node, query)
	defer sessions.TxnUnBinding(session)

	selectNode, ok := node.(*sqlparser.Select)
	if !ok {
		return errors.New("ExecuteStreamFetch.only.support.select")
	}

	plan := planner.NewSelectPlan(log, database, query, selectNode, router)
	if err := plan.Build(); err != nil {
		return err
	}
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = plan.ReqMode
	reqCtx.Querys = plan.Querys
	reqCtx.RawQuery = plan.RawQuery
	return txn.ExecuteStreamFetch(reqCtx, callback, streamBufferSize)
}

// Execute used to execute querys to shards.
func (spanner *Spanner) Execute(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	// Execute.
	if spanner.isTwoPC() {
		if spanner.IsDML(node) {
			return spanner.ExecuteTwoPC(session, database, query, node)
		}
		return spanner.ExecuteNormal(session, database, query, node)
	}
	return spanner.ExecuteNormal(session, database, query, node)
}

// ExecuteSingle used to execute query on one shard without planner.
// The query must contain the database, such as db.table.
func (spanner *Spanner) ExecuteSingle(query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.execute.single.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteSingle(query)
}

// ExecuteScatter used to execute query on all shards without planner.
func (spanner *Spanner) ExecuteScatter(query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.execute.scatter.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteScatter(query)
}

// ExecuteOnThisBackend used to executye query on the backend whitout planner.
func (spanner *Spanner) ExecuteOnThisBackend(backend string, query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.execute.on.this.backend..txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteOnThisBackend(backend, query)
}

// ExecuteOnBackup used to executye query on the backup.
func (spanner *Spanner) ExecuteOnBackup(database string, query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateBackupTransaction()
	if err != nil {
		log.Error("spanner.execute.on.backup..txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.Execute(database, query)
}
