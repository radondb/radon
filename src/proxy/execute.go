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

	"executor"
	"optimizer"
	"planner"
	"planner/builder"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ExecuteMultiStmtsInTxn used to execute multiple statements in the transaction.
func (spanner *Spanner) ExecuteMultiStmtsInTxn(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	router := spanner.router
	sessions := spanner.sessions
	txSession := sessions.getTxnSession(session)

	sessions.MultiStmtTxnBinding(session, nil, node, query)

	plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	if err != nil {
		return nil, err
	}
	executors := executor.NewTree(log, plans, txSession.transaction)
	qr, err := executors.Execute()
	if err != nil {
		// need the user to rollback
		return nil, err
	}

	sessions.MultiStmtTxnUnBinding(session, false)
	return qr, nil
}

// ExecuteSingleStmtTxnTwoPC used to execute single statement transaction with 2pc commit.
func (spanner *Spanner) ExecuteSingleStmtTxnTwoPC(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
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
	txn.SetMaxJoinRows(conf.Proxy.MaxJoinRows)
	txn.SetIsExecOnRep(isExecOnRep(conf.Proxy.LoadBalance, node))

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
		if x := txn.RollbackPhaseOne(); x != nil {
			log.Error("spanner.execute.2pc.error.to.rollback.phaseOne.still.error:[%v]", x)
		}
		return nil, err
	}

	// Transaction commit.
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

	txSession := spanner.sessions.getTxnSession(session)
	if spanner.isTwoPC() && txSession.transaction != nil {
		return nil, errors.Errorf("in.multiStmtTrans.unsupported.DDL:%v.", query)
	}

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
	txn.SetMaxJoinRows(conf.Proxy.MaxJoinRows)
	txn.SetIsExecOnRep(isExecOnRep(conf.Proxy.LoadBalance, node))

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
func (spanner *Spanner) ExecuteStreamFetch(session *driver.Session, database string, query string, node sqlparser.Statement, callback func(qr *sqltypes.Result) error) error {
	log := spanner.log
	conf := spanner.conf
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

	txn.SetIsExecOnRep(conf.Proxy.LoadBalance != 0)

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
	m, ok := plan.Root.(*builder.MergeNode)
	if !ok {
		return errors.New("ExecuteStreamFetch.unsupport.cross-shard.join")
	}
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = m.ReqMode
	reqCtx.Querys = m.GetQuery()
	reqCtx.RawQuery = plan.RawQuery
	streamBufferSize := spanner.conf.Proxy.StreamBufferSize
	return txn.ExecuteStreamFetch(reqCtx, callback, streamBufferSize)
}

// ExecuteDML used to execute some DML querys to shards.
func (spanner *Spanner) ExecuteDML(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	privilegePlug := spanner.plugins.PlugPrivilege()
	if err := privilegePlug.Check(session.Schema(), session.User(), node); err != nil {
		return nil, err
	}

	if spanner.isTwoPC() {
		txSession := spanner.sessions.getTxnSession(session)
		if spanner.IsDML(node) {
			if txSession.transaction == nil {
				return spanner.ExecuteSingleStmtTxnTwoPC(session, database, query, node)
			} else {
				return spanner.ExecuteMultiStmtsInTxn(session, database, query, node)
			}
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

// isExecOnRep will be called when the query is not in multi-statement txn:
// 1. When the query is Select, the parameter `loadBalance` can take effect.
// 2. By using hint can directly decide the load-balance mode, instead of check `loadBalance`.
func isExecOnRep(loadBalance int, node sqlparser.Statement) bool {
	if node, ok := node.(*sqlparser.Select); ok {
		if len(node.Comments) > 0 {
			comment := strings.Replace(common.BytesToString(node.Comments[0]), " ", "", -1)
			if comment == "/*+loadbalance=0*/" {
				return false
			}
			if comment == "/*+loadbalance=1*/" {
				return true
			}
		}
		return loadBalance != 0
	}
	return false
}
