/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"fmt"
	"sync"
	"time"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqldb"
)

var (
	txnCounterXaStart         = "#xa.start"
	txnCounterXaStartError    = "#xa.start.error"
	txnCounterXaEnd           = "#xa.end"
	txnCounterXaEndError      = "#xa.end.error"
	txnCounterXaPrepare       = "#xa.prepare"
	txnCounterXaPrepareError  = "#xa.prepare.error"
	txnCounterXaCommit        = "#xa.commit"
	txnCounterXaCommitError   = "#xa.commit.error"
	txnCounterXaRollback      = "#xa.rollback"
	txnCounterXaRollbackError = "#xa.rollback.error"
)

var (
	xaMaxRetryNum = 10
)

type txnXAState int32

const (
	txnXAStateNone txnXAState = iota
	txnXAStateStart
	txnXAStateStartFinished
	txnXAStateEnd
	txnXAStateEndFinished
	txnXAStatePrepare
	txnXAStatePrepareFinished
	txnXAStateCommit
	txnXAStateCommitFinished
	txnXAStateRollback
	txnXAStateRollbackFinished
	txnXAStateRecover
	txnXAStateRecoverFinished
)

// executeXACommand used to execute XA statements.
func (txn *Txn) executeXACommand(query string, state txnXAState) error {
	rctx := &xcontext.RequestContext{
		RawQuery: query,
		Mode:     txn.req.Mode,
		Querys:   txn.req.Querys,
	}
	return txn.executeXA(rctx, state)
}

// executeXA only used to execute the 'XA START','XA END', 'XA PREPARE', 'XA COMMIT'/'XA ROLLBACK' statements.
func (txn *Txn) executeXA(req *xcontext.RequestContext, state txnXAState) error {
	var err error
	var mu sync.Mutex
	var wg sync.WaitGroup

	log := txn.log
	allErrors := make([]error, 0, 8)

	txn.state.Set(int32(txnStateExecutingTwoPC))
	defer queryStats.Record("txn.2pc.execute", time.Now())
	oneShard := func(state txnXAState, back string, txn *Txn, query string) {
		var x error
		var c Connection
		defer wg.Done()

		switch state {
		case txnXAStateStart, txnXAStateEnd, txnXAStatePrepare:
			if c, x = txn.twopcConnection(back); x != nil {
				log.Error("txn.xa.fetch.connection.state[%v].on[%s].query[%v].error:%+v", state, back, query, x)
			} else {
				if _, x = c.Execute(query); x != nil {
					log.Error("txn.xa.execute[%v].on[%v].error:%+v", query, c.Address(), x)
				}
			}
		case txnXAStateCommit, txnXAStateRollback:
			maxRetry := xaMaxRetryNum
			for retry := 0; retry < maxRetry; retry++ {
				if retry == 0 {
					if c, x = txn.twopcConnection(back); x != nil {
						log.Error("txn.xa.twopc.connection[maxretry:%v, retried:%v].state[%v].on[%s].query[%v].error:%+v", maxRetry, retry, state, back, query, x)
						continue
					}
				} else {
					// Retry the connection for commit/rollback.
					if c, x = txn.reFetchTwopcConnection(back); x != nil {
						log.Error("txn.xa.fetch.connection[maxretry:%v, retried:%v].state[%v].on[%s].query[%v].error:%+v", maxRetry, retry, state, back, query, x)
						time.Sleep(time.Second * time.Duration(retry))
						continue
					}
				}

				if _, x = c.Execute(query); x != nil {
					log.Error("txn.xa.execute[maxretry:%v, retried:%v].state[%v].on[%v].query[%v].error[%T]:%+v", maxRetry, retry, state, c.Address(), query, x, x)
					if sqlErr, ok := x.(*sqldb.SQLError); ok {
						// XAE04:
						// https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_xaer_nota
						// Error: 1397 SQLSTATE: XAE04 (ER_XAER_NOTA)
						// Message: XAER_NOTA: Unknown XID
						if sqlErr.Num == 1397 {
							log.Error("txn.xa.[%v].XAE04.error....", state)
							break
						}
					}
					time.Sleep(time.Second * time.Duration(retry))
					continue
				}
				break
			}
		}

		if x != nil {
			mu.Lock()
			allErrors = append(allErrors, x)
			mu.Unlock()
		}
	}

	switch req.Mode {
	case xcontext.ReqNormal:
		backends := make(map[string]bool)
		for _, query := range req.Querys {
			_, ok := backends[query.Backend]
			if !ok {
				backends[query.Backend] = true
			}
		}

		// Only do XA when Querys's backends numbers larger than one.
		beLen := len(backends)
		if beLen > 1 {
			switch state {
			case txnXAStateCommit, txnXAStateRollback:
				// Acquire the commit lock if the txn is write.
				txn.mgr.CommitLock()
				defer txn.mgr.CommitUnlock()
			}

			for back := range backends {
				wg.Add(1)
				go oneShard(state, back, txn, req.RawQuery)
			}
		}
	case xcontext.ReqScatter:
		backends := txn.backends
		switch state {
		case txnXAStateCommit, txnXAStateRollback:
			// Acquire the commit lock when the txn commit/rollback
			txn.mgr.CommitLock()
			defer txn.mgr.CommitUnlock()
		}

		for back := range backends {
			wg.Add(1)
			go oneShard(state, back, txn, req.RawQuery)
		}
	}

	wg.Wait()
	if len(allErrors) > 0 {
		err = allErrors[0]
	}
	return err
}

func (txn *Txn) xaStart() error {
	log := txn.log
	txnCounters.Add(txnCounterXaStart, 1)
	txn.xaState.Set(int32(txnXAStateStart))
	defer func() { txn.xaState.Set(int32(txnXAStateStartFinished)) }()

	if txn.isMultiStmtTxn {
		txn.xid = fmt.Sprintf("MULTRXID-%v-%v", time.Now().Format("20060102150405"), txn.id)
	} else {
		txn.xid = fmt.Sprintf("RXID-%v-%v", time.Now().Format("20060102150405"), txn.id)
	}
	start := fmt.Sprintf("XA START '%v'", txn.xid)
	if err := txn.executeXACommand(start, txnXAStateStart); err != nil {
		log.Error("xa.start[%v].error:%v", start, err)
		txnCounters.Add(txnCounterXaStartError, 1)
		txn.incErrors()
		return err
	}
	log.Debug("%v", start)
	return nil
}

func (txn *Txn) xaEnd() error {
	log := txn.log
	txnCounters.Add(txnCounterXaEnd, 1)
	txn.xaState.Set(int32(txnXAStateEnd))
	defer func() { txn.xaState.Set(int32(txnXAStateEndFinished)) }()

	end := fmt.Sprintf("XA END '%v'", txn.xid)
	if err := txn.executeXACommand(end, txnXAStateEnd); err != nil {
		log.Error("xa.end[%v].error:%v", end, err)
		txnCounters.Add(txnCounterXaEndError, 1)
		txn.incErrors()
		return err
	}
	log.Debug("%v", end)
	return nil
}

func (txn *Txn) xaPrepare() error {
	log := txn.log
	txnCounters.Add(txnCounterXaPrepare, 1)
	txn.xaState.Set(int32(txnXAStatePrepare))
	defer func() { txn.xaState.Set(int32(txnXAStatePrepareFinished)) }()

	prepare := fmt.Sprintf("XA PREPARE '%v'", txn.xid)
	if err := txn.executeXACommand(prepare, txnXAStatePrepare); err != nil {
		log.Error("xa.prepare[%v].error:%v", prepare, err)
		txnCounters.Add(txnCounterXaPrepareError, 1)
		txn.incErrors()
		return err
	}
	log.Debug("%v", prepare)
	return nil
}

func (txn *Txn) xaCommit() {
	log := txn.log
	txnCounters.Add(txnCounterXaCommit, 1)
	txn.xaState.Set(int32(txnXAStateCommit))
	// if the commit is failed, the status is set txnXAStateCommitFinished which is not used.
	// If need, more states will be added.
	defer func() { txn.xaState.Set(int32(txnXAStateCommitFinished)) }()

	commit := fmt.Sprintf("XA COMMIT '%v'", txn.xid)
	if err := txn.executeXACommand(commit, txnXAStateCommit); err != nil {
		log.Error("xa.commit[%v].error:%v", commit, err)
		txn.incErrors()
		txnCounters.Add(txnCounterXaCommitError, 1)

		if err := txn.WriteXaCommitErrLog(txnXACommitErrStateCommit); err != nil {
			log.Error("txn.xa.WriteXaCommitErrLog.query[%v].error[%T]:%+v", commit, err, err)
		}
	}
	log.Debug("%v", commit)
}

func (txn *Txn) xaRollback() {
	log := txn.log
	txnCounters.Add(txnCounterXaRollback, 1)
	txn.xaState.Set(int32(txnXAStateRollback))
	defer func() { txn.xaState.Set(int32(txnXAStateRollbackFinished)) }()

	rollback := fmt.Sprintf("XA ROLLBACK '%v'", txn.xid)
	if err := txn.executeXACommand(rollback, txnXAStateRollback); err != nil {
		log.Error("xa.rollback[%v].error:%v", rollback, err)
		txnCounters.Add(txnCounterXaRollbackError, 1)
		txn.incErrors()

		if err := txn.WriteXaCommitErrLog(txnXACommitErrStateRollback); err != nil {
			log.Error("txn.xa.WriteXaCommitErrLog.query[%v].error[%T]:%+v", rollback, err, err)
		}
	}
	log.Debug("%v", rollback)
}
