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

	"xbase/sync2"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	txnCounterTxnCreate             = "#txn.create"
	txnCounterTwopcConnectionError  = "#get.twopc.connection.error"
	txnCounterNormalConnectionError = "#get.normal.connection.error"
	txnCounterXaStart               = "#xa.start"
	txnCounterXaStartError          = "#xa.start.error"
	txnCounterXaEnd                 = "#xa.end"
	txnCounterXaEndError            = "#xa.end.error"
	txnCounterXaPrepare             = "#xa.prepare"
	txnCounterXaPrepareError        = "#xa.prepare.error"
	txnCounterXaCommit              = "#xa.commit"
	txnCounterXaCommitError         = "#xa.commit.error"
	txnCounterXaRollback            = "#xa.rollback"
	txnCounterXaRollbackError       = "#xa.rollback.error"
	txnCounterTxnBegin              = "#txn.begin"
	txnCounterTxnFinish             = "#txn.finish"
	txnCounterTxnAbort              = "#txn.abort"
)

var (
	xaMaxRetryNum = 20
)

type txnState int32

const (
	txnStateLive txnState = iota
	txnStateBeginning
	txnStateExecutingTwoPC
	txnStateExecutingNormal
	txnStateRollbacking
	txnStateCommitting
	txnStateFinshing
	txnStateAborting
	txnStateRecovering
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

// Transaction interface.
type Transaction interface {
	XID() string
	TxID() uint64
	State() int32
	XaState() int32
	Abort() error

	Begin() error
	Rollback() error
	Commit() error
	Finish() error

	SetTimeout(timeout int)
	SetMaxResult(max int)

	Execute(req *xcontext.RequestContext) (*sqltypes.Result, error)
	ExecuteRaw(database string, query string) (*sqltypes.Result, error)
}

// Txn tuple.
type Txn struct {
	log               *xlog.Log
	id                uint64
	xid               string
	mu                sync.Mutex
	mgr               *TxnManager
	req               *xcontext.RequestContext
	txnd              *TxnDetail
	twopc             bool
	start             time.Time
	state             sync2.AtomicInt32
	xaState           sync2.AtomicInt32
	backends          map[string]*Pool
	timeout           int
	maxResult         int
	errors            int
	twopcConnections  map[string]Connection
	normalConnections []Connection
	twopcConnMu       sync.RWMutex
	normalConnMu      sync.RWMutex
}

// NewTxn creates the new Txn.
func NewTxn(log *xlog.Log, txid uint64, mgr *TxnManager, backends map[string]*Pool) (*Txn, error) {
	txn := &Txn{
		log:               log,
		id:                txid,
		mgr:               mgr,
		backends:          backends,
		start:             time.Now(),
		twopcConnections:  make(map[string]Connection),
		normalConnections: make([]Connection, 0, 8),
		state:             sync2.NewAtomicInt32(int32(txnStateLive)),
	}
	txnd := NewTxnDetail(txn)
	txn.txnd = txnd
	tz.Add(txnd)
	txnCounters.Add(txnCounterTxnCreate, 1)
	return txn, nil
}

// SetTimeout used to set the txn timeout.
func (txn *Txn) SetTimeout(timeout int) {
	txn.timeout = timeout
}

// SetMaxResult used to set the txn max result.
func (txn *Txn) SetMaxResult(max int) {
	txn.maxResult = max
}

// TxID returns txn id.
func (txn *Txn) TxID() uint64 {
	return txn.id
}

// XID returns txn xid.
func (txn *Txn) XID() string {
	return txn.xid
}

// State returns txn.state.
func (txn *Txn) State() int32 {
	return txn.state.Get()
}

// XaState returns txn xastate.
func (txn *Txn) XaState() int32 {
	return txn.xaState.Get()
}

func (txn *Txn) incErrors() {
	txn.errors++
}

// twopcConnection used to get a connection via backend name from pool.
// The connection is stored in twopcConnections.
func (txn *Txn) twopcConnection(backend string) (Connection, error) {
	var err error

	txn.twopcConnMu.RLock()
	conn, ok := txn.twopcConnections[backend]
	txn.twopcConnMu.RUnlock()
	if !ok {
		pool, ok := txn.backends[backend]
		if !ok {
			txnCounters.Add(txnCounterTwopcConnectionError, 1)
			return nil, errors.Errorf("txn.can.not.get.twopc.connection.by.backend[%+v].from.pool", backend)
		}
		conn, err = pool.Get()
		if err != nil {
			return nil, err
		}
		txn.twopcConnMu.Lock()
		txn.twopcConnections[backend] = conn
		txn.twopcConnMu.Unlock()
	}
	return conn, nil
}

func (txn *Txn) reFetchTwopcConnection(backend string) (Connection, error) {
	txn.twopcConnMu.Lock()
	conn, ok := txn.twopcConnections[backend]
	if ok {
		delete(txn.twopcConnections, backend)
		conn.Close()
	}
	txn.twopcConnMu.Unlock()
	return txn.twopcConnection(backend)
}

// normalConnection used to get a connection via backend name from pool.
// The Connection is stored in normalConnections for recycling.
func (txn *Txn) normalConnection(backend string) (Connection, error) {
	pool, ok := txn.backends[backend]
	if !ok {
		txnCounters.Add(txnCounterNormalConnectionError, 1)
		return nil, errors.Errorf("txn.can.not.get.normal.connection.by.backend[%+v].from.pool", backend)
	}
	conn, err := pool.Get()
	if err != nil {
		return nil, err
	}
	txn.normalConnMu.Lock()
	txn.normalConnections = append(txn.normalConnections, conn)
	txn.normalConnMu.Unlock()
	return conn, nil
}

func (txn *Txn) fetchOneConnection(back string) (Connection, error) {
	var err error
	var conn Connection
	if txn.twopc {
		if conn, err = txn.twopcConnection(back); err != nil {
			return nil, err
		}
	} else {
		if conn, err = txn.normalConnection(back); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (txn *Txn) xaStart() error {
	txnCounters.Add(txnCounterXaStart, 1)
	txn.xaState.Set(int32(txnXAStateStart))
	defer func() { txn.xaState.Set(int32(txnXAStateStartFinished)) }()

	txn.xid = fmt.Sprintf("RXID-%v-%v", time.Now().Format("20060102150405"), txn.id)
	start := fmt.Sprintf("XA START '%v'", txn.xid)
	if err := txn.executeXACommand(start, txnXAStateStart); err != nil {
		txnCounters.Add(txnCounterXaStartError, 1)
		txn.incErrors()
		return err
	}
	return nil
}

func (txn *Txn) xaEnd() error {
	txnCounters.Add(txnCounterXaEnd, 1)
	txn.xaState.Set(int32(txnXAStateEnd))
	defer func() { txn.xaState.Set(int32(txnXAStateEndFinished)) }()

	end := fmt.Sprintf("XA END '%v'", txn.xid)
	if err := txn.executeXACommand(end, txnXAStateEnd); err != nil {
		txnCounters.Add(txnCounterXaEndError, 1)
		txn.incErrors()
		return err
	}
	return nil
}

func (txn *Txn) xaPrepare() error {
	txnCounters.Add(txnCounterXaPrepare, 1)
	txn.xaState.Set(int32(txnXAStatePrepare))
	defer func() { txn.xaState.Set(int32(txnXAStatePrepareFinished)) }()

	prepare := fmt.Sprintf("XA PREPARE '%v'", txn.xid)
	if err := txn.executeXACommand(prepare, txnXAStatePrepare); err != nil {
		txnCounters.Add(txnCounterXaPrepareError, 1)
		txn.incErrors()
		return err
	}
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
		txn.incErrors()
		txnCounters.Add(txnCounterXaCommitError, 1)

		if err := txn.WriteXaCommitErrLog(txnXACommitErrStateCommit); err != nil {
			log.Error("txn.xa.WriteXaCommitErrLog.query[%v].error[%T]:%+v", commit, err, err)
		}
	}
}

func (txn *Txn) xaRollback() {
	log := txn.log
	txnCounters.Add(txnCounterXaRollback, 1)
	txn.xaState.Set(int32(txnXAStateRollback))
	defer func() { txn.xaState.Set(int32(txnXAStateRollbackFinished)) }()

	rollback := fmt.Sprintf("XA ROLLBACK '%v'", txn.xid)
	if err := txn.executeXACommand(rollback, txnXAStateRollback); err != nil {
		txnCounters.Add(txnCounterXaRollbackError, 1)
		txn.incErrors()

		if err := txn.WriteXaCommitErrLog(txnXACommitErrStateRollback); err != nil {
			log.Error("txn.xa.WriteXaCommitErrLog.query[%v].error[%T]:%+v", rollback, err, err)
		}
	}
}

// Begin used to start a XA transaction.
func (txn *Txn) Begin() error {
	txnCounters.Add(txnCounterTxnBegin, 1)
	txn.twopc = true

	txn.req = xcontext.NewRequestContext()
	txn.req.Mode = xcontext.ReqScatter
	return txn.xaStart()
}

// Commit does:
// 1. XA END
// 2. XA PREPARE
// 3. XA COMMIT
func (txn *Txn) Commit() error {
	txn.state.Set(int32(txnStateCommitting))

	// Here, we only handle the write-txn.
	// Commit nothing for read-txn.
	switch txn.req.TxnMode {
	case xcontext.TxnWrite:
		// 1. XA END.
		if err := txn.xaEnd(); err != nil {
			return err
		}

		// 2. XA PREPARE.
		if err := txn.xaPrepare(); err != nil {
			return err
		}

		// 3. XA COMMIT
		txn.xaCommit()
	}
	return nil
}

// Rollback used to rollback a XA transaction.
// 1. XA ROLLBACK
func (txn *Txn) Rollback() error {
	log := txn.log
	txn.state.Set(int32(txnStateRollbacking))

	// Here, we only handle the write-txn.
	// Rollback nothing for read-txn.
	switch txn.req.TxnMode {
	case xcontext.TxnWrite:
		log.Warning("txn.rollback.xid[%v]", txn.xid)
		// 1. XA END.
		if err := txn.xaEnd(); err != nil {
			return err
		}

		// 2. XA PREPARE.
		if err := txn.xaPrepare(); err != nil {
			return err
		}

		// 3. XA ROLLBACK
		txn.xaRollback()
	}
	return nil
}

// ExecuteRaw used to execute raw query, txn not implemented.
func (txn *Txn) ExecuteRaw(database string, query string) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("txn.ExecuteRaw.not.implemented")
}

// Execute used to execute the query.
// If the txn is in twopc mode, we do the xaStart before the real query execute.
func (txn *Txn) Execute(req *xcontext.RequestContext) (*sqltypes.Result, error) {
	if txn.twopc {
		txn.req = req
		switch req.TxnMode {
		case xcontext.TxnRead:
			// read-txn acquires the commit read-lock.
			txn.mgr.CommitRLock()
			defer txn.mgr.CommitRUnlock()
		}
	}
	qr, err := txn.execute(req)
	if err != nil {
		txn.incErrors()
		return nil, err
	}
	return qr, err
}

// Execute used to execute a query to backends.
func (txn *Txn) execute(req *xcontext.RequestContext) (*sqltypes.Result, error) {
	var err error
	var mu sync.Mutex
	var wg sync.WaitGroup

	log := txn.log
	qr := &sqltypes.Result{}
	allErrors := make([]error, 0, 8)

	if txn.twopc {
		defer queryStats.Record("txn.2pc.execute", time.Now())
		txn.state.Set(int32(txnStateExecutingTwoPC))
	} else {
		defer queryStats.Record("txn.normal.execute", time.Now())
		txn.state.Set(int32(txnStateExecutingNormal))
	}

	// Execute backend-querys.
	oneShard := func(back string, txn *Txn, querys []string) {
		var x error
		var c Connection
		defer wg.Done()

		if c, x = txn.fetchOneConnection(back); x != nil {
			log.Error("txn.fetch.connection.on[%s].querys[%v].error:%+v", back, querys, x)
		} else {
			for _, query := range querys {
				var innerqr *sqltypes.Result

				// Execute to backends.
				if innerqr, x = c.ExecuteWithLimits(query, txn.timeout, txn.maxResult); x != nil {
					log.Error("txn.execute.on[%v].query[%v].error:%+v", c.Address(), query, x)
					break
				}
				mu.Lock()
				qr.AppendResult(innerqr)
				mu.Unlock()
			}
		}

		if x != nil {
			mu.Lock()
			allErrors = append(allErrors, x)
			mu.Unlock()
		}
	}

	switch req.Mode {
	// ReqSingle mode: execute on the first one shard of txn.backends.
	case xcontext.ReqSingle:
		qs := []string{req.RawQuery}
		for back := range txn.backends {
			wg.Add(1)
			oneShard(back, txn, qs)
			break
		}
	// ReqScatter mode: execute on the all shards of txn.backends.
	case xcontext.ReqScatter:
		qs := []string{req.RawQuery}
		beLen := len(txn.backends)
		for back := range txn.backends {
			wg.Add(1)
			if beLen > 1 {
				go oneShard(back, txn, qs)
			} else {
				oneShard(back, txn, qs)
			}
		}
	// ReqNormal mode: execute on the some shards of txn.backends.
	case xcontext.ReqNormal:
		queryMap := make(map[string][]string)
		for _, query := range req.Querys {
			v, ok := queryMap[query.Backend]
			if !ok {
				v = make([]string, 0, 4)
				v = append(v, query.Query)
			} else {
				v = append(v, query.Query)
			}
			queryMap[query.Backend] = v
		}
		beLen := len(queryMap)
		for back, qs := range queryMap {
			wg.Add(1)
			if beLen > 1 {
				go oneShard(back, txn, qs)
			} else {
				oneShard(back, txn, qs)
			}
		}
	}

	wg.Wait()
	if len(allErrors) > 0 {
		err = allErrors[0]
	}
	return qr, err
}

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

	// Only do XA when backends numbers larger than one.
	beLen := len(txn.backends)
	if beLen > 1 {
		switch state {
		case txnXAStateCommit, txnXAStateRollback:
			// Acquire the commit lock if the txn is write.
			txn.mgr.CommitLock()
			defer txn.mgr.CommitUnlock()
		}

		// Scatter.
		for back := range txn.backends {
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

// ExecuteStreamFetch used to execute stream fetch query.
func (txn *Txn) ExecuteStreamFetch(req *xcontext.RequestContext, callback func(*sqltypes.Result) error, streamBufferSize int) error {
	var err error
	var mu sync.Mutex
	var wg sync.WaitGroup

	log := txn.log
	cursors := make([]driver.Rows, 0, 8)
	allErrors := make([]error, 0, 8)

	defer func() {
		for _, cursor := range cursors {
			cursor.Close()
		}
	}()

	oneShard := func(c Connection, query string) {
		defer wg.Done()
		cursor, x := c.ExecuteStreamFetch(query)
		if x != nil {
			mu.Lock()
			allErrors = append(allErrors, x)
			mu.Unlock()
			return
		}
		mu.Lock()
		cursors = append(cursors, cursor)
		mu.Unlock()
	}

	for _, qt := range req.Querys {
		var conn Connection
		if conn, err = txn.fetchOneConnection(qt.Backend); err != nil {
			return err
		}
		wg.Add(1)
		go oneShard(conn, qt.Query)
	}
	wg.Wait()
	if len(allErrors) > 0 {
		return allErrors[0]
	}

	// Send Fields.
	fields := cursors[0].Fields()
	fieldsQr := &sqltypes.Result{Fields: fields, State: sqltypes.RStateFields}
	if err := callback(fieldsQr); err != nil {
		return err
	}

	// Send rows.
	var allByteCount, allBatchCount, allRowCount uint64

	byteCount := 0
	cursorFinished := 0
	bitmap := make([]bool, len(cursors))
	qr := &sqltypes.Result{Fields: fields, Rows: make([][]sqltypes.Value, 0, 256), State: sqltypes.RStateRows}
	for cursorFinished < len(cursors) {
		for i, cursor := range cursors {
			fetchPerLoop := 64
			name := req.Querys[i].Backend
			for fetchPerLoop > 0 {
				if cursor.Next() {
					allRowCount++
					row, err := cursor.RowValues()
					if err != nil {
						log.Error("txn.stream.cursor[%s].RowValues.error:%+v", name, err)
						return err
					}
					rowLen := sqltypes.Values(row).Len()
					byteCount += rowLen
					allByteCount += uint64(rowLen)
					qr.Rows = append(qr.Rows, row)

				} else {
					if !bitmap[i] {
						if x := cursor.LastError(); x != nil {
							log.Error("txn.stream.cursor[%s].last.error:%+v", name, x)
							return x
						}
						bitmap[i] = true
						cursorFinished++
					}
				}
				fetchPerLoop--
			}
		}

		if byteCount >= streamBufferSize {
			if x := callback(qr); x != nil {
				log.Error("txn.stream.cursor.send1.error:%+v", x)
				return x
			}
			qr.Rows = qr.Rows[:0]
			byteCount = 0
			allBatchCount++

			log.Warning("txn.steam.send.[streamBufferSize:%v, hasSentRows:%v, hasSentBytes:%v, hasSentBatchs:%v, cursorFinished:%d/%d]",
				streamBufferSize, allRowCount, allByteCount, allBatchCount, cursorFinished, len(cursors))
		}
	}
	if len(qr.Rows) > 0 {
		if x := callback(qr); x != nil {
			log.Error("txn.stream.cursor.send2.error:%+v", x)
			return x
		}
	}
	log.Warning("txn.stream.send.done[allRows:%v, allBytes:%v, allBatches:%v]", allRowCount, allByteCount, allBatchCount)

	// Send finished.
	finishQr := &sqltypes.Result{Fields: fields, RowsAffected: allRowCount, State: sqltypes.RStateFinished}
	return callback(finishQr)
}

// ExecuteScatter used to execute query on all shards.
func (txn *Txn) ExecuteScatter(query string) (*sqltypes.Result, error) {
	rctx := &xcontext.RequestContext{
		RawQuery: query,
		Mode:     xcontext.ReqScatter,
	}
	return txn.Execute(rctx)
}

// ExecuteSingle used to execute query on one shard.
func (txn *Txn) ExecuteSingle(query string) (*sqltypes.Result, error) {
	rctx := &xcontext.RequestContext{
		RawQuery: query,
		Mode:     xcontext.ReqSingle,
	}
	return txn.Execute(rctx)
}

// ExecuteOnThisBackend used to send the query to this backend.
func (txn *Txn) ExecuteOnThisBackend(backend string, query string) (*sqltypes.Result, error) {
	qt := xcontext.QueryTuple{
		Query:   query,
		Backend: backend,
	}
	rctx := &xcontext.RequestContext{
		Querys: []xcontext.QueryTuple{qt},
	}
	return txn.Execute(rctx)
}

// Finish used to finish a transaction.
// If the lastErr is nil, we will recycle all the twopc connections to the pool for reuse,
// otherwise we wil close all of the them.
func (txn *Txn) Finish() error {
	txnCounters.Add(txnCounterTxnFinish, 1)

	txn.mu.Lock()
	defer txn.mu.Unlock()

	defer tz.Remove(txn.txnd)
	defer func() { txn.twopc = false }()

	// If the txn has aborted, we won't do finish.
	if txn.state.Get() == int32(txnStateAborting) {
		return nil
	}

	txn.xaState.Set(int32(txnXAStateNone))
	txn.state.Set(int32(txnStateFinshing))

	// 2pc connections.
	for id, conn := range txn.twopcConnections {
		if txn.errors > 0 {
			conn.Close()
		} else {
			conn.Recycle()
		}
		delete(txn.twopcConnections, id)
	}

	// normal connections.
	for _, conn := range txn.normalConnections {
		if txn.errors > 0 {
			conn.Close()
		} else {
			conn.Recycle()
		}
	}
	txn.mgr.Remove()
	return nil
}

// Abort used to abort all txn connections.
func (txn *Txn) Abort() error {
	txnCounters.Add(txnCounterTxnAbort, 1)

	txn.mu.Lock()
	defer txn.mu.Unlock()

	defer tz.Remove(txn.txnd)
	defer func() { txn.twopc = false }()

	// If the txn has finished, we won't do abort.
	if txn.state.Get() == int32(txnStateFinshing) {
		return nil
	}
	txn.state.Set(int32(txnStateAborting))

	// 2pc connections.
	for id, conn := range txn.twopcConnections {
		conn.Kill("txn.abort")
		txn.twopcConnMu.Lock()
		delete(txn.twopcConnections, id)
		txn.twopcConnMu.Unlock()
	}

	// normal connections.
	txn.normalConnMu.RLock()
	for _, conn := range txn.normalConnections {
		conn.Kill("txn.abort")
	}
	txn.normalConnMu.RUnlock()
	txn.mgr.Remove()
	return nil
}

// WriteXaCommitErrLog used to write the error xaid to the log.
func (txn *Txn) WriteXaCommitErrLog(state string) error {
	return txn.mgr.xaCheck.WriteXaCommitErrLog(txn, state)
}
