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
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	txnCounterTxnCreate             = "#txn.create"
	txnCounterTwopcConnectionError  = "#get.twopc.connection.error"
	txnCounterNormalConnectionError = "#get.normal.connection.error"
	txnCounterTxnBegin              = "#txn.begin"
	txnCounterTxnFinish             = "#txn.finish"
	txnCounterTxnAbort              = "#txn.abort"
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

	BeginScatter() error
	CommitScatter() error
	RollbackScatter() error
	SetMultiStmtTxn()

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
	isMultiStmtTxn    bool
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

// Begin used to start a XA transaction.
// Begin only does:
// 1. set twopc to true
func (txn *Txn) Begin() error {
	txnCounters.Add(txnCounterTxnBegin, 1)
	txn.twopc = true
	return nil
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

// BeginScatter used to start a XA transaction in the multiple-statement transaction
func (txn *Txn) BeginScatter() error {
	txnCounters.Add(txnCounterTxnBegin, 1)
	txn.twopc = true

	txn.req = xcontext.NewRequestContext()
	txn.req.Mode = xcontext.ReqScatter
	return txn.xaStart()
}

// CommitScatter is used in the multiple-statement transaction
func (txn *Txn) CommitScatter() error {
	txn.state.Set(int32(txnStateCommitting))
	txn.twopc = true
	txn.req = xcontext.NewRequestContext()
	txn.req.Mode = xcontext.ReqScatter

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
	return nil
}

// RollbackScatter is used in the multiple-statement transaction
func (txn *Txn) RollbackScatter() error {
	log := txn.log
	txn.state.Set(int32(txnStateRollbacking))
	txn.twopc = true
	txn.req = xcontext.NewRequestContext()
	txn.req.Mode = xcontext.ReqScatter

	log.Warning("txn.rollback.scatter.xid[%v]", txn.xid)
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
	return nil
}

// SetMultiStmtTxn ...
func (txn *Txn) SetMultiStmtTxn() {
	txn.isMultiStmtTxn = true
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
		case xcontext.TxnWrite:
			// write-txn xa starts to the single statement.
			if !txn.isMultiStmtTxn {
				if err := txn.xaStart(); err != nil {
					return nil, err
				}
			}
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
	cursorFinished := 0
	rows := make(chan []sqltypes.Value, 65536)
	stop := make(chan bool)
	oneFetch := func(name string, cursor driver.Rows) {
		defer wg.Done()
		for {
			if cursor.Next() {
				row, err := cursor.RowValues()
				if err != nil {
					log.Error("txn.stream.cursor[%s].RowValues.error:%+v", name, err)
					mu.Lock()
					allErrors = append(allErrors, err)
					cursorFinished++
					if cursorFinished == len(cursors) {
						close(rows)
					}
					mu.Unlock()
					return
				}
				select {
				case <-stop:
					return
				case rows <- row:
				}
			} else {
				mu.Lock()
				cursorFinished++
				if cursorFinished == len(cursors) {
					close(rows)
				}
				mu.Unlock()
				return
			}
		}
	}

	// producer.
	for i, cursor := range cursors {
		name := req.Querys[i].Backend
		wg.Add(1)
		go oneFetch(name, cursor)
	}
	// consumer.
	var allRowCount uint64
	wg.Add(1)
	go func() {
		var allByteCount, allBatchCount uint64
		byteCount := 0
		qr := &sqltypes.Result{Fields: fields, Rows: make([][]sqltypes.Value, 0, 256), State: sqltypes.RStateRows}
		defer func() {
			close(stop)
			wg.Done()
		}()
		for {
			if row, ok := <-rows; ok {
				rowLen := sqltypes.Values(row).Len()
				allRowCount++
				byteCount += rowLen
				allByteCount += uint64(rowLen)
				qr.Rows = append(qr.Rows, row)

				if byteCount >= streamBufferSize {
					if x := callback(qr); x != nil {
						log.Error("txn.stream.cursor.send1.error:%+v", x)
						mu.Lock()
						allErrors = append(allErrors, x)
						mu.Unlock()
						return
					}
					qr.Rows = qr.Rows[:0]
					allBatchCount++
					byteCount = 0
				}
			} else {
				if len(qr.Rows) > 0 {
					if x := callback(qr); x != nil {
						log.Error("txn.stream.cursor.send2.error:%+v", x)
						mu.Lock()
						allErrors = append(allErrors, x)
						mu.Unlock()
						return
					}
				}
				log.Warning("txn.stream.send.done[allRows:%v, allBytes:%v, allBatches:%v]", allRowCount, allByteCount, allBatchCount)
				return
			}
		}
	}()
	wg.Wait()
	if len(allErrors) > 0 {
		return allErrors[0]
	}

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
	defer func() {
		txn.twopc = false
		txn.isMultiStmtTxn = false
	}()

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
	defer func() {
		txn.twopc = false
		txn.isMultiStmtTxn = false
	}()

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
