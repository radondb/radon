/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 * This code was derived from https://github.com/youtube/vitess.
 */

package backend

import (
	"sort"
	"sync"
	"time"
)

// TxnDetail is a simple wrapper for Query
type TxnDetail struct {
	txnID uint64
	txn   Transaction
	start time.Time
}

// NewTxnDetail creates a new TxnDetail
func NewTxnDetail(txn Transaction) *TxnDetail {
	return &TxnDetail{txnID: txn.TxID(), txn: txn, start: time.Now()}
}

// Txnz holds a thread safe list of TxnDetails
type Txnz struct {
	mu         sync.RWMutex
	txnDetails map[uint64]*TxnDetail
}

// NewTxnz creates a new Txnz
func NewTxnz() *Txnz {
	return &Txnz{txnDetails: make(map[uint64]*TxnDetail)}
}

// Add adds a TxnDetail to Txnz
func (tz *Txnz) Add(td *TxnDetail) {
	tz.mu.Lock()
	defer tz.mu.Unlock()
	tz.txnDetails[td.txnID] = td
}

// Remove removes a TxnDetail from Txnz
func (tz *Txnz) Remove(td *TxnDetail) {
	tz.mu.Lock()
	defer tz.mu.Unlock()
	delete(tz.txnDetails, td.txnID)
}

// TxnDetailzRow is used for rendering TxnDetail in a template
type TxnDetailzRow struct {
	Start    time.Time
	Duration time.Duration
	TxnID    uint64
	XAID     string
	Query    string
	State    string
	XaState  string
	Color    string
}

type byTxStartTime []TxnDetailzRow

func (a byTxStartTime) Len() int           { return len(a) }
func (a byTxStartTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTxStartTime) Less(i, j int) bool { return a[i].Start.Before(a[j].Start) }

var (
	txnStates = map[int32]string{
		int32(txnStateLive):            "txnStateLive",
		int32(txnStateBeginning):       "txnStateBeginning",
		int32(txnStateExecutingTwoPC):  "txnStateExecutingTwoPC",
		int32(txnStateExecutingNormal): "txnStateExecutingNormal",
		int32(txnStateRollbacking):     "txnStateRollbacking",
		int32(txnStateCommitting):      "txnStateCommitting",
		int32(txnStateFinshing):        "txnStateFinshing",
		int32(txnStateAborting):        "txnStateAborting",
	}

	xaStates = map[int32]string{
		int32(txnXAStateNone):            "txnXAStateNone",
		int32(txnXAStateStart):           "txnXAStateStart",
		int32(txnXAStateStartFinished):   "txnXAStateStartFinished",
		int32(txnXAStateEnd):             "txnXAStateEnd",
		int32(txnXAStateEndFinished):     "txnXAStateEndFinished",
		int32(txnXAStatePrepare):         "txnXAStatePrepare",
		int32(txnXAStatePrepareFinished): "txnXAStatePrepareFinished",
		int32(txnXAStateCommit):          "txnXAStateCommit",
		int32(txnXAStateRollback):        "txnXAStateRollback",
	}
)

// GetTxnzRows returns a list of TxnDetailzRow sorted by start time
func (tz *Txnz) GetTxnzRows() []TxnDetailzRow {
	tz.mu.RLock()
	rows := []TxnDetailzRow{}
	for _, td := range tz.txnDetails {
		state := "UNKNOW"
		if s, ok := txnStates[td.txn.State()]; ok {
			state = s
		}
		xaState := "NONE"
		if s, ok := xaStates[td.txn.XaState()]; ok {
			xaState = s
		}

		row := TxnDetailzRow{
			Start:    td.start,
			Duration: time.Since(td.start),
			TxnID:    td.txnID,
			XAID:     td.txn.XID(),
			State:    state,
			XaState:  xaState,
		}
		if row.Duration < 10*time.Millisecond {
			row.Color = "low"
		} else if row.Duration < 100*time.Millisecond {
			row.Color = "medium"
		} else {
			row.Color = "high"
		}
		rows = append(rows, row)
	}
	tz.mu.RUnlock()
	sort.Sort(byTxStartTime(rows))
	return rows
}
