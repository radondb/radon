/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"backend"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	shiftUnfinished = 0
	shiftFinished   = 1
)

// Reshard ...
type Reshard struct {
	mu              sync.RWMutex
	wg              sync.WaitGroup
	log             *xlog.Log
	scatter         *backend.Scatter
	router          *router.Router
	spanner         *Spanner
	user            string
	db              string
	singleTable     string
	dstDB           string
	reshardTable    string
	tmpReshardTable string
	ticker          *time.Ticker
	handle          ReshardHandle
	shiftProcessBar int
	shiftStatus     error
}

var _ ReshardHandle = &Reshard{}

// ReshardHandle ...
type ReshardHandle interface {
	ShiftProcess() error
}

// ShiftProcess is call the shift tool cmd.
func (reshard *Reshard) ShiftProcess() error {
	return shiftTableLow(reshard.db, reshard.singleTable, reshard.dstDB, reshard.reshardTable, reshard.user, reshard.spanner)
}

// ShiftProcessBar about status of the Shift Process Bar.
func (reshard *Reshard) ShiftProcessBar() int {
	reshard.mu.RLock()
	defer reshard.mu.RUnlock()
	return reshard.shiftProcessBar
}

// SetShiftProcessBar set the Shift Process Bar.
func (reshard *Reshard) SetShiftProcessBar(finished int) {
	reshard.mu.Lock()
	defer reshard.mu.Unlock()
	reshard.shiftProcessBar = finished
}

// ShiftStatus about shift status.
func (reshard *Reshard) ShiftStatus() error {
	reshard.mu.RLock()
	defer reshard.mu.RUnlock()
	return reshard.shiftStatus
}

// SetShiftStatus set the shift status.
func (reshard *Reshard) SetShiftStatus(err error) {
	reshard.mu.Lock()
	defer reshard.mu.Unlock()
	reshard.shiftStatus = err
}

// NewReshard ...
func NewReshard(log *xlog.Log, scatter *backend.Scatter, router *router.Router,
	spanner *Spanner, user string) *Reshard {
	return &Reshard{
		log:     log,
		scatter: scatter,
		router:  router,
		spanner: spanner,
		ticker:  time.NewTicker(time.Duration(time.Second * 5)),
		user:    user,
	}
}

// SetHandle set the handle
func (reshard *Reshard) SetHandle(r ReshardHandle) {
	reshard.handle = r
}

// CheckReshardDBTable check the database and table.
func (reshard *Reshard) CheckReshardDBTable(db, singleTable, dstDB, dstTable string) (bool, error) {
	isSingle, err := reshard.IsSingleTable(db, singleTable)
	if err != nil {
		err := fmt.Errorf("reshard.check.[%s].is.singleTable.err.%v", singleTable, err)
		return false, err
	}

	if isSingle != true {
		err := fmt.Errorf("reshard.check.[%s].is.not.singleTable", singleTable)
		return false, err
	}

	err = reshard.router.CheckDatabase(dstDB)
	if err != nil {
		err := fmt.Errorf("reshard.check.[%s].is.not.exist", dstDB)
		return false, err
	}

	// make sure the dstTable is not exist to the shift.
	isExist, err := reshard.router.CheckTable(dstDB, dstTable)
	if err == nil && isExist == false {
		return true, nil
	}

	if err == nil {
		err = fmt.Errorf("reshard.check.[%s].is.exist", dstTable)
	}
	return false, err
}

// IsSingleTable check the table is Single or not.
func (reshard *Reshard) IsSingleTable(db, singleTable string) (bool, error) {
	table, err := reshard.router.TableConfig(db, singleTable)
	if err != nil {
		return false, err
	}

	if table.ShardType == "SINGLE" {
		return true, nil
	}
	return false, nil
}

// ReShardTable just reshard single table to the sharding table now.
func (reshard *Reshard) ReShardTable(db, singleTable, dstDB, dstTable string) (*sqltypes.Result, error) {
	log := reshard.log
	qr := &sqltypes.Result{}

	if ok, err := reshard.CheckReshardDBTable(db, singleTable, dstDB, dstTable); ok != true {
		log.Error("reshard.check[%s.%s->%s.%s].is.not.ok:%v.", db, singleTable, dstDB, dstTable, err)
		err := fmt.Sprintf("reshard.check[%s.%s->%s.%s].is.not.ok:%v.", db, singleTable, dstDB, dstTable, err)
		return qr, errors.New(err)
	}
	reshard.db = db
	reshard.singleTable = singleTable
	reshard.dstDB = dstDB
	reshard.reshardTable = dstTable

	// start the shift process.
	reshard.shiftTable(reshard.user)
	return qr, nil
}

// The call is returned immediately, won't call wg.Wait()
// 1. the shift status will be filled by rc when finished
// 2. the shift progress bar will call other interface.
func (reshard *Reshard) shiftTable(user string) error {
	var wg sync.WaitGroup

	oneshift := func(db, srcTable, dstDB, dstTable string, user string, spanner *Spanner) {
		defer wg.Done()

		err := reshard.handle.ShiftProcess()
		reshard.SetShiftProcessBar(shiftFinished)
		if err != nil {
			reshard.SetShiftStatus(err)
			return
		}

		reshard.SetShiftStatus(nil)
	}

	wg.Add(1)
	go oneshift(reshard.db, reshard.singleTable, reshard.dstDB, reshard.reshardTable, user, reshard.spanner)
	return nil
}
