/*
 * Radon
 *
 * Copyright 2018-2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shiftmanager

import (
	"sync"

	"github.com/radondb/shift/shift"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// maxShiftInstancesAlived used represents the max alive shift instance nums.
const maxShiftInstancesAlived = 10

// ShiftManager used to manager the infos about the shift
// Called by reshard/rebalance
type ShiftManager struct {
	log *xlog.Log
	mu  sync.Mutex
	// Limit max 10 shift instances
	// key: for reshard, key is db_table, for rebalance, key is db_table_backend
	instancesAlived map[string]*shiftInstancesAlived
	// Store shift infos no matter success or failed
	// key: for reshard, key is db_table, for rebalance, key is db_table_backend
	instancesFinished map[string]*shiftInstancesFinished
}

// shiftInstancesAlived used to store alived shift instances.
type shiftInstancesAlived struct {
	shift     *shift.Shift
	status    ShiftStatus
	progress  string
	shiftType ShiftType
}

// shiftInstancesFinished used to store the finished shift instances no matter success or failed.
type shiftInstancesFinished struct {
	status    ShiftStatus
	progress  string
	shiftType ShiftType
}

// NewShiftManager -- used to create a new shift manager.
func NewShiftManager(log *xlog.Log) ShiftMgrHandler {
	return &ShiftManager{
		log: log,
	}
}

// Init -- used to init the plug module.
func (shiftMgr *ShiftManager) Init() error {
	shiftMgr.instancesAlived = make(map[string]*shiftInstancesAlived)
	shiftMgr.instancesFinished = make(map[string]*shiftInstancesFinished)
	return nil
}

// StartAnInstance -- used to start a new shift instance.
func (shiftMgr *ShiftManager) StartAnInstance(shiftInfo ShiftInfo, typ ShiftType) error {
	return nil
}

// WaitInstanceFinishThread -- used to start a thread to wait a shift instance to finish.
func (shiftMgr *ShiftManager) WaitInstanceFinishThread(key string) error {
	return nil
}

// addAnInstance -- used to add an shift instance to shift manager.
// key: for reshard, key is db_table, for rebalance, key is db_table_backend.
func (shiftMgr *ShiftManager) addAnInstance(key string, shift *shift.Shift) error {
	return nil
}

// GetStatus -- used to get shift status specified by key.
// key: for reshard, key is db_table, for rebalance, key is db_table_backend.
func (shiftMgr *ShiftManager) GetStatus(key string) ShiftStatus {
	return ShiftStatusNone
}

// GetProgress -- used to get shift progress specified by key.
// key: for reshard, key is db_table, for rebalance, key is db_table_backend.
func (shiftMgr *ShiftManager) GetProgress(key string) *sqltypes.Result {
	return nil
}

// StopOneInstance used to stop one shift instance specified by key.
// key: for reshard, key is db_table, for rebalance, key is db_table_backend.
func (shiftMgr *ShiftManager) StopOneInstance(key string) error {
	return nil
}

// StopAllInstance used to stop all shift instances.
func (shiftMgr *ShiftManager) StopAllInstance() error {
	return nil
}

// Close -- used to close all the shift instances that are on working.
func (shiftMgr *ShiftManager) Close() error {
	return nil
}
