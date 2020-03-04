/*
 * Radon
 *
 * Copyright 2018-2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shiftmanager

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/radondb/shift/build"
	"github.com/radondb/shift/shift"
	sxlog "github.com/radondb/shift/xlog"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const maxShiftInstancesAlived = 10

// ShiftManager used to manager the infos about the shift
// Called by reshard/rebalance
type ShiftManager struct {
	log *xlog.Log
	mu  sync.Mutex
	wg  sync.WaitGroup

	// Limit max 10 shift instances
	// key: for reshard, key is `db`.`table`, for rebalance, key is `db`.`table`_backend
	instancesAlived map[string]*shiftInstancesAlived
	// Store shift infos no matter success or failed
	// key: for reshard, key is `db`.`table`, for rebalance, key is `db`.`table`_backend
	instancesFinished map[string]*shiftInstancesFinished
}

// shiftInstancesAlived used to store alived shift instances.
type shiftInstancesAlived struct {
	shift     shift.ShiftHandler
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

// NewShiftInstance -- used to new a shift instance
func (shiftMgr *ShiftManager) NewShiftInstance(shiftInfo *ShiftInfo, typ ShiftType) (shift.ShiftHandler, error) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	build := build.GetInfo()
	shiftMgr.log.Warning("shift:[%+v]\n", build)

	shiftMgr.log.Warning(`
           IMPORTANT: Please check that the shift run completes successfully.
           At the end of a successful shift run prints "shift.completed.OK!".`)

	cfg := &shift.Config{
		From:                   shiftInfo.From,
		FromUser:               shiftInfo.FromUser,
		FromPassword:           shiftInfo.FromPassword,
		FromDatabase:           shiftInfo.FromDatabase,
		FromTable:              shiftInfo.FromTable,
		To:                     shiftInfo.To,
		ToUser:                 shiftInfo.ToUser,
		ToPassword:             shiftInfo.ToPassword,
		ToDatabase:             shiftInfo.ToDatabase,
		ToTable:                shiftInfo.ToTable,
		Rebalance:              shiftInfo.Rebalance,
		Cleanup:                shiftInfo.Cleanup,
		MySQLDump:              shiftInfo.MysqlDump,
		Threads:                shiftInfo.Threads,
		Behinds:                shiftInfo.PosBehinds,
		RadonURL:               shiftInfo.RadonURL,
		Checksum:               shiftInfo.Checksum,
		WaitTimeBeforeChecksum: shiftInfo.WaitTimeBeforeChecksum,
	}

	switch typ {
	case ShiftTypeReshard:
		cfg.ToFlavor = shift.ToRadonDBFlavor
	case ShiftTypeRebalance:
		cfg.ToFlavor = shift.ToMySQLFlavor
	default:
		shiftMgr.log.Error(`shift.wrong.shifttype: [%+v].`, typ)
		return nil, fmt.Errorf("shift.wrong.shifttype: [%+v].", typ)
	}

	shiftMgr.log.Info("shift.cfg:%+v", cfg)

	slog := sxlog.NewStdLog(sxlog.Level(sxlog.WARNING))
	return shift.NewShift(slog, cfg), nil
}

// StartShiftInstance -- used to start a new shift instance
func (shiftMgr *ShiftManager) StartShiftInstance(key string, shift shift.ShiftHandler, typ ShiftType) error {
	shiftMgr.mu.Lock()
	defer shiftMgr.mu.Unlock()
	// check if the instance specified by key has been already in instancesAlived
	if _, ok := shiftMgr.instancesAlived[key]; ok {
		return fmt.Errorf("shift.instance[%v].is.already.running", key)
	}

	if typ == ShiftTypeNone {
		return fmt.Errorf("shift.instance.type.should.not.be.none")
	}
	if err := shift.Start(); err != nil {
		shiftMgr.log.Error("shift.instance.start.error:%+v", err)
		return err
	}

	if err := shiftMgr.addAnAlivedInstance(key, typ, shift); err != nil {
		shiftMgr.log.Error("shiftMgr.add.instance.error:%+v", err)
		return err
	}
	return nil
}

// addAnInstance -- used to add an shift instance to shift manager
// key: for reshard, key is `db`.`table`, for rebalance, key is `db`.`table`_backend
func (shiftMgr *ShiftManager) addAnAlivedInstance(key string, typ ShiftType, shift shift.ShiftHandler) error {
	if len(shiftMgr.instancesAlived) < maxShiftInstancesAlived {
		shiftMgr.instancesAlived[key] = &shiftInstancesAlived{
			shift:     shift,
			status:    ShiftStatusMigrating,
			progress:  "",
			shiftType: typ,
		}
		return nil
	}
	return fmt.Errorf("shift.instances.num.exceeding.10.limits")
}

func (shiftMgr *ShiftManager) updateFinishedInstance(key string, status ShiftStatus, typ ShiftType) {
	shiftMgr.mu.Lock()
	defer shiftMgr.mu.Unlock()
	finished := &shiftInstancesFinished{
		status:    status,
		progress:  "",
		shiftType: typ,
	}
	// the finished instance added into instancesFinished
	shiftMgr.instancesFinished[key] = finished
	// the finished instance in instancesAlived should be removed
	delete(shiftMgr.instancesAlived, key)
}

// WaitInstanceFinishThread -- used to start a thread to excute wait finish in background.
func (shiftMgr *ShiftManager) WaitInstanceFinishThread(key string) error {
	shiftMgr.mu.Lock()
	defer shiftMgr.mu.Unlock()
	// get instance specified by key
	instance, ok := shiftMgr.instancesAlived[key]
	if !ok {
		shiftMgr.log.Error("shift.manager.wait.thread.start.error:instance[%v].not.found", key)
		return fmt.Errorf("shift.manager.wait.thread.start.error:instance[%v].not.found", key)
	}

	shiftMgr.wg.Add(1)
	go func(shiftMgr *ShiftManager, instance *shiftInstancesAlived) {
		defer shiftMgr.wg.Done()
		err := instance.shift.WaitFinish()
		if err != nil {
			shiftMgr.log.Error("shift.manager.shift.instance[%v].wait.thread.finish.error:%+v", key, err)
			shiftMgr.updateFinishedInstance(key, ShiftStatusFail, instance.shiftType)
		} else {
			shiftMgr.log.Info("shift.manager.shift.instance[%v].wait.thread.finish.success.", key)
			shiftMgr.updateFinishedInstance(key, ShiftStatusSuccess, instance.shiftType)
		}
	}(shiftMgr, instance)
	return nil
}

// WaitInstanceFinish -- used to wait instance run until finished.
func (shiftMgr *ShiftManager) WaitInstanceFinish(key string) error {
	shiftMgr.mu.Lock()
	// get instance specified by key
	instance, ok := shiftMgr.instancesAlived[key]
	if !ok {
		shiftMgr.log.Error("shift.manager.wait.thread.start.error:instance[%v].not.found", key)
		return fmt.Errorf("shift.manager.wait.thread.start.error:instance[%v].not.found", key)
	}
	// release lock, let instance do finish work alone
	shiftMgr.mu.Unlock()

	err := instance.shift.WaitFinish()
	if err != nil {
		shiftMgr.log.Error("shift.manager.shift.instance[%v].wait.thread.finish.error:%+v", key, err)
		shiftMgr.updateFinishedInstance(key, ShiftStatusFail, instance.shiftType)
	} else {
		shiftMgr.log.Info("shift.manager.shift.instance[%v].wait.thread.finish.success.", key)
		shiftMgr.updateFinishedInstance(key, ShiftStatusSuccess, instance.shiftType)
	}
	return err
}

// getInstancesAlivedNums() used to count instancesAlived nums
func (shiftMgr *ShiftManager) getInstancesAlivedNums() int {
	shiftMgr.mu.Lock()
	defer shiftMgr.mu.Unlock()
	return len(shiftMgr.instancesAlived)
}

// getInstancesFinishedNums() used to count instancesAlived nums
func (shiftMgr *ShiftManager) getInstancesFinishedNums() int {
	shiftMgr.mu.Lock()
	defer shiftMgr.mu.Unlock()
	return len(shiftMgr.instancesFinished)
}

// GetStatus -- used to get shift status specified by key
// key: for reshard, key is `db`.`table`, for rebalance, key is `db`.`table`_backend
func (shiftMgr *ShiftManager) GetStatus(key string) ShiftStatus {
	if v, ok := shiftMgr.instancesAlived[key]; ok {
		return v.status
	}
	if v, ok := shiftMgr.instancesFinished[key]; ok {
		return v.status
	}
	shiftMgr.log.Error("shift.manager.get.status.error:instance[%v].not.found", key)
	return ShiftStatusNone
}

// GetProgress -- used to get shift progress specified by key
// key: for reshard, key is `db`.`table`, for rebalance, key is `db`.`table`_backend
func (shiftMgr *ShiftManager) GetProgress(key string) *sqltypes.Result {
	return nil
}

// GetShiftType -- used to get shift type specified by key
func (shiftMgr *ShiftManager) GetShiftType(key string) ShiftType {
	if v, ok := shiftMgr.instancesAlived[key]; ok {
		return v.shiftType
	}
	if v, ok := shiftMgr.instancesFinished[key]; ok {
		return v.shiftType
	}
	shiftMgr.log.Error("shift.manager.get.shift.type.error:instance[%v].not.found", key)
	return ShiftTypeNone
}

// StopOneInstance used to stop one shift instance specified by key
// key: for reshard, key is `db`.`table`, for rebalance, key is `db`.`table`_backend
func (shiftMgr *ShiftManager) StopOneInstance(key string) error {
	instance, ok := shiftMgr.instancesAlived[key]
	if ok {
		instance.shift.SetStopSignal()
		return nil
	}

	shiftMgr.log.Error("shift.manager.instance[%v].not.found", key)
	return fmt.Errorf("shift.manager.instance[%v].not.found", key)
}

// StopAllInstance used to stop all shift instances
// When call Close(), WaitInstanceFinishThread will get err and exit goroutine
func (shiftMgr *ShiftManager) StopAllInstance() error {
	for _, instance := range shiftMgr.instancesAlived {
		instance.shift.SetStopSignal()
	}
	return nil
}

// Close -- used to close all the shift instances that are on working
// When call Close(), WaitInstanceFinishThread will get err and exit goroutine
func (shiftMgr *ShiftManager) Close() error {
	err := shiftMgr.StopAllInstance()
	if err != nil {
		return err
	}
	// wait all instances running in background to finish wait
	shiftMgr.wg.Wait()
	return nil
}
