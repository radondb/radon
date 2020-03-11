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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestNewShiftInstance(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shift, err := shiftMgr.NewShiftInstance(MockShiftInfo, ShiftTypeReshard)
	assert.NotNil(t, shift)
	assert.Nil(t, err)

	shift, err = shiftMgr.NewShiftInstance(MockShiftInfo, ShiftTypeRebalance)
	assert.NotNil(t, shift)
	assert.Nil(t, err)

	shift, err = shiftMgr.NewShiftInstance(MockShiftInfo, ShiftTypeNone)
	assert.Nil(t, shift)
	assert.NotNil(t, err)
}

func startInstanceOK(t *testing.T, log *xlog.Log, shiftMgr ShiftMgrHandler, key string, typ ShiftType) {
	mockshift := NewMockShift(log)
	err := shiftMgr.StartShiftInstance(key, mockshift, typ)
	assert.Nil(t, err)
	assert.Equal(t, ShiftStatusMigrating, shiftMgr.GetStatus(key))
	assert.Equal(t, typ, shiftMgr.GetShiftType(key))
}

func startInstanceErr(t *testing.T, log *xlog.Log, shiftMgr ShiftMgrHandler, key string, typ ShiftType) {
	mockshift := NewMockShift(log)
	err := shiftMgr.StartShiftInstance(key, mockshift, typ)
	log.Error("start.instance.get.err:%v", err)
	assert.NotNil(t, err)
}

func TestStartShiftInstance(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()
	// test ShiftTypeReshard
	keyOK := "db_tbl_1"
	startInstanceOK(t, log, shiftMgr, keyOK, ShiftTypeReshard)

	// test ShiftTypeRebalance
	keyOK = "db_tbl_2"
	startInstanceOK(t, log, shiftMgr, keyOK, ShiftTypeRebalance)

	// Now shiftMgr has two alived instances
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
}

func TestStartShiftInstanceErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()
	keyOK := "db_tbl"

	// register an instance
	startInstanceOK(t, log, shiftMgr, keyOK, ShiftTypeReshard)
	// Now shiftMgr has 1 alived instance
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())

	// test shift start err with duplicate key
	startInstanceErr(t, log, shiftMgr, keyOK, ShiftTypeRebalance)

	// test shift start err with a new key but shift type is ShiftTypeNone
	keyOK = "db_xx"
	startInstanceErr(t, log, shiftMgr, keyOK, ShiftTypeNone)

	// Now shiftMgr still has 1 alived instances
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
}

func TestStartShiftInstanceErrWith10Limits(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Nanosecond)
			keyReshard := fmt.Sprintf("db_reshard_%d", i)
			startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
		}
	}()
	wg.Add(1)
	go func() {
		wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Nanosecond)
			keyRebalance := fmt.Sprintf("db_rebalance_%d", i)
			startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
		}
	}()
	wg.Wait()
	// sleep 1 millsecond to wait start work done
	time.Sleep(1 * time.Millisecond)

	assert.Equal(t, 10, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// test shift start and we'll get error, the alived instances > 10
	startInstanceErr(t, log, shiftMgr, "db_table_11", ShiftTypeRebalance)
}

func TestWaitInstanceFinishThread(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()
	keyOK := "db_tbl"

	// 1. start an instance
	startInstanceOK(t, log, shiftMgr, keyOK, ShiftTypeReshard)
	// Now shiftMgr has 1 alived instance
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. start wait work thread
	shiftMgr.WaitInstanceFinishThread(keyOK)

	// 3. send finish ok signal to instance
	shift := shiftMgr.(*ShiftManager).instancesAlived[keyOK].shift
	shift.(*MockShift).setAllDoneSignal()

	// 4. check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusSuccess, shiftMgr.GetStatus(keyOK))
	assert.Equal(t, ShiftTypeReshard, shiftMgr.GetShiftType(keyOK))
}

func TestWaitInstanceFinishThreadShiftStopError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start two instances
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	keyRebalance := "db_rebalance"
	startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. start wait work thread
	err := shiftMgr.WaitInstanceFinishThread("unknown_key")
	assert.NotNil(t, err)

	err = shiftMgr.WaitInstanceFinishThread(keyReshard)
	assert.Nil(t, err)
	err = shiftMgr.WaitInstanceFinishThread(keyRebalance)
	assert.Nil(t, err)

	// 3. send stop/error ok signal to instance
	shift := shiftMgr.(*ShiftManager).instancesAlived[keyReshard].shift
	shift.(*MockShift).SetStopSignal()
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyReshard))
	assert.Equal(t, ShiftTypeReshard, shiftMgr.GetShiftType(keyReshard))

	// 4. check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	shift = shiftMgr.(*ShiftManager).instancesAlived[keyRebalance].shift
	shift.(*MockShift).SetStopSignal()
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyRebalance))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyRebalance))
}

func TestWaitInstanceFinishThreadShiftRunError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start two instances
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	keyRebalance := "db_rebalance"
	startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. start wait work thread
	shiftMgr.WaitInstanceFinishThread(keyReshard)
	shiftMgr.WaitInstanceFinishThread(keyRebalance)

	// 3. send error ok signal to instance
	shift := shiftMgr.(*ShiftManager).instancesAlived[keyReshard].shift
	shift.(*MockShift).setErrSignal()
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyReshard))
	assert.Equal(t, ShiftTypeReshard, shiftMgr.GetShiftType(keyReshard))

	// 4. check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	shift = shiftMgr.(*ShiftManager).instancesAlived[keyRebalance].shift
	shift.(*MockShift).setErrSignal()
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyRebalance))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyRebalance))
}

func TestWaitInstanceFinish(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()
	keyOK_1 := "db_tbl_1"
	keyOK_2 := "db_tbl_2"

	// 1. start two instance
	startInstanceOK(t, log, shiftMgr, keyOK_1, ShiftTypeRebalance)
	// Now shiftMgr has 1 alived instance
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	startInstanceOK(t, log, shiftMgr, keyOK_2, ShiftTypeRebalance)
	// Now shiftMgr has 2 alived instance
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. mock a finish thread, wait 1s to finish an instance
	go func() {
		// sleep 1s to mock finish work of keyOK_1
		time.Sleep(1 * time.Second)
		// send finish ok signal to instance keyOK_1
		shift_1 := shiftMgr.(*ShiftManager).instancesAlived[keyOK_1].shift
		shift_1.(*MockShift).setAllDoneSignal()

		// sleep 1s to mock finish work of keyOK_2
		time.Sleep(1 * time.Second)
		// send finish ok signal to instance keyOK_2
		shift_2 := shiftMgr.(*ShiftManager).instancesAlived[keyOK_2].shift
		shift_2.(*MockShift).setAllDoneSignal()
	}()

	// 3. start wait work, wait 1s keyOK_1 instance finish
	shiftMgr.WaitInstanceFinish(keyOK_1)
	// check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusSuccess, shiftMgr.GetStatus(keyOK_1))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyOK_1))

	// 4. start wait work, wait 2s keyOK_1 instance finish
	shiftMgr.WaitInstanceFinish(keyOK_2)
	// check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusSuccess, shiftMgr.GetStatus(keyOK_2))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyOK_2))

}

func TestWaitInstanceFinishRunError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()
	keyOK_1 := "db_tbl_1"
	keyOK_2 := "db_tbl_2"

	// 1. start two instance
	startInstanceOK(t, log, shiftMgr, keyOK_1, ShiftTypeRebalance)
	// Now shiftMgr has 1 alived instance
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	startInstanceOK(t, log, shiftMgr, keyOK_2, ShiftTypeRebalance)
	// Now shiftMgr has 2 alived instance
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. mock a finish thread, wait 1s to finish an instance
	go func() {
		// sleep 1s to mock finish work of keyOK_1
		time.Sleep(1 * time.Second)
		// send finish ok signal to instance keyOK_1
		shift_1 := shiftMgr.(*ShiftManager).instancesAlived[keyOK_1].shift
		shift_1.(*MockShift).setErrSignal()

		// sleep 1s to mock finish work of keyOK_2
		time.Sleep(1 * time.Second)
		// send finish ok signal to instance keyOK_2
		shift_2 := shiftMgr.(*ShiftManager).instancesAlived[keyOK_2].shift
		shift_2.(*MockShift).setErrSignal()
	}()

	// 3. start wait work, wait 1s keyOK_1 instance finish
	shiftMgr.WaitInstanceFinish(keyOK_1)
	// check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyOK_1))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyOK_1))

	// 4. start wait work, wait 2s keyOK_1 instance finish
	shiftMgr.WaitInstanceFinish(keyOK_2)
	// check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyOK_2))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyOK_2))

}

func TestWaitInstanceFinishStop(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()
	keyOK_1 := "db_tbl_1"
	keyOK_2 := "db_tbl_2"

	// 1. start two instance
	startInstanceOK(t, log, shiftMgr, keyOK_1, ShiftTypeRebalance)
	// Now shiftMgr has 1 alived instance
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	startInstanceOK(t, log, shiftMgr, keyOK_2, ShiftTypeRebalance)
	// Now shiftMgr has 2 alived instance
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. mock a finish thread, wait 1s to finish an instance
	go func() {
		// sleep 1s to mock finish work of keyOK_1
		time.Sleep(1 * time.Second)
		// send finish ok signal to instance keyOK_1
		shift_1 := shiftMgr.(*ShiftManager).instancesAlived[keyOK_1].shift
		shift_1.(*MockShift).SetStopSignal()

		// sleep 1s to mock finish work of keyOK_2
		time.Sleep(1 * time.Second)
		// send finish ok signal to instance keyOK_2
		shift_2 := shiftMgr.(*ShiftManager).instancesAlived[keyOK_2].shift
		shift_2.(*MockShift).SetStopSignal()
	}()

	// 3. start wait work, wait 1s keyOK_1 instance finish
	shiftMgr.WaitInstanceFinish(keyOK_1)
	// check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyOK_1))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyOK_1))

	// 4. start wait work, wait 2s keyOK_1 instance finish
	shiftMgr.WaitInstanceFinish(keyOK_2)
	// check alived instances and finish instances
	// sleep 1s to wait the thread done the work then we can check instance nums
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyOK_2))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyOK_2))

}

func TestStopOneInstance(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start two instances
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	keyRebalance := "db_rebalance"
	startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. start wait work thread
	err := shiftMgr.WaitInstanceFinishThread(keyReshard)
	assert.Nil(t, err)
	err = shiftMgr.WaitInstanceFinishThread(keyRebalance)
	assert.Nil(t, err)

	// 3. stop db_reshard
	err = shiftMgr.StopOneInstance(keyReshard)
	time.Sleep(1 * time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyReshard))
	assert.Equal(t, ShiftTypeReshard, shiftMgr.GetShiftType(keyReshard))

	// 4. stop db_rebalance
	err = shiftMgr.StopOneInstance(keyRebalance)
	time.Sleep(1 * time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyRebalance))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyRebalance))
}

func TestStopOneInstanceError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start two instances
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	keyRebalance := "db_rebalance"
	startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. start wait work thread
	shiftMgr.WaitInstanceFinishThread(keyReshard)
	err := shiftMgr.WaitInstanceFinishThread(keyRebalance)
	assert.Nil(t, nil)

	// 3. stop db_reshard
	err = shiftMgr.StopOneInstance("db_err_key")
	assert.NotNil(t, err)
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
}

func TestStopAllInstance(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start two instances
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	keyRebalance := "db_rebalance"
	startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// 2. start wait work thread
	err := shiftMgr.WaitInstanceFinishThread(keyReshard)
	assert.Nil(t, err)
	err = shiftMgr.WaitInstanceFinishThread(keyRebalance)
	assert.Nil(t, err)

	// 3. stop db_reshard
	err = shiftMgr.StopAllInstance()
	time.Sleep(5 * time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 2, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyReshard))
	assert.Equal(t, ShiftTypeReshard, shiftMgr.GetShiftType(keyReshard))
	assert.Equal(t, ShiftStatusFail, shiftMgr.GetStatus(keyRebalance))
	assert.Equal(t, ShiftTypeRebalance, shiftMgr.GetShiftType(keyRebalance))
}

func TestGetStatusError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start one instance
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	status := shiftMgr.GetStatus("key_not_found")
	assert.Equal(t, ShiftStatusNone, status)
}

func TestClose(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Nanosecond)
			keyReshard := fmt.Sprintf("db_reshard_%d", i)
			// 1. new and start an instance
			startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
			// 2. wait thread start
			err := shiftMgr.WaitInstanceFinishThread(keyReshard)
			assert.Nil(t, err)
		}
	}()
	wg.Add(1)
	go func() {
		wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(50 * time.Nanosecond)
			keyRebalance := fmt.Sprintf("db_rebalance_%d", i)
			// 1. new and start an instance
			startInstanceOK(t, log, shiftMgr, keyRebalance, ShiftTypeRebalance)
			// 2. wait thread start
			err := shiftMgr.WaitInstanceFinishThread(keyRebalance)
			assert.Nil(t, err)
		}
	}()
	wg.Wait()
	// sleep 1 millsecond to wait start work done
	time.Sleep(1 * time.Millisecond)

	assert.Equal(t, 10, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	// call close
	err := shiftMgr.Close()
	assert.Nil(t, err)

	// sleep 1 millsecond to wait close work done
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 10, shiftMgr.(*ShiftManager).getInstancesFinishedNums())
}

func TestGetShiftTypeError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start one instance
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	typ := shiftMgr.GetShiftType("key_not_found")
	assert.Equal(t, ShiftTypeNone, typ)
}

func TestGetProgress(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	shiftMgr := NewShiftManager(log)
	shiftMgr.Init()

	// 1. start one instance
	keyReshard := "db_reshard"
	startInstanceOK(t, log, shiftMgr, keyReshard, ShiftTypeReshard)
	assert.Equal(t, 1, shiftMgr.(*ShiftManager).getInstancesAlivedNums())
	assert.Equal(t, 0, shiftMgr.(*ShiftManager).getInstancesFinishedNums())

	progress := shiftMgr.GetProgress(keyReshard)
	// TODO: now GetProgress() func not implemented.
	assert.Nil(t, progress)
}
