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

	"github.com/radondb/shift/shift"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// MockShiftInfo used to mock a shift info
var MockShiftInfo = &ShiftInfo{
	From:         "src",
	FromUser:     "test",
	FromPassword: "test",
	FromDatabase: "testdb",
	FromTable:    "tbl",

	To:         "dst",
	ToUser:     "user",
	ToPassword: "user",
	ToDatabase: "todb",
	ToTable:    "totbl",

	Cleanup:                false,
	Checksum:               true,
	MysqlDump:              "mysqldump",
	Threads:                128,
	PosBehinds:             2048,
	WaitTimeBeforeChecksum: 10,
	RadonURL:               "",
}

// MockShift used to mock a shift for test
type MockShift struct {
	log *xlog.Log

	err        chan struct{}
	allDone    chan struct{}
	stopSignal chan struct{}
}

// NewMockShift used to new a mockshift
func NewMockShift(log *xlog.Log) shift.ShiftHandler {
	return &MockShift{
		log:        log,
		err:        make(chan struct{}),
		allDone:    make(chan struct{}),
		stopSignal: make(chan struct{}),
	}
}

// Start used to start a shift work.
func (mockShift *MockShift) Start() error {
	return nil
}

// WaitFinish used to wait success or fail signal to finish.
func (mockShift *MockShift) WaitFinish() error {
	log := mockShift.log
	select {
	case <-mockShift.getAllDoneCh():
		log.Info("mockshift.table.OK")
		return nil
	case <-mockShift.getErrorCh():
		log.Error("mockshift.table.get.error")
		return fmt.Errorf("mockshift.table.get.error")
	case <-mockShift.getStopSignal():
		log.Info("mockshift.table.get.stop.signal")
		return fmt.Errorf("mockshift.table.get.stop.signal")
	}
}

// ChecksumTable used to checksum data src tbl and dst tbl.
func (mockShift *MockShift) ChecksumTable() error {
	return nil
}

// SetStopSignal used set a stop signal to stop a shift work.
func (mockShift *MockShift) SetStopSignal() {
	close(mockShift.stopSignal)
}

// setAllDoneSignal used to set allDone signal
func (mockShift MockShift) setAllDoneSignal() {
	close(mockShift.allDone)
}

// setErrSignal used to set allDone signal
func (mockShift MockShift) setErrSignal() {
	close(mockShift.err)
}

// getAllDoneCh used to get success signal
func (mockShift *MockShift) getAllDoneCh() <-chan struct{} {
	return mockShift.allDone
}

// getErrorCh used to get error signal
func (mockShift *MockShift) getErrorCh() <-chan struct{} {
	return mockShift.err
}

// getStopSignal used to get stop signal
func (mockShift *MockShift) getStopSignal() <-chan struct{} {
	return mockShift.stopSignal
}
