/*
 * Radon
 *
 * Copyright 2018-2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shiftmanager

import (
	"github.com/radondb/shift/shift"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ShiftStatus used to indicates shift instance's status.
type ShiftStatus int

const (
	// ShiftStatusNone enum, if status is none, some errors happen
	ShiftStatusNone ShiftStatus = iota

	// ShiftStatusMigrating enum
	ShiftStatusMigrating

	// ShiftStatusSuccess enum
	ShiftStatusSuccess

	// ShiftStatusFail enum
	ShiftStatusFail
)

// ShiftType is used to distinguish what different type of shift
// If it is called by reshard, the type will be ShiftTypeReshard
// If it is called by rebalance, the type will be ShiftTypeRebalance
type ShiftType int

const (
	// ShiftTypeNone enum, a shift type should not be none
	ShiftTypeNone ShiftType = iota

	// ShiftTypeReshard enum
	ShiftTypeReshard

	// ShiftTypeRebalance enum
	ShiftTypeRebalance
)

// ShiftInfo used to record basic infos used by shift
type ShiftInfo struct {
	From         string
	FromUser     string
	FromPassword string
	FromDatabase string
	FromTable    string

	To         string
	ToUser     string
	ToPassword string
	ToDatabase string
	ToTable    string

	Rebalance              bool
	Cleanup                bool // if Cleanup is true, drop the FromTable.
	Checksum               bool
	MysqlDump              string
	Threads                int
	PosBehinds             int
	WaitTimeBeforeChecksum int
	RadonURL               string
}

type ShiftMgrHandler interface {
	Init() error
	NewShiftInstance(shiftInfo *ShiftInfo, typ ShiftType) (shift.ShiftHandler, error)
	StartShiftInstance(key string, shift shift.ShiftHandler, typ ShiftType) error
	WaitInstanceFinishThread(key string) error
	WaitInstanceFinish(key string) error
	StopOneInstance(key string) error
	StopAllInstance() error
	GetStatus(key string) ShiftStatus
	GetProgress(key string) *sqltypes.Result
	GetShiftType(key string) ShiftType
	Close() error
}
