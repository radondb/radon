/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"testing"

	"github.com/radondb/shift/xlog"
	"github.com/stretchr/testify/assert"
)

func TestReadWriteShiftProgress(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	expected := &ShiftProgress{
		DumpProgressRate: "5%",
		DumpRemainTime:   "20s",
		PositionBehinds:  "1000",
		SynGTID:          "1-1",
		MasterGTID:       "1-2",
		MigrateStatus:    "migrating",
	}
	cfg := &Config{
		FromDatabase: "testdb",
		FromTable:    "testtbl",
	}
	shift := NewShift(log, cfg)
	shift.progress = expected
	err := shift.WriteShiftProgress()
	assert.Nil(t, err)
	actual, err := shift.ReadShiftProgress()
	assert.True(t, assert.ObjectsAreEqualValues(expected, actual))
}
