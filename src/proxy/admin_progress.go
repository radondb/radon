/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"

	"router"

	"github.com/radondb/shift/shift"
	shiftLog "github.com/radondb/shift/xlog"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Progress ...
type Progress struct {
	log    *xlog.Log
	router *router.Router
	db     string
	table  string
}

// NewProgress -- creates new progress.
func NewProgress(log *xlog.Log, router *router.Router, db string, table string) *Progress {
	return &Progress{
		log:    log,
		router: router,
		db:     db,
		table:  table,
	}
}

// CheckDBTable check the database and table.
func (progress *Progress) CheckDBTable() error {
	// make sure the [db].table is exist, if exist, get the table config
	if tableCfg, err := progress.router.TableConfig(progress.db, progress.table); err != nil {
		return err
	} else if tableCfg.ShardType != "SINGLE" {
		return fmt.Errorf("progress.check.[%s].is.singleTable.err", progress.table)
	}

	return nil
}

// GetShiftProgressInfo get the shift progress info
func (progress *Progress) GetShiftProgressInfo() (*sqltypes.Result, error) {
	log := shiftLog.NewStdLog(shiftLog.Level(shiftLog.INFO))
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "DumpProgressRate", Type: querypb.Type_VARCHAR},
		{Name: "DumpRemainTime", Type: querypb.Type_VARCHAR},
		{Name: "PositionBehinds", Type: querypb.Type_VARCHAR},
		{Name: "SynGTID", Type: querypb.Type_VARCHAR},
		{Name: "MasterGTID", Type: querypb.Type_VARCHAR},
		{Name: "MigrateStatus", Type: querypb.Type_VARCHAR},
	}

	if err := progress.CheckDBTable(); err != nil {
		return qr, err
	}

	cfg := &shift.Config{
		FromDatabase: progress.db,
		FromTable:    progress.table,
	}

	shift := shift.NewShift(log, cfg)
	shiftProgress, err := shift.ReadShiftProgress()
	if err != nil {
		return qr, err
	}

	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(shiftProgress.DumpProgressRate)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(shiftProgress.DumpRemainTime)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(shiftProgress.PositionBehinds)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(shiftProgress.SynGTID)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(shiftProgress.MasterGTID)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(shiftProgress.MigrateStatus)),
	}
	qr.Rows = append(qr.Rows, row)

	return qr, nil
}
