/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package autoincrement

import (
	"sync"
	"time"

	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// AutoIncrement struct.
// Using Now().UnixNano as start seed seq.
type AutoIncrement struct {
	mu     sync.Mutex
	log    *xlog.Log
	seq    uint64
	router *router.Router
}

// NewAutoIncrement -- creates new AutoIncrement.
func NewAutoIncrement(log *xlog.Log, router *router.Router) AutoIncrementHandler {
	return &AutoIncrement{
		log:    log,
		router: router,
	}
}

// Init -- used to init the plug module.
func (autoinc *AutoIncrement) Init() error {
	autoinc.seq = uint64(time.Now().UnixNano())
	return nil
}

// Process -- process auto-increment.
// Append the auto-increment column&value to the end of the row if not exists.
func (autoinc *AutoIncrement) Process(database string, ins *sqlparser.Insert) error {
	var seq uint64
	router := autoinc.router

	// Qualifier is database in the insert query, such as "db.t1".
	if !ins.Table.Qualifier.IsEmpty() {
		database = ins.Table.Qualifier.String()
	}
	table := ins.Table.Name.String()

	tblInfo, err := router.TableConfig(database, table)
	if err != nil {
		return err
	}

	// Get seq(thread-safe).
	autoinc.mu.Lock()
	seq = autoinc.seq
	switch rows := ins.Rows.(type) {
	case sqlparser.Values:
		autoinc.seq += uint64(len(rows))
	}
	autoinc.mu.Unlock()

	if tblInfo.AutoIncrement != nil {
		modifyForAutoinc(ins, tblInfo.AutoIncrement, seq)
	}
	return nil
}

// Close -- close the plugin.
func (autoinc *AutoIncrement) Close() error {
	return nil
}
