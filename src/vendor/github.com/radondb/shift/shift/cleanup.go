/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/client"
)

// Cleanup used to clean up the table on the from who has shifted,
// Or cleanup the to tables who half shifted.
// This func must be called after canal closed, otherwise it maybe replicated by canal.
func (shift *Shift) Cleanup() error {
	log := shift.log

	// Set throttle to unlimits.
	if err := shift.setRadonThrottle(0); err != nil {
		log.Error("shift.cleanup.set.radon.throttle.error")
		return errors.Trace(err)
	}

	// Set readonly to false.
	if err := shift.setRadonReadOnly(false); err != nil {
		log.Error("shift.cleanup.set.radon.readonly.error")
		return errors.Trace(err)
	}

	// Cleanup.
	if shift.cfg.Cleanup {
		if shift.allDone.Get() {
			return shift.cleanupFrom()
		}
		return shift.cleanupTo()
	}

	// Rename fromTable.
	if shift.cfg.Rebalance && shift.allDone.Get() {
		return shift.renameFromTable()
	}
	return nil
}

// cleanupFrom used to cleanup the table on from.
// This func was called after shift succuess with cfg.Cleanup=true.
func (shift *Shift) cleanupFrom() error {
	log := shift.log
	cfg := shift.cfg

	log.Info("shift.cleanup.from.table[%s.%s]...", cfg.FromDatabase, cfg.FromTable)
	if _, isSystem := sysDatabases[strings.ToLower(cfg.FromDatabase)]; !isSystem {
		from, err := client.Connect(cfg.From, cfg.FromUser, cfg.FromPassword, "")
		if err != nil {
			log.Error("shift.cleanup.from.new.connection.error")
			return errors.Trace(err)
		}

		sql := fmt.Sprintf("drop table `%s`.`%s`", cfg.FromDatabase, cfg.FromTable)
		if _, err := from.Execute(sql); err != nil {
			log.Error("shift.execute.sql[%s].error", sql)
			return errors.Trace(err)
		}
		log.Info("shift.cleanup.from.table.done.and.do.close.to.connect...")
		return from.Close()
	} else {
		log.Info("shift.table.is.system.cleanup.skip...")
		return nil
	}
}

// cleanupTo used to cleanup the table on to.
// This func was called when shift failed.
func (shift *Shift) cleanupTo() error {
	log := shift.log
	cfg := shift.cfg

	log.Info("shift.cleanup.to[%s/%s]...", cfg.ToDatabase, cfg.ToTable)
	if _, isSystem := sysDatabases[strings.ToLower(cfg.FromDatabase)]; !isSystem {
		to, err := client.Connect(cfg.To, cfg.ToUser, cfg.ToPassword, "")
		if err != nil {
			log.Error("shift.cleanup.to.new.connection.error")
			return errors.Trace(err)
		}

		sql := fmt.Sprintf("drop table `%s`.`%s`", cfg.ToDatabase, cfg.ToTable)
		if _, err := to.Execute(sql); err != nil {
			log.Error("shift.cleanup.to.execute[%s].error", sql)
			return errors.Trace(err)
		}
		log.Info("shift.cleanup.to.done.and.do.close.to.connect...")
		return to.Close()
	} else {
		log.Info("shift.table.is.system.cleanup.skip...")
		return nil
	}
}
