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

	"github.com/siddontang/go-mysql/client"
)

// Cleanup used to clean up the table on the from who has shifted,
// Or cleanup the to tables who half shifted.
// This func must be called after canal closed, otherwise it maybe replicated by canal.
func (shift *Shift) Cleanup() {
	log := shift.log

	// Set throttle to unlimits.
	if err := shift.setRadonThrottle(0); err != nil {
		log.Error("shift.cleanup.set.radon.throttle.error:%+v", err)
	}

	// Set readonly to false.
	if err := shift.setRadonReadOnly(false); err != nil {
		log.Error("shift.cleanup.set.radon.readonly.error:%+v", err)
	}

	// Cleanup.
	if shift.cfg.Cleanup {
		if shift.allDone {
			shift.cleanupFrom()
		} else {
			shift.cleanupTo()
		}
	}
}

// cleanupFrom used to cleanup the table on from.
// This func was called after shift succuess with cfg.Cleanup=true.
func (shift *Shift) cleanupFrom() {
	log := shift.log
	cfg := shift.cfg

	log.Info("shift.cleanup.from.table[%s.%s]...", cfg.FromDatabase, cfg.FromTable)
	if _, isSystem := sysDatabases[strings.ToLower(cfg.FromDatabase)]; !isSystem {
		from, err := client.Connect(cfg.From, cfg.FromUser, cfg.FromPassword, "")
		if err != nil {
			shift.panicMe("shift.cleanup.connection.error:%+v", err)
		}
		defer from.Close()

		sql := fmt.Sprintf("drop table `%s`.`%s`", cfg.FromDatabase, cfg.FromTable)
		if _, err := from.Execute(sql); err != nil {
			shift.panicMe("shift.execute.sql[%s].error:%+v", sql, err)
		}
	} else {
		log.Info("shift.table.is.system.cleanup.skip...")
	}
	log.Info("shift.cleanup.from.table.done...")
}

// cleanupTo used to cleanup the table on to.
// This func was called when shift failed.
func (shift *Shift) cleanupTo() {
	log := shift.log
	cfg := shift.cfg

	log.Info("shift.cleanup.to[%s/%s]...", cfg.ToDatabase, cfg.ToTable)
	if _, isSystem := sysDatabases[strings.ToLower(cfg.FromDatabase)]; !isSystem {
		to, err := client.Connect(cfg.To, cfg.ToUser, cfg.ToPassword, "")
		if err != nil {
			log.Error("shift.cleanup.to.connect.error:%+v", err)
			return
		}
		defer to.Close()

		sql := fmt.Sprintf("drop table `%s`.`%s`", cfg.ToDatabase, cfg.ToTable)
		if _, err := to.Execute(sql); err != nil {
			log.Error("shift.cleanup.to.execute[%s].error:%+v", sql, err)
			return
		}
	} else {
		log.Info("shift.table.is.system.cleanup.skip...")
	}
	log.Info("shift.cleanup.to.done...")
}
