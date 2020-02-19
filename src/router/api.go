/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"config"

	"github.com/pkg/errors"
	"github.com/radondb/shift/shift"
)

// RDatabase tuple.
type RDatabase struct {
	DB     string
	Tables []*Table
}

// Rule tuple.
type Rule struct {
	Schemas []RDatabase
}

// Rules returns router's schemas.
func (r *Router) Rules() *Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rule := &Rule{}

	for key, schema := range r.Schemas {
		rdb := RDatabase{DB: key}
		for _, v := range schema.Tables {
			rdb.Tables = append(rdb.Tables, v)
		}
		rule.Schemas = append(rule.Schemas, rdb)
	}
	return rule
}

// PartitionRuleShift used to shift a rule from backend to another.
// The processes as:
// 1. change the backend in memory.
// 2. flush the table config to disk.
// 3. reload the config to memory.
// Note:
// If the reload fails, panic it since the config is in chaos.
func (r *Router) PartitionRuleShift(fromBackend string, toBackend string, database string, partitionTable string) error {
	log := r.log

	log.Warning("router.partition.rule.shift.from[%s].to[%s].database[%s].partitionTable[%s]", fromBackend, toBackend, database, partitionTable)
	table, err := r.changeTheRuleBackend(fromBackend, toBackend, database, partitionTable)
	if err != nil {
		log.Error("router.partition.rule.shift.changeTheRuleBackend.error:%+v", err)
		return err
	}
	log.Warning("router.partition.rule.shift.change.the.rule.done")

	log.Warning("router.partition.rule.shift.RefreshTable.prepare")
	if err := r.RefreshTable(database, table); err != nil {
		log.Panic("router.partition.rule.shift.RefreshTable.error:%+v", err)
		return err
	}
	log.Warning("router.partition.rule.shift.RefreshTable.done")
	return nil
}

//
// 1. Find the table config and partition config.
// 2. Change the backend.
// 3. Write tableconfig to disk.
func (r *Router) changeTheRuleBackend(fromBackend string, toBackend string, database string, partitionTable string) (string, error) {
	log := r.log
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fromBackend == toBackend {
		return "", errors.Errorf("router.rule.change.from[%s].cant.equal.to[%s]", fromBackend, toBackend)
	}

	// 1. Find the table config.
	v, partitionConfig, err := r.findPartitionConfig(fromBackend, database, partitionTable)
	if err != nil {
		return "", err
	}
	table := v.Name
	tableConfig := v.TableConfig

	// 2. Change the backend to to-backend.
	if tableConfig.ShardType == "GLOBAL" {
		for _, partition := range tableConfig.Partitions {
			if partition.Backend == toBackend {
				return "", errors.Errorf("the.table:[%s].already.exists.in.the.backend[%s]", partitionTable, toBackend)
			}
		}
		partConf := &config.PartitionConfig{
			Table:   table,
			Backend: toBackend,
		}
		tableConfig.Partitions = append(tableConfig.Partitions, partConf)
	} else {
		partitionConfig.Backend = toBackend
	}

	// 3. Flush table config to disk.
	if err := r.writeTableFrmData(database, table, tableConfig); err != nil {
		// Memory config reset.
		if tableConfig.ShardType == "GLOBAL" {
			tableConfig.Partitions = append(tableConfig.Partitions[:(len(tableConfig.Partitions) - 1)])
		} else {
			partitionConfig.Backend = fromBackend
		}
		return "", err
	}

	// 4. Update the version.
	if err := config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("change.the.rule.table.update.version.error:%v", err)
		return "", err
	}
	return table, nil
}

// ReLoad used to re-load the config files from disk to cache.
func (r *Router) ReLoad() error {
	log := r.log

	// Clear the cache.
	log.Warning("router.reload.clear...")
	r.clear()

	// ReLoad the meta from disk.
	log.Warning("router.reload.load.meta.from.disk...")
	return r.LoadConfig()
}

func (r *Router) findPartitionConfig(backend, database, partitionTable string) (*Table, *config.PartitionConfig, error) {
	schema, ok := r.Schemas[database]
	if !ok {
		return nil, nil, errors.Errorf("router.find.partition.config.cant.found.database:%s", database)
	}

	for _, v := range schema.Tables {
		for _, partition := range v.TableConfig.Partitions {
			if (partition.Backend == backend) && (partition.Table == partitionTable) {
				log.Warning("router.find.partition.config.[%s:%s].backend.[%s].find.partition:%+v", database, partitionTable, backend, partition)
				return v, partition, nil
			}
		}
	}
	return nil, nil, errors.Errorf("router.find.table.config.cant.found.backend[%s]+table:[%s]", backend, partitionTable)
}

// PatitionStatusModify used to modify the status and cleanup in partitionconfig.
func (r *Router) PatitionStatusModify(migrateStatus shift.Status, isCleanup bool, fromBackend, toBackend, database, partitionTable string) error {
	log := r.log

	log.Warning("router.patition.status.modify.migrateStatus[%d].cleanup[%t].from[%s].to[%s].database[%s].partitionTable[%s]",
		migrateStatus, isCleanup, fromBackend, toBackend, database, partitionTable)
	table, err := r.changeTheRuleStatus(migrateStatus, isCleanup, fromBackend, toBackend, database, partitionTable)
	if err != nil {
		log.Error("router.patition.status.modify.changeTheRuleStatus.error:%+v", err)
		return err
	}
	log.Warning("router.patition.status.modify.changeTheRuleStatus.done")

	log.Warning("router.patition.status.modify.RefreshTable.prepare")
	if err := r.RefreshTable(database, table); err != nil {
		log.Panic("router.patition.status.modify.RefreshTable.error:%+v", err)
		return err
	}
	log.Warning("router.patition.status.modify.RefreshTable.done")
	return nil
}

// migrateStatus:
// 0. migrating.
//  Change the status to "migrating".
//  Change the cleanup to "toBackend".
//
// 1. migrate success.
//  Change the status to "".
//  If isCleanup is true, the data has been cleaned up in shift, just change cleanup to "",
//  else if shard tables, the rule `backend` has been changed to `toBackend`, so we need change cleanup to `fromBackend`,
//  else if golbal tables, we need find the `fromBackend` partitionConfig, and change the cleanup to "".
//
// 2. migrate failure.
//  Change the status to "".
//  If isCleanup is true, just change cleanup tp "",
//  else change cleanup to `toBackend`.
func (r *Router) changeTheRuleStatus(migrateStatus shift.Status, isCleanup bool, fromBackend, toBackend, database, partitionTable string) (string, error) {
	var backend, status, cleanup string
	log := r.log
	r.mu.RLock()
	defer r.mu.RUnlock()

	switch migrateStatus {
	case shift.MIGRATING:
		backend = fromBackend
		status = config.MIGRATING
		cleanup = toBackend
	case shift.SUCCESS:
		backend = toBackend
		status = ""
		if !isCleanup {
			cleanup = fromBackend
		}
	case shift.FAILURE:
		backend = fromBackend
		status = ""
		if !isCleanup {
			cleanup = toBackend
		}
	}

	// 1. Find the table config.
	v, partitionConfig, err := r.findPartitionConfig(backend, database, partitionTable)
	if err != nil {
		return "", err
	}
	table := v.Name
	tableConfig := v.TableConfig
	//For golbal tables, need find the `fromBackend` partitionConfig.
	if tableConfig.ShardType == "GLOBAL" && migrateStatus == 1 {
		for _, partition := range v.TableConfig.Partitions {
			if (partition.Backend == fromBackend) && (partition.Table == partitionTable) {
				partitionConfig = partition
				cleanup = ""
				break
			}
		}
	}

	// 2. Change status and cleanup.
	originalStatus := partitionConfig.Status
	originalCleanup := partitionConfig.Cleanup
	partitionConfig.Status = status
	partitionConfig.Cleanup = cleanup

	// 3. Flush table config to disk.
	if err := r.writeTableFrmData(database, table, tableConfig); err != nil {
		// Memory config reset.
		partitionConfig.Status = originalStatus
		partitionConfig.Cleanup = originalCleanup
		return "", err
	}

	// 4. Update the version.
	if err := config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("change.the.rule.table.update.version.error:%v", err)
		return "", err
	}
	return table, nil
}
