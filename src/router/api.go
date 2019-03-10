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
	var table string
	var tableConfig *config.TableConfig
	var partitionConfig *config.PartitionConfig

	log := r.log
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fromBackend == toBackend {
		return "", errors.Errorf("router.rule.change.from[%s].cant.equal.to[%s]", fromBackend, toBackend)
	}

	schema, ok := r.Schemas[database]
	if !ok {
		return "", errors.Errorf("router.rule.change.cant.found.database:%s", database)
	}

	// 1. Find the table config.
	found := false
	for _, v := range schema.Tables {
		if found {
			break
		}
		for _, partition := range v.TableConfig.Partitions {
			if (partition.Backend == fromBackend) && (partition.Table == partitionTable) {
				log.Warning("router.rule[%s:%s].change.from[%s].to[%s].found:%+v", database, partitionTable, fromBackend, toBackend, partition)

				found = true
				table = v.Name
				tableConfig = v.TableConfig
				partitionConfig = partition
				break
			}
		}
	}
	if !found {
		return "", errors.Errorf("router.rule.change.cant.found.backend[%s]+table:[%s]", fromBackend, partitionTable)
	}

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
