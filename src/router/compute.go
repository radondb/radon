/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"fmt"
	"sort"
	"strconv"

	"config"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

var (
	partitionNums = []int{8, 16, 32, 64}
)

// HashUniform used to uniform the hash slots to backends.
func (r *Router) HashUniform(table, shardkey string, backends []string, partitionNum *sqlparser.SQLVal) (*config.TableConfig, error) {
	if table == "" {
		return nil, errors.New("table.cant.be.null")
	}
	if shardkey == "" {
		return nil, errors.New("shard.key.cant.be.null")
	}

	slots := r.conf.Slots
	blocks := r.conf.Blocks
	if partitionNum != nil {
		num, err := strconv.Atoi(common.BytesToString(partitionNum.Val))
		if err != nil {
			return nil, err
		}

		exists := false
		for _, partNum := range partitionNums {
			if num == partNum {
				exists = true
				break
			}
		}
		if !exists {
			return nil, errors.New("number.of.partitions.must.be.one.of.the.list.[8, 16, 32, 64]")
		}
		blocks = slots / num
	}

	nums := len(backends)
	if nums == 0 {
		return nil, errors.New("router.compute.backends.is.null")
	}
	if nums >= slots {
		return nil, errors.Errorf("router.compute.backends[%d].too.many:[max:%d]", nums, slots)
	}

	// sort backends.
	sort.Strings(backends)
	tableConf := &config.TableConfig{
		Name:       table,
		Slots:      slots,
		Blocks:     blocks,
		ShardKey:   shardkey,
		ShardType:  methodTypeHash,
		Partitions: make([]*config.PartitionConfig, 0, 16),
	}

	slotsPerShard := slots / nums
	tablesPerShard := slotsPerShard / blocks
	for s := 0; s < nums; s++ {
		for i := 0; i < tablesPerShard; i++ {
			step := s * slotsPerShard
			min := i*blocks + step
			max := (i+1)*blocks + step
			if i == tablesPerShard-1 {
				if s == nums-1 {
					max = slots
				} else {
					max = step + slotsPerShard
				}
			}
			name := s*tablesPerShard + i
			partConf := &config.PartitionConfig{
				Table:   fmt.Sprintf("%s_%04d", table, name),
				Segment: fmt.Sprintf("%d-%d", min, max),
				Backend: backends[s],
			}
			tableConf.Partitions = append(tableConf.Partitions, partConf)
		}
	}
	return tableConf, nil
}

// GlobalUniform used to uniform the global table to backends.
func (r *Router) GlobalUniform(table string, backends []string) (*config.TableConfig, error) {
	if table == "" {
		return nil, errors.New("table.cant.be.null")
	}
	nums := len(backends)
	if nums == 0 {
		return nil, errors.New("router.compute.backends.is.null")
	}

	tableConf := &config.TableConfig{
		Name:       table,
		ShardType:  methodTypeGlobal,
		ShardKey:   "",
		Partitions: make([]*config.PartitionConfig, 0, 16),
	}

	for s := 0; s < nums; s++ {
		partConf := &config.PartitionConfig{
			Table:   table,
			Backend: backends[s],
		}
		tableConf.Partitions = append(tableConf.Partitions, partConf)
	}
	return tableConf, nil
}

// SingleUniform used to uniform the single table to backends.
func (r *Router) SingleUniform(table string, backends []string) (*config.TableConfig, error) {
	if table == "" {
		return nil, errors.New("table.cant.be.null")
	}
	nums := len(backends)
	if nums == 0 {
		return nil, errors.New("router.compute.backends.is.null")
	}

	return &config.TableConfig{
		Name:      table,
		ShardType: methodTypeSingle,
		ShardKey:  "",
		Partitions: []*config.PartitionConfig{&config.PartitionConfig{
			Table:   table,
			Backend: backends[0],
		}},
	}, nil
}

func listMergePartition(partitionDef sqlparser.PartitionDefinitions) (map[string]string, error) {
	partitionMap := make(map[string]string)
	for _, onePart := range partitionDef {
		row := onePart.Row
		valuesNum := len(row)
		for i := 0; i < valuesNum; i++ {
			key := common.BytesToString(row[i].(*sqlparser.SQLVal).Val)
			if _, ok := partitionMap[key]; !ok {
				partitionMap[key] = onePart.Backend
			} else {
				if partitionMap[key] != onePart.Backend {
					return nil, errors.New("partition.list.different.backend.with.same.values")
				}
			}
		}
	}
	return partitionMap, nil
}

// ListUniform used to uniform the list table to backends.
func (r *Router) ListUniform(table string, shardkey string, partitionDef sqlparser.PartitionDefinitions) (*config.TableConfig, error) {
	if table == "" {
		return nil, errors.New("table.cant.be.null")
	}
	if shardkey == "" {
		return nil, errors.New("shard.key.cant.be.null")
	}

	listMap, err := listMergePartition(partitionDef)
	if err != nil {
		return nil, err
	}

	nums := len(listMap)
	if nums == 0 {
		return nil, errors.New("router.compute.partition.list.is.null")
	}

	tableConf := &config.TableConfig{
		Name:       table,
		ShardType:  methodTypeList,
		ShardKey:   shardkey,
		Partitions: make([]*config.PartitionConfig, 0, 16),
	}

	i := 0
	for listValue, backend := range listMap {
		partConf := &config.PartitionConfig{
			Table:     fmt.Sprintf("%s_%04d", table, i),
			Backend:   backend,
			ListValue: listValue,
		}
		tableConf.Partitions = append(tableConf.Partitions, partConf)
		i++
	}
	return tableConf, nil
}
