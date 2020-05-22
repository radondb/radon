/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"encoding/json"
	"strings"
	"sync"

	"config"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Extra -- router extra params.
type Extra struct {
	AutoIncrement *config.AutoIncrement
}

// Table tuple.
type Table struct {
	// Table name
	Name string `json:",omitempty"`
	// Shard key
	ShardKey string `json:",omitempty"`
	// partition method
	Partition Partition `json:",omitempty"`
	// table config.
	TableConfig *config.TableConfig `json:"-"`
}

// Schema tuple.
type Schema struct {
	// database name
	DB string `json:",omitempty"`
	// tables map, key is table name
	Tables map[string]*Table `json:",omitempty"`
}

// Router tuple.
type Router struct {
	log     *xlog.Log
	mu      sync.RWMutex
	metadir string
	dbACL   *DatabaseACL
	conf    *config.RouterConfig

	// schemas map, key is database name
	Schemas map[string]*Schema `json:",omitempty"`
}

// NewRouter creates the new router.
func NewRouter(log *xlog.Log, metadir string, conf *config.RouterConfig) *Router {
	route := &Router{
		log:     log,
		metadir: metadir,
		conf:    conf,
		dbACL:   NewDatabaseACL(),
		Schemas: make(map[string]*Schema),
	}
	return route
}

// addTable -- used to add a table router to schema map.
func (r *Router) addTable(db string, tbl *config.TableConfig) error {
	var table *Table

	// check db exists or not
	if err := r.checkDatabase(db); err != nil {
		return err
	}
	if tbl == nil {
		return errors.New("table.config..can't.be.nil")
	}

	// get schema
	schema, _ := r.Schemas[db]

	// table
	if _, ok := schema.Tables[tbl.Name]; !ok {
		table = &Table{
			Name:        tbl.Name,
			ShardKey:    tbl.ShardKey,
			TableConfig: tbl,
		}
		schema.Tables[tbl.Name] = table
	} else {
		return errors.Errorf("router.add.db[%v].table[%v].exists", db, tbl.Name)
	}

	// methods
	switch tbl.ShardType {
	case methodTypeHash:
		slots := tbl.Slots
		if slots == 0 {
			slots = r.conf.Slots
		}
		hash := NewHash(r.log, slots, tbl)
		if err := hash.Build(); err != nil {
			return err
		}
		table.Partition = hash
	case methodTypeGlobal:
		global := NewGlobal(r.log, tbl)
		if err := global.Build(); err != nil {
			return err
		}
		table.Partition = global
	case methodTypeSingle:
		single := NewSingle(r.log, tbl)
		if err := single.Build(); err != nil {
			return err
		}
		table.Partition = single
	case methodTypeList:
		list := NewList(r.log, tbl)
		if err := list.Build(); err != nil {
			return err
		}
		table.Partition = list
	default:
		return errors.Errorf("router.unsupport.shardtype:[%v]", tbl.ShardType)
	}
	return nil
}

// removeTable -- used to remove a table router from schema map.
func (r *Router) removeTable(db string, table string) error {
	var ok bool
	var schema *Schema

	// schema
	if schema, ok = r.Schemas[db]; !ok {
		return errors.Errorf("router.can.not.find.db[%v]", db)
	}
	// table
	if _, ok = schema.Tables[table]; !ok {
		return errors.Errorf("router.can.not.find.table[%v]", table)
	}
	// remove
	delete(schema.Tables, table)
	return nil
}

// getRenameTableConfig -- used to rename table config and return the new tableconfig.
func (r *Router) getRenameTableConfig(db, fromTable, toTable string) (*config.TableConfig, error) {
	var ok bool
	var schema *Schema

	if schema, ok = r.Schemas[db]; !ok {
		return nil, errors.Errorf("router.can.not.find.db[%v]", db)
	}
	if _, ok = schema.Tables[fromTable]; !ok {
		return nil, errors.Errorf("router.can.not.find.table[%v]", fromTable)
	}
	if _, ok = schema.Tables[toTable]; ok {
		return nil, errors.Errorf("router.find.table[%v].exists", toTable)
	}

	table := schema.Tables[fromTable]
	tableConfig := table.TableConfig
	tableConfig.Name = toTable
	for _, partition := range tableConfig.Partitions {
		partition.Table = strings.Replace(partition.Table, fromTable, toTable, 1)
	}
	return tableConfig, nil
}

func (r *Router) addDatabase(db string) error {
	if db == "" {
		return errors.Errorf("router.database.should.not.be.empty")
	}
	if _, ok := r.Schemas[db]; !ok {
		schema := &Schema{DB: db, Tables: make(map[string]*Table)}
		r.Schemas[db] = schema
		return nil
	}
	return errors.Errorf("router.database.exists")
}

func (r *Router) dropDatabase(db string) error {
	if _, ok := r.Schemas[db]; !ok {
		return errors.Errorf("router.can.not.find.db[%v]", db)
	}
	delete(r.Schemas, db)
	return nil
}

// clear used to reset Schemas to new.
func (r *Router) clear() {
	r.Schemas = make(map[string]*Schema)
}

// DatabaseACL used to check whether the database is a system database.
func (r *Router) DatabaseACL(database string) error {
	if ok := r.dbACL.Allow(database); !ok {
		r.log.Warning("router.database.acl.check.fail[db:%s]", database)
		return sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking privileges for database %s", database)
	}
	return nil
}

// IsSystemDB used to check whether the database is a system database.
func (r *Router) IsSystemDB(database string) bool {
	return r.dbACL.IsSystemDB(database)
}

// IsPartitionHash used to check whether the partitionType is hash.
func (r *Router) IsPartitionHash(partitionType MethodType) bool {
	return partitionType == methodTypeHash
}

func (r *Router) getTable(database string, tableName string) (*Table, error) {
	var ok bool
	var schema *Schema
	var table *Table

	// lock
	r.mu.RLock()
	defer r.mu.RUnlock()

	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	if tableName == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, tableName)
	}

	// schema
	if schema, ok = r.Schemas[database]; !ok {
		r.log.Error("router.can.not.find.db[%v]", database)
		return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, database+"."+tableName)
	}

	// table
	if table, ok = schema.Tables[tableName]; !ok {
		r.log.Error("router.can.not.find.table[%v]", tableName)
		return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, tableName)
	}
	return table, nil
}

// ShardKey used to lookup shardkey from given database and table name.
func (r *Router) ShardKey(database string, tableName string) (string, error) {
	table, err := r.getTable(database, tableName)
	if err != nil {
		return "", err
	}
	return table.ShardKey, nil
}

// PartitionType used to get PartitionType from given database and table name.
func (r *Router) PartitionType(database string, tableName string) (MethodType, error) {
	table, err := r.getTable(database, tableName)
	if err != nil {
		return "", err
	}
	return table.Partition.Type(), nil
}

// TableConfig returns the config by database and tableName.
func (r *Router) TableConfig(database string, tableName string) (*config.TableConfig, error) {
	table, err := r.getTable(database, tableName)
	if err != nil {
		return nil, err
	}
	return table.TableConfig, nil
}

// Lookup used to lookup a router(partition table name and backend) through db&table
func (r *Router) Lookup(database string, tableName string, startKey *sqlparser.SQLVal, endKey *sqlparser.SQLVal) ([]Segment, error) {
	var ok bool
	var err error
	var schema *Schema
	var table *Table

	// lock
	r.mu.RLock()
	defer r.mu.RUnlock()

	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	if tableName == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, tableName)
	}

	// schema
	if schema, ok = r.Schemas[database]; !ok {
		r.log.Error("router.can.not.find.db[%v]", database)
		return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
	}

	// table
	if table, ok = schema.Tables[tableName]; !ok {
		r.log.Error("router.can.not.find.table[%v]", tableName)
		return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, tableName)
	}

	// router info
	partInfos, err := table.Partition.Lookup(startKey, endKey)
	if err != nil {
		r.log.Error("router.partition.lookup.error:%+v", err)
		return nil, err
	}
	return partInfos, nil
}

// Tables returns all the tables.
func (r *Router) Tables() map[string][]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make(map[string][]string)
	for _, schema := range r.Schemas {
		db := schema.DB
		tables := make([]string, 0, 16)
		for _, table := range schema.Tables {
			tables = append(tables, table.Name)
		}
		list[db] = tables
	}
	return list
}

// JSON returns the info of router.
func (r *Router) JSON() string {
	bout, err := json.MarshalIndent(r, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(bout)
}

// GetIndex returns index based on sqlval.
func (r *Router) GetIndex(database, tableName string, sqlval *sqlparser.SQLVal) (int, error) {
	table, err := r.getTable(database, tableName)
	if err != nil {
		return -1, err
	}

	index, err := table.Partition.GetIndex(sqlval)
	if err != nil {
		r.log.Error("router.partition.getindex.error:%+v", err)
		return -1, err
	}
	return index, nil
}

// GetSegments returns Segments based on indexes.
func (r *Router) GetSegments(database, tableName string, indexes []int) ([]Segment, error) {
	table, err := r.getTable(database, tableName)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return table.Partition.GetSegments(), nil
	}

	var segs []Segment
	for _, idx := range indexes {
		segment, err := table.Partition.GetSegment(idx)
		if err != nil {
			return nil, err
		}

		isRepeat := false
		for _, seg := range segs {
			if seg.Range == segment.Range {
				isRepeat = true
				break
			}
		}
		if !isRepeat {
			segs = append(segs, segment)
		}
	}
	return segs, nil
}
