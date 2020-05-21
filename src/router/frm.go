/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"config"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

const (
	TableTypeSingle  = "single"
	TableTypeGlobal  = "global"
	TableTypeUnknown = "unknown"

	TableTypePartitionHash  = "hash"
	TableTypePartitionList  = "list"
	TableTypePartitionRange = "range"
)

const (
	NAME_CHAR_LEN     = 64
	TABLE_NAME_SUFFIX = 5 // table name suffix: "_0032"
)

// writeTableFrmData used to write table's json schema to file.
// The file name is : [schema-dir]/[database]/[table].json.
// If the [schema-dir]/[database] directoryis not exists, we will create it first.
func (r *Router) writeTableFrmData(db string, table string, tconf *config.TableConfig) error {
	log := r.log
	dir := path.Join(r.metadir, db)
	if tconf == nil {
		return errors.New("table.config..can't.be.nil")
	}

	log.Info("frm.write.data[db:%s, table:%s, shardType:%s]", db, table, tconf.ShardType)
	file := path.Join(dir, fmt.Sprintf("%s.json", table))
	if err := config.WriteConfig(file, tconf); err != nil {
		log.Error("frm.write.to.file[%v].error:%v", file, err)
		return err
	}
	return nil
}

// removeTableFrmData used to remove table json file.
func (r *Router) removeTableFrmData(db string, table string) error {
	log := r.log
	dir := path.Join(r.metadir, db)
	file := path.Join(dir, fmt.Sprintf("%s.json", table))
	log.Warning("frm.remove.file[%v].for.[db:%s, table:%s]", file, db, table)
	return os.Remove(file)
}

// readTableFrmData used to read json file to TableConfig.
func (r *Router) readTableFrmData(file string) (*config.TableConfig, error) {
	log := r.log
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("frm.read.from.file[%v].error:%v", file, err)
		return nil, err
	}
	conf, err := config.ReadTableConfig(string(data))
	if err != nil {
		log.Error("frm.read.parse.json.file[%v].error:%v", file, err)
		return nil, err
	}
	return conf, nil
}

// loadTableFromFile used to add a table read from the json file.
func (r *Router) loadTableFromFile(db, file string) error {
	log := r.log
	log.Info("frm.load.table.from.file:%v", file)

	conf, err := r.readTableFrmData(file)
	if err != nil {
		log.Error("frm.load.table.read.file[%v].error:%+v", file, err)
		return err
	}
	if err := r.addTable(db, conf); err != nil {
		log.Error("frm.load.table.add.router[%v].error:%+v", file, err)
		return err
	}
	return nil
}

// loadTable used to add a table read from the json file.
func (r *Router) loadTable(db string, table string) error {
	log := r.log
	log.Warning("frm.load.table[db:%s, table:%s]", db, table)

	dir := path.Join(r.metadir, db)
	file := path.Join(dir, fmt.Sprintf("%s.json", table))
	return r.loadTableFromFile(db, file)
}

func (r *Router) writeDatabaseFrmData(db string) error {
	log := r.log
	dir := path.Join(r.metadir, db)
	log.Info("frm.write.database[db:%s]", db)
	// Create dir.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if x := os.MkdirAll(dir, os.ModePerm); x != nil {
			log.Error("frm.write.mkdir[%v].error:%v", dir, err)
			return x
		}
	}
	return nil
}

func (r *Router) CreateDatabase(db string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	if len(db) > NAME_CHAR_LEN {
		return sqldb.NewSQLError(sqldb.ER_TOO_LONG_IDENT, db)
	}
	if r.checkNameInvalid(db) {
		log.Error("frm.check.database.name[%v].invalid.contains.char:'/' or space ' '", db)
		return errors.Errorf("invalid.database.name.currently.not.support.dbname[%v].contains.with.char:'/' or space ' '", db)
	}
	if err := r.addDatabase(db); err != nil {
		log.Error("frm.create.addDatabase.error:%v", err)
		return err
	}
	if err := r.writeDatabaseFrmData(db); err != nil {
		log.Error("frm.writeTableFrmData[db:%v].file.error:%+v", db, err)
		// if write fail, drop db in memory cache added by addDatabase
		if err := r.dropDatabase(db); err != nil {
			log.Error("frm.drop.database[db:%v].error.from.router.cache.when.write.to.disk.failed:%+v", db, err)
		}
		return err
	}
	if err := config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("frm.create.table.update.version.error:%v", err)
		return err
	}
	return nil
}

// DropDatabase used to remove a database-schema from the schemas
// and remove all the table-schema files who belongs to this database.
func (r *Router) DropDatabase(db string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	// Drop database from route.
	if err := r.dropDatabase(db); err != nil {
		return err
	}

	// Delete database dir.
	dir := path.Join(r.metadir, db)
	log.Info("frm.drop.database.file[%v]", dir)
	if err := os.RemoveAll(dir); err != nil {
		r.log.Error("frm.drop.database[%v].error:%v", dir, err)
		return err
	}

	// Update version.
	if err := config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("frm.drop.database.update.version.error:%v", err)
		return err
	}
	return nil
}

// CheckDatabase is used to check the Database exist.
func (r *Router) CheckDatabase(db string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.checkDatabase(db)
}

// checkDatabase is used to check the database exists or not without lock,
// only used in internal of router.
func (r *Router) checkDatabase(db string) error {
	if db == "" {
		return sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}

	if _, ok := r.Schemas[db]; !ok {
		return sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, db)
	}
	return nil
}

// CheckTable is used to check the table exist.
func (r *Router) CheckTable(database string, tableName string) (isExist bool, err error) {
	var ok bool

	// lock
	r.mu.RLock()
	defer r.mu.RUnlock()

	// check database exists or not
	if err := r.checkDatabase(database); err != nil {
		return false, err
	}
	if tableName == "" {
		return false, errors.Errorf("tableName.is.empty")
	}

	// schema
	schema, _ := r.Schemas[database]

	// table
	if _, ok = schema.Tables[tableName]; !ok {
		return false, nil
	}
	return true, nil
}

// CreateNonPartTable used to add a non-partitioned table to router and flush the schema to disk.
// Lock.
func (r *Router) CreateNonPartTable(db, table, tableType string, backends []string, extra *Extra) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	var tableConf *config.TableConfig

	switch tableType {
	case TableTypeGlobal:
		if tableConf, err = r.GlobalUniform(table, backends); err != nil {
			return err
		}
	case TableTypeSingle:
		if tableConf, err = r.SingleUniform(table, backends); err != nil {
			return err
		}
	default:
		err := errors.Errorf("tableType is unsupported: %s", tableType)
		return err
	}

	if extra != nil {
		tableConf.AutoIncrement = extra.AutoIncrement
	}

	return r.createTable(db, table, tableConf)
}

// CreateHashTable used to add a hash table to router and flush the schema to disk.
func (r *Router) CreateHashTable(db, table, shardKey string, tableType string, backends []string, partitionNum *sqlparser.SQLVal, extra *Extra) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	var tableConf *config.TableConfig

	switch tableType {
	case TableTypePartitionHash:
		if tableConf, err = r.HashUniform(table, shardKey, backends, partitionNum); err != nil {
			return err
		}
	default:
		err := errors.Errorf("tableType is unsupported: %s", tableType)
		return err
	}

	if extra != nil {
		tableConf.AutoIncrement = extra.AutoIncrement
	}

	return r.createTable(db, table, tableConf)
}

// CreateListTable used to add a list table to router and flush the schema to disk.
func (r *Router) CreateListTable(db, table, shardKey string, tableType string,
	partitionDef sqlparser.PartitionOptions, extra *Extra) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	var tableConf *config.TableConfig

	switch tableType {
	case TableTypePartitionList:
		if tableConf, err = r.ListUniform(table, shardKey, partitionDef); err != nil {
			return err
		}

	default:
		err := errors.Errorf("tableType is unsupported: %s", tableType)
		return err
	}

	if extra != nil {
		tableConf.AutoIncrement = extra.AutoIncrement
	}

	return r.createTable(db, table, tableConf)
}

// checkNameInvalid used to check if db or table name contains invalid char '/'.
func (r *Router) checkNameInvalid(name string) bool {
	// 1. Currently radon don`t support db/table name like `a/a`, in MySQL, `/` will be converted to `@002f`
	// by func strconvert(), e.g.:`a/a` will first converted to `a@002fa` and then write to disk.
	// see: https://github.com/mysql/mysql-server/blob/5.7/sql/sql_table.cc#L518
	// 2. For space ' ', if last_char_is_space, mysql don`t support, otherwise,
	// space will be changed to `@0020f`, we both don`t support in radon.
	// see func check_table_name(): https://github.com/mysql/mysql-server/blob/5.7/sql/table.cc#L4284
	return strings.ContainsAny(name, " /")
}

func (r *Router) createTable(db, table string, tableConf *config.TableConfig) error {
	var err error
	log := r.log

	// see func in mysql sql/table.cc: check_and_convert_db_name() and check_table_name()
	if (len(table) + TABLE_NAME_SUFFIX) > NAME_CHAR_LEN {
		return sqldb.NewSQLError(sqldb.ER_TOO_LONG_IDENT, table)
	}
	if r.checkNameInvalid(table) {
		log.Error("frm.check.table.name[%v].invalid.contains.char:'/' or space ' '", table)
		return errors.Errorf("invalid.table.name.currently.not.support.tablename[%v].contains.with.char:'/' or space ' '", table)
	}

	// add config to router.
	if err = r.addTable(db, tableConf); err != nil {
		log.Error("frm.create.add.route.error:%v", err)
		return err
	}
	if err = r.writeTableFrmData(db, table, tableConf); err != nil {
		// clear db/table cache in memory
		r.removeTable(db, table)
		log.Error("frm.create.table[db:%v, table:%v].file.error:%+v", db, tableConf.Name, err)
		return err
	}

	if err = config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("frm.create.table.update.version.error:%v", err)
		return err
	}
	return nil
}

// DropTable used to remove a table from router and remove the schema file from disk.
func (r *Router) DropTable(db, table string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	if err := r.removeTable(db, table); err != nil {
		log.Error("frm.drop.table[%s.%s].remove.route.error:%v", db, table, err)
		return err
	}
	if err := r.removeTableFrmData(db, table); err != nil {
		log.Error("frm.drop.table[%s.%s].remove.frmdata.error:%v", db, table, err)
		return err
	}

	if err := config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("frm.drop.table.update.version.error:%v", err)
		return err
	}
	return nil
}

func (r *Router) dropTable(db, table string) error {
	log := r.log
	if err := r.removeTable(db, table); err != nil {
		log.Error("frm.drop.table[%s.%s].remove.route.error:%v", db, table, err)
		return err
	}
	if err := r.removeTableFrmData(db, table); err != nil {
		log.Error("frm.drop.table[%s.%s].remove.frmdata.error:%v", db, table, err)
		return err
	}

	if err := config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("frm.drop.table.update.version.error:%v", err)
		return err
	}
	return nil
}

// RenameTable used to rename a table from router and update the schema file on disk.
func (r *Router) RenameTable(db, fromTable, toTable string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	var tableConfig *config.TableConfig
	tableConfig, err := r.getRenameTableConfig(db, fromTable, toTable)
	if err != nil {
		log.Error("frm.get.rename.table.config[%s.%s->%s].error:%v", db, fromTable, toTable, err)
		return err
	}

	if err := r.createTable(db, toTable, tableConfig); err != nil {
		log.Error("frm.create.table[db:%v, table:%v].file.error:%+v", db, toTable, err)
		return err
	}

	if err := r.dropTable(db, fromTable); err != nil {
		log.Error("frm.dropTable.table[%s.%s].error:%v", db, fromTable, err)
		return err
	}
	return nil
}

// RefreshTable used to re-update the table from file.
// Lock.
func (r *Router) RefreshTable(db, table string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	if err := r.removeTable(db, table); err != nil {
		log.Error("frm.refresh.table[%s.%s].remove.route.error:%v", db, table, err)
		return err
	}
	if err := r.loadTable(db, table); err != nil {
		log.Error("frm.refresh.table[%s.%s].load.table.error:%v", db, table, err)
		return err
	}
	return nil
}

// LoadConfig used to load all schemas stored in metadir.
// When an IO error occurs during the file reading, panic me.
func (r *Router) LoadConfig() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	// Clear the router first.
	r.clear()

	// Check the schemadir, create it if not exists.
	if _, err := os.Stat(r.metadir); os.IsNotExist(err) {
		if x := os.MkdirAll(r.metadir, os.ModePerm); x != nil {
			log.Error("router.load.create.dir[%v].error:%v", r.metadir, x)
			return x
		}
		return nil
	}

	frms := make(map[string][]string)
	files, err := ioutil.ReadDir(r.metadir)
	if err != nil {
		log.Error("router.load.readdir[%v].error:%v", r.metadir, err)
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			dbName := f.Name()
			jsons := []string{}
			subdir := path.Join(r.metadir, dbName)
			subFiles, err := ioutil.ReadDir(subdir)
			if err != nil {
				log.Error("router.load.readsubdir[%v].error:%v", subdir, err)
				return err
			}
			for _, subFile := range subFiles {
				if !subFile.IsDir() {
					jsons = append(jsons, path.Join(subdir, subFile.Name()))
				}
			}
			frms[dbName] = jsons

			// Add database to router.
			if err := r.addDatabase(dbName); err != nil {
				return err
			}
		}
	}

	for k, v := range frms {
		for _, file := range v {
			if err := r.loadTableFromFile(k, file); err != nil {
				log.Error("router.load.table..from.file[%v].error:%+v", file, err)
				return err
			}
		}
	}
	return nil
}

// AddForTest used to add table config for test.
func (r *Router) AddForTest(db string, confs ...*config.TableConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log := r.log
	// add config to router.
	for _, conf := range confs {
		if err := r.addTable(db, conf); err != nil {
			log.Error("frm.for.test.addroute.error:%v", err)
			return err
		}
	}
	return nil
}
