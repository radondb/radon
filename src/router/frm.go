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

	"config"

	"github.com/pkg/errors"
)

const (
	TableTypeSingle    = "single"
	TableTypeGlobal    = "global"
	TableTypeUnknow    = "unknow"

	TableTypePartitionHash  = "hash"
	TableTypePartitionList  = "list"
	TableTypePartitionRange = "range"
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
	log.Warning("frm.remove.file[%v].for.[db:%s, table:%s]", db, table, file)
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
	if err := r.addDatabase(db); err != nil {
		log.Error("frm.create.addDatabase.error:%v", err)
		return err
	}
	if err := r.writeDatabaseFrmData(db); err != nil {
		log.Error("frm.writeTableFrmData[db:%v].file.error:%+v", db, err)
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

	if _, ok := r.Schemas[db]; !ok {
		return errors.Errorf("router.can.not.find.db[%v]", db)
	}
	return nil
}

// CheckTable is used to check the table exist.
func (r *Router) CheckTable(database string, tableName string) (isExist bool, err error) {
	var ok bool

	// lock
	r.mu.RLock()
	defer r.mu.RUnlock()

	if database == "" {
		return false, errors.Errorf("database.is.empty")
	}
	if tableName == "" {
		return false, errors.Errorf("tableName.is.empty")
	}

	// schema
	var schema *Schema
	if schema, ok = r.Schemas[database]; !ok {
		return false, errors.Errorf("router.can.not.find.db[%v]", database)
	}

	// table
	if _, ok = schema.Tables[tableName]; !ok {
		return false, nil
	}
	return true, nil
}

// CreateTable used to add a table to router and flush the schema to disk.
// Lock.
func (r *Router) CreateTable(db, table, shardKey string, tableType string, backends []string, extra *Extra) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	var tableConf *config.TableConfig
	log := r.log

	switch tableType {
	case TableTypeGlobal:
		if tableConf, err = r.GlobalUniform(table, backends); err != nil {
			return err
		}
	case TableTypeSingle:
		if tableConf, err = r.SingleUniform(table, backends); err != nil {
			return err
		}
	case TableTypePartitionHash:
		if tableConf, err = r.HashUniform(table, shardKey, backends); err != nil {
			return err
		}
	default:
		if tableConf, err = r.HashUniform(table, shardKey, backends); err != nil {
			return err
		}
	}

	if extra != nil {
		tableConf.AutoIncrement = extra.AutoIncrement
	}

	// add config to router.
	if err = r.addTable(db, tableConf); err != nil {
		log.Error("frm.create.add.route.error:%v", err)
		return err
	}
	if err = r.writeTableFrmData(db, table, tableConf); err != nil {
		log.Error("frm.create.table[db:%v, table:%v].file.error:%+v", db, tableConf.Name, err)
		return err
	}

	if err = config.UpdateVersion(r.metadir); err != nil {
		log.Panicf("frm.create.table.update.version.error:%v", err)
		return err
	}
	return nil
}

func (r *Router) createTable(db, table string, tableConf *config.TableConfig) error {
	var err error

	// add config to router.
	if err = r.addTable(db, tableConf); err != nil {
		log.Error("frm.create.add.route.error:%v", err)
		return err
	}
	if err = r.writeTableFrmData(db, table, tableConf); err != nil {
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
