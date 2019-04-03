/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"strings"

	"plugins/autoincrement"
	"router"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var (
	supportEngines = []string{
		"innodb",
		"tokudb",
	}
)

func checkEngine(ddl *sqlparser.DDL) {
	check := false
	engine := ddl.TableSpec.Options.Engine
	for _, eng := range supportEngines {
		if eng == strings.ToLower(engine) {
			check = true
			break
		}
	}

	// Change the storage engine to InnoDB.
	if !check {
		ddl.TableSpec.Options.Engine = "InnoDB"
	}
}

func tryGetShardKey(ddl *sqlparser.DDL) (string, error) {
	shardKey := ddl.PartitionName
	table := ddl.Table.Name.String()

	if "dual" == table {
		return "", fmt.Errorf("spanner.ddl.check.create.table[%s].error:not support", table)
	}

	if shardKey != "" {
		shardKeyOK := false
		constraintCheckOK := true
		// shardKey check and constraint check in column definition
		for _, col := range ddl.TableSpec.Columns {
			colName := col.Name.String()
			if colName == shardKey {
				shardKeyOK = true
			} else {
				switch col.Type.KeyOpt {
				case sqlparser.ColKeyUnique, sqlparser.ColKeyUniqueKey, sqlparser.ColKeyPrimary, sqlparser.ColKey:
					constraintCheckOK = false
				}
			}
		}

		if !shardKeyOK {
			return "", fmt.Errorf("Sharding Key column '%s' doesn't exist in table", shardKey)
		}
		if !constraintCheckOK {
			return "", fmt.Errorf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
		}

		// constraint check in index definition
		for _, index := range ddl.TableSpec.Indexes {
			constraintCheckOK = false
			info := index.Info
			if info.Unique || info.Primary {
				for _, colIdx := range index.Columns {
					colName := colIdx.Column.String()
					if colName == shardKey {
						constraintCheckOK = true
						break
					}
				}
				if !constraintCheckOK {
					return "", fmt.Errorf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
				}
			}
		}
		return shardKey, nil
	} else {
		for _, col := range ddl.TableSpec.Columns {
			colName := col.Name.String()
			switch col.Type.KeyOpt {
			case sqlparser.ColKeyUnique, sqlparser.ColKeyUniqueKey, sqlparser.ColKeyPrimary, sqlparser.ColKey:
				return colName, nil
			}
		}
		// constraint check in index definition.
		for _, index := range ddl.TableSpec.Indexes {
			info := index.Info
			if info.Unique || info.Primary {
				if len(index.Columns) == 1 {
					return index.Columns[0].Column.String(), nil
				}
			}
		}
	}
	return "", fmt.Errorf("The unique/primary constraint shoule be defined or add 'PARTITION BY HASH' to mandatory indication")
}

func checkDatabaseExists(database string, router *router.Router) bool {
	tblList := router.Tables()
	_, ok := tblList[database]
	return ok
}

func checkTableExists(database string, table string, router *router.Router) bool {
	tblList := router.Tables()
	tables, ok := tblList[database]
	if !ok {
		return false
	}
	for _, t := range tables {
		if t == table {
			return true
		}
	}
	return false
}

// handleDDL used to handle the DDL command.
// Here we need to deal with database.table grammar.
// Supports:
// 1. CREATE/DROP DATABASE
// 2. CREATE/DROP TABLE ... PARTITION BY HASH(shardkey)
// 3. CREATE/DROP INDEX ON TABLE(columns...)
// 4. ALTER TABLE .. ENGINE=xx
// 5. ALTER TABLE .. ADD COLUMN (column definition)
// 6. ALTER TABLE .. MODIFY COLUMN column definition
// 7. ALTER TABLE .. DROP COLUMN column
func (spanner *Spanner) handleDDL(session *driver.Session, query string, node *sqlparser.DDL) (*sqltypes.Result, error) {
	log := spanner.log
	route := spanner.router
	scatter := spanner.scatter

	ddl := node
	database := session.Schema()
	// Database operation.
	if !ddl.Database.IsEmpty() {
		database = ddl.Database.String()
	}

	// Table operation.
	// when Drop Table, maybe multiple tables, can't use the ddl.Table.Qualifier.
	if ddl.Action != sqlparser.DropTableStr && !ddl.Table.Qualifier.IsEmpty() {
		database = ddl.Table.Qualifier.String()
	}

	var databases []string
	if ddl.Action == sqlparser.DropTableStr {
		for _, tableIdent := range ddl.Tables {
			if !tableIdent.Qualifier.IsEmpty() {
				databases = append(databases, ddl.Table.Qualifier.String())
			}
		}
	}
	databases = append(databases, database)

	for _, db := range databases {
		// Check the database ACL.
		if err := route.DatabaseACL(db); err != nil {
			return nil, err
		}
		// Check the database privilege.
		privilegePlug := spanner.plugins.PlugPrivilege()
		if err := privilegePlug.Check(db, session.User(), node); err != nil {
			return nil, err
		}
	}

	switch ddl.Action {
	case sqlparser.CreateDBStr:
		if node.IfNotExists && checkDatabaseExists(database, route) {
			return &sqltypes.Result{}, nil
		}
		if err := route.CreateDatabase(database); err != nil {
			return nil, err
		}
		return spanner.ExecuteScatter(query)
	case sqlparser.DropDBStr:
		if node.IfExists && !checkDatabaseExists(database, route) {
			return &sqltypes.Result{}, nil
		}
		// Execute the ddl.
		qr, err := spanner.ExecuteScatter(query)
		if err != nil {
			return nil, err
		}
		// Drop database from router.
		if err := route.DropDatabase(database); err != nil {
			return nil, err
		}
		return qr, nil
	case sqlparser.CreateTableStr:
		var err error
		table := ddl.Table.Name.String()
		backends := scatter.Backends()
		shardKey := ddl.PartitionName
		tableType := router.TableTypeUnknow

		if !checkDatabaseExists(database, route) {
			return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
		}

		// Check table exists.
		if node.IfNotExists && checkTableExists(database, table, route) {
			return &sqltypes.Result{}, nil
		}

		// Check engine.
		checkEngine(ddl)

		switch ddl.TableSpec.Options.Type {
		case sqlparser.PartitionTableType, sqlparser.NormalTableType:
			if shardKey, err = tryGetShardKey(ddl); err != nil {
				return nil, err
			}
			tableType = router.TableTypePartition
		case sqlparser.GlobalTableType:
			tableType = router.TableTypeGlobal
		case sqlparser.SingleTableType:
			tableType = router.TableTypeSingle
		}

		autoinc, err := autoincrement.GetAutoIncrement(node)
		if err != nil {
			return nil, err
		}
		extra := &router.Extra{
			AutoIncrement: autoinc,
		}
		if err := route.CreateTable(database, table, shardKey, tableType, backends, extra); err != nil {
			return nil, err
		}
		r, err := spanner.ExecuteDDL(session, database, sqlparser.String(ddl), node)
		if err != nil {
			// Try to drop table.
			route.DropTable(database, table)
			return nil, err
		}
		return r, nil
	case sqlparser.DropTableStr:
		r := &sqltypes.Result{}
		tables := ddl.Tables
		for _, tableIdent := range tables {
			node.Table = tableIdent
			table := tableIdent.Name.String()
			db := database
			// The query need differentiate, ddl_plan will define database.
			var query string
			if tableIdent.Qualifier.IsEmpty() {
				query = fmt.Sprintf("drop table %s", table)
			} else {
				//If the tableIdent with Qualifier, us it as db, or else use the default.
				db = tableIdent.Qualifier.String()
				query = fmt.Sprintf("drop table %s.%s", db, table)
			}

			// Check the database and table is exists.
			if !checkDatabaseExists(db, route) {
				return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, db)
			}

			// Check table exists.
			if node.IfExists && !checkTableExists(db, table, route) {
				return &sqltypes.Result{}, nil
			}

			// Execute.
			r, err := spanner.ExecuteDDL(session, db, query, node)
			if err != nil {
				log.Error("spanner.ddl.execute[%v].error[%+v]", query, err)
			}
			if err := route.DropTable(db, table); err != nil {
				log.Error("spanner.ddl.router.drop.table[%s].error[%+v]", table, err)
			}

			if err != nil {
				return r, err
			}
		}
		return r, nil
	case sqlparser.CreateIndexStr, sqlparser.DropIndexStr,
		sqlparser.AlterEngineStr, sqlparser.AlterCharsetStr,
		sqlparser.AlterAddColumnStr, sqlparser.AlterDropColumnStr, sqlparser.AlterModifyColumnStr,
		sqlparser.TruncateTableStr:

		// Check the database and table is exists.
		if !checkDatabaseExists(database, route) {
			return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
		}

		table := ddl.Table.Name.String()
		if !checkTableExists(database, table, route) {
			return nil, sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, table)
		}
		// Execute.
		r, err := spanner.ExecuteDDL(session, database, query, node)
		if err != nil {
			log.Error("spanner.ddl[%v].error[%+v]", query, err)
		}
		return r, err
	default:
		log.Error("spanner.ddl[%v, %+v].access.denied", query, node)
		return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; you don't have the privilege for %v operation", ddl.Action)
	}
}
