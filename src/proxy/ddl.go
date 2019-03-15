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

	"plugins"
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

// CheckCreateTable used to check the CRERATE TABLE statement.
func CheckCreateTable(ddl *sqlparser.DDL) error {
	shardKey := ddl.PartitionName
	table := ddl.Table.Name.String()

	if "dual" == table {
		return fmt.Errorf("spanner.ddl.check.create.table[%s].error:not support", table)
	}

	// when shardtype is HASH,check shard key and UNIQUE/PRIMARY KEY constraint.
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
			return fmt.Errorf("Sharding Key column '%s' doesn't exist in table", shardKey)
		}
		if !constraintCheckOK {
			return fmt.Errorf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
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
					return fmt.Errorf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
				}
			}
		}
	}

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
	return nil
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
	if !ddl.Table.Qualifier.IsEmpty() {
		database = ddl.Table.Qualifier.String()
	}

	// Check the database ACL.
	if err := route.DatabaseACL(database); err != nil {
		return nil, err
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
		table := ddl.Table.Name.String()
		backends := scatter.Backends()
		shardKey := ddl.PartitionName

		if !checkDatabaseExists(database, route) {
			return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
		}

		// Check table exists.
		if node.IfNotExists && checkTableExists(database, table, route) {
			return &sqltypes.Result{}, nil
		}

		// Check the table and change the engine.
		if err := CheckCreateTable(ddl); err != nil {
			log.Error("spanner.ddl.check.create.table[%s].error:%+v", table, err)
			return nil, err
		}

		// Create table.
		extra := &router.Extra{
			AutoIncrement: plugins.GetAutoIncrement(node),
		}
		if err := route.CreateTable(database, table, shardKey, backends, extra); err != nil {
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
		// Check the database and table is exists.
		table := ddl.Table.Name.String()

		if !checkDatabaseExists(database, route) {
			return nil, sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, database)
		}

		// Check table exists.
		if node.IfExists && !checkTableExists(database, table, route) {
			return &sqltypes.Result{}, nil
		}

		// Execute.
		r, err := spanner.ExecuteDDL(session, database, query, node)
		if err != nil {
			log.Error("spanner.ddl.execute[%v].error[%+v]", query, err)
		}
		if err := route.DropTable(database, table); err != nil {
			log.Error("spanner.ddl.router.drop.table[%s].error[%+v]", table, err)
		}
		return r, err
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
