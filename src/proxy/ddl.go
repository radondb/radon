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
	"router"
	"strings"

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
	// Check the sharding key.
	if shardKey == "" {
		return fmt.Errorf("create table must end with 'PARTITION BY HASH(shard-key)'")
	}

	if "dual" == table {
		return fmt.Errorf("spanner.ddl.check.create.table[%s].error:not surpport", table)
	}

	// UNIQUE/PRIMARY constraint check.
	shardKeyOK := false
	for _, col := range ddl.TableSpec.Columns {
		colName := col.Name.String()
		if colName == shardKey {
			shardKeyOK = true
		} else {
			switch col.Type.KeyOpt {
			case sqlparser.ColKeyUnique, sqlparser.ColKeyUniqueKey, sqlparser.ColKeyPrimary:
				return fmt.Errorf("The unique/primary constraint only be defined on the sharding key column[%s] not [%s]", shardKey, colName)
			}
		}
	}
	if !shardKeyOK {
		return fmt.Errorf("Sharding Key column '%s' doesn't exist in table", shardKey)
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

func checkDatabaseAndTable(database string, table string, router *router.Router) error {
	tblList := router.Tables()
	tables, ok := tblList[database]
	if !ok {
		return sqldb.NewSQLError(sqldb.ER_BAD_DB_ERROR, "", database)
	}
	found := false
	for _, t := range tables {
		if t == table {
			found = true
			break
		}
	}
	if !found {
		return sqldb.NewSQLError(sqldb.ER_NO_SUCH_TABLE, "", table)
	}
	return nil
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
func (spanner *Spanner) handleDDL(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	router := spanner.router
	scatter := spanner.scatter

	ddl := node.(*sqlparser.DDL)
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
	if err := router.DatabaseACL(database); err != nil {
		return nil, err
	}
	switch ddl.Action {
	case sqlparser.CreateDBStr:
		return spanner.ExecuteScatter(query)
	case sqlparser.DropDBStr:
		// Execute the ddl.
		qr, err := spanner.ExecuteScatter(query)
		if err != nil {
			return nil, err
		}
		// Drop database from router.
		if err := router.DropDatabase(database); err != nil {
			return nil, err
		}
		return qr, nil
	case sqlparser.CreateTableStr:
		table := ddl.Table.Name.String()
		backends := scatter.Backends()
		shardKey := ddl.PartitionName

		// Check the table and change the engine.
		if err := CheckCreateTable(ddl); err != nil {
			log.Error("spanner.ddl.check.create.table[%s].error:%+v", table, err)
			return nil, err
		}

		// Create table.
		if err := router.CreateTable(database, table, shardKey, backends); err != nil {
			return nil, err
		}
		r, err := spanner.ExecuteDDL(session, database, sqlparser.String(ddl), node)
		if err != nil {
			// Try to drop table.
			router.DropTable(database, table)
			return nil, err
		}
		return r, nil
	case sqlparser.DropTableStr:
		// Check the database and table is exists.
		table := ddl.Table.Name.String()
		if err := checkDatabaseAndTable(database, table, router); err != nil {
			return nil, err
		}

		// Execute.
		r, err := spanner.ExecuteDDL(session, database, query, node)
		if err != nil {
			log.Error("spanner.ddl.execute[%v].error[%+v]", query, err)
		}
		if err := router.DropTable(database, table); err != nil {
			log.Error("spanner.ddl.router.drop.table[%s].error[%+v]", table, err)
		}
		return r, err
	case sqlparser.CreateIndexStr, sqlparser.DropIndexStr,
		sqlparser.AlterEngineStr, sqlparser.AlterCharsetStr,
		sqlparser.AlterAddColumnStr, sqlparser.AlterDropColumnStr, sqlparser.AlterModifyColumnStr,
		sqlparser.TruncateTableStr:

		// Check the database and table is exists.
		table := ddl.Table.Name.String()
		if err := checkDatabaseAndTable(database, table, router); err != nil {
			return nil, err
		}

		// Execute.
		r, err := spanner.ExecuteDDL(session, database, query, node)
		if err != nil {
			log.Error("spanner.ddl[%v].error[%+v]", query, err)
		}
		return r, err
	default:
		log.Error("spanner.ddl[%v, %+v].access.denied", query, node)
		return nil, sqldb.NewSQLError(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; you don't have the privilege for %v operation", ddl.Action)
	}
}
