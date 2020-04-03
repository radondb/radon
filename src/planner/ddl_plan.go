/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"router"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &DDLPlan{}
)

// DDLPlan represents a CREATE, ALTER, DROP or RENAME plan
type DDLPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// ddl ast
	node *sqlparser.DDL

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	// mode
	ReqMode xcontext.RequestMode

	// query and backend tuple
	Querys []xcontext.QueryTuple
}

// NewDDLPlan used to create DDLPlan
func NewDDLPlan(log *xlog.Log, database string, query string, node *sqlparser.DDL, router *router.Router) *DDLPlan {
	return &DDLPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeDDL,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
	}
}

// checkUnsupportedOperations used to check whether we do unsupported operations when shardtype is HASH/LIST.
func (p *DDLPlan) checkUnsupportedOperations(database, table string) error {
	node := p.node
	// Get the shard key.
	shardKey, err := p.router.ShardKey(database, table)
	if err != nil {
		return err
	}
	// Unsupported operations check when shardtype is HASH/LIST.
	if shardKey != "" {
		switch node.Action {
		case sqlparser.AlterDropColumnStr:
			if shardKey == node.DropColumnName {
				return errors.New("unsupported: cannot.drop.the.column.on.shard.key")
			}
		case sqlparser.AlterModifyColumnStr:
			if shardKey == node.ModifyColumnDef.Name.String() {
				return errors.New("unsupported: cannot.modify.the.column.on.shard.key")
			}
			// constraint check in column definition
			if node.ModifyColumnDef.Type.PrimaryKeyOpt == sqlparser.ColKeyPrimary ||
				node.ModifyColumnDef.Type.UniqueKeyOpt == sqlparser.ColKeyUniqueKey {
				err := fmt.Sprintf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
				return errors.New(err)
			}
		case sqlparser.AlterAddColumnStr:
			// constraint check in column definition
			for _, col := range node.TableSpec.Columns {
				if col.Type.PrimaryKeyOpt == sqlparser.ColKeyPrimary ||
					col.Type.UniqueKeyOpt == sqlparser.ColKeyUniqueKey {
					err := fmt.Sprintf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
					return errors.New(err)
				}
			}
			// constraint check in index definition
			for _, index := range node.TableSpec.Indexes {
				if index.Unique || index.Primary {
					err := fmt.Sprintf("The unique/primary constraint should be only defined on the sharding key column[%s]", shardKey)
					return errors.New(err)
				}
			}
		}
	}
	return nil
}

// commonImpl used to build distributed querys for create/alter.
func (p *DDLPlan) commonImpl() error {
	oldNode := p.node
	oldTable := oldNode.Table.Name.String()
	database := p.database
	if !oldNode.Table.Qualifier.IsEmpty() {
		database = oldNode.Table.Qualifier.String()
	}
	if err := p.checkUnsupportedOperations(database, oldTable); err != nil {
		return err
	}

	segments, err := p.router.Lookup(database, oldTable, nil, nil)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		// Rewrite ddl ast, replace oldTable to segment table(new table) and format a new query
		newNode := *oldNode
		newTable := segment.Table
		buf := sqlparser.NewTrackedBuffer(nil)
		newNode.Table = sqlparser.TableName{
			Name:      sqlparser.NewTableIdent(newTable),
			Qualifier: sqlparser.NewTableIdent(database),
		}
		newNode.NewName = newNode.Table
		newNode.Format(buf)
		newQuery := buf.String()

		tuple := xcontext.QueryTuple{
			Query:   newQuery,
			Backend: segment.Backend,
			Range:   segment.Range.String(),
		}
		p.Querys = append(p.Querys, tuple)
	}
	return nil
}

// dropTblImpl used to build distributed querys for: drop table t1
func (p *DDLPlan) dropTblImpl() error {
	oldNode := p.node
	oldTable := oldNode.Table.Name.String()
	database := p.database
	if !oldNode.Table.Qualifier.IsEmpty() {
		database = oldNode.Table.Qualifier.String()
	}

	segments, err := p.router.Lookup(database, oldTable, nil, nil)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		// Rewrite ddl ast, replace oldTable to segment table(new table) and format a new query
		newNode := *oldNode
		newTable := segment.Table
		buf := sqlparser.NewTrackedBuffer(nil)
		// Now we just drop a table once a time, here can be optimized with proxy/ddl.go in the future
		newNode.Tables = sqlparser.TableNames{
			sqlparser.TableName{
				Name:      sqlparser.NewTableIdent(newTable),
				Qualifier: sqlparser.NewTableIdent(database),
			},
		}
		newNode.Format(buf)
		newQuery := buf.String()

		tuple := xcontext.QueryTuple{
			Query:   newQuery,
			Backend: segment.Backend,
			Range:   segment.Range.String(),
		}
		p.Querys = append(p.Querys, tuple)
	}
	return nil
}

// renameImpl used to build distributed querys for rename oldTbl to newTbl.
func (p *DDLPlan) renameImpl() error {
	oldNode := p.node
	// Check if fromDatabase and toDatabase is same or not.
	fromDatabase := p.database
	if !oldNode.Table.Qualifier.IsEmpty() {
		fromDatabase = oldNode.Table.Qualifier.String()
	}
	if toDatabase := oldNode.NewName.Qualifier.String(); toDatabase != "" && toDatabase != fromDatabase {
		// toDatabase must equal to fromDatabase if not empty
		err := fmt.Sprintf("unsupported: Database is not equal[%s:%s]", fromDatabase, toDatabase)
		return errors.New(err)
	}

	oldFromTable := oldNode.Table.Name.String()
	oldToTable := oldNode.NewName.Name.String()
	segments, err := p.router.Lookup(fromDatabase, oldFromTable, nil, nil)
	if err != nil {
		return err
	}

	for _, segment := range segments {
		// Get newFromTable and newToTable
		newFromTable := segment.Table
		var newToTable string
		shardKey, err := p.router.ShardKey(fromDatabase, oldFromTable)
		if err != nil {
			return err
		}
		if shardKey != "" {
			// just to the shardtable, the suffix with "_0001" is valid
			splits := strings.SplitN(segment.Table, "_", -1)
			suffix := splits[len(splits)-1]
			newToTable = oldToTable + "_" + suffix
		} else {
			newToTable = oldToTable
		}

		// Rewrite rename ast, replace oldFromTable to newFromTable and oldToTable to newToTable, then format to a new query
		newNode := *oldNode
		buf := sqlparser.NewTrackedBuffer(nil)
		newNode.Table = sqlparser.TableName{
			Name:      sqlparser.NewTableIdent(newFromTable),
			Qualifier: sqlparser.NewTableIdent(fromDatabase),
		}
		newNode.NewName = sqlparser.TableName{
			Name:      sqlparser.NewTableIdent(newToTable),
			Qualifier: sqlparser.NewTableIdent(fromDatabase),
		}
		newNode.Format(buf)
		newQuery := buf.String()

		tuple := xcontext.QueryTuple{
			Query:   newQuery,
			Backend: segment.Backend,
			Range:   segment.Range.String(),
		}
		p.Querys = append(p.Querys, tuple)
	}
	return nil
}

// Build used to build DDL distributed querys.
// sqlparser.DDL is a simple grammar ast, it just parses database and table name in the prefix.
// In our sql syntax in sql.y, alter will be changed to rename in case next:
// ALTER ignore_opt TABLE table_name RENAME to_opt table_name
// {
//   Change this to a rename statement
//   $$ = &DDL{Action: RenameStr, Table: $4, NewName: $7}
// }
func (p *DDLPlan) Build() error {
	var err error

	switch p.node.Action {
	case sqlparser.DropTableStr:
		err = p.dropTblImpl()
	case sqlparser.RenameStr:
		err = p.renameImpl()
	default:
		err = p.commonImpl()
	}
	return err
}

// Type returns the type of the plan.
func (p *DDLPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *DDLPlan) JSON() string {
	type explain struct {
		RawQuery   string                `json:",omitempty"`
		Partitions []xcontext.QueryTuple `json:",omitempty"`
	}

	// Partitions.
	var parts []xcontext.QueryTuple
	parts = append(parts, p.Querys...)
	exp := &explain{
		RawQuery:   p.RawQuery,
		Partitions: parts,
	}
	// If exp include escape, json will add '\' before it.
	// e.g.: "\n\t tbl \n" will be "\\n\\t tbl \\n"
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return common.BytesToString(bout)
}

// Size returns the memory size.
func (p *DDLPlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
