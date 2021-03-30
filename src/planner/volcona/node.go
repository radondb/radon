/*
 * Radon
 *
 * Copyright 2021 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package volcona

import (
	"config"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// Plan interface.
type Node interface {
	build()
}

type tableInfo struct {
	database string
	name     string
	alias    string
	// table's config.
	tableConf *config.TableConfig
	// table's route.
	Segments []router.Segment `json:",omitempty"`
	// table expression in select ast 'From'.
	tableExpr *sqlparser.AliasedTableExpr
	// table's parent node, the type always a Route.
	parent *Route
}

type Route struct {
	// select ast.
	Stmt sqlparser.SelectStatement
	// 未来将使用bitmap
	isDual   bool
	hasParen bool
	isGlobal bool
	// the shard index slice.
	indexes []int
	// referred tables' tableInfo map.
	referTables map[string]*tableInfo
}

func (n *Route) build() {
}

type Join struct {
	hasParen bool
}

func (n *Join) build() {
}
