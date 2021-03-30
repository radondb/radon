package volcona

import (
	"fmt"
	"router"

	"github.com/pkg/errors"
	"github.com/radondb/shift/xlog"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type planBuilder struct {
	log      *xlog.Log
	router   *router.Router
	database string
}

func NewPlanBuilder(log *xlog.Log, router *router.Router, database string) *planBuilder {
	return &planBuilder{
		log:      log,
		router:   router,
		database: database,
	}
}

// BuildNode used to build the plannode tree.
func BuildNode(log *xlog.Log, router *router.Router, database string, node sqlparser.SelectStatement) (Node, error) {
	var err error
	var root Node
	b := NewPlanBuilder(log, router, database)
	switch node := node.(type) {
	case *sqlparser.Select:
		root, err = b.processSelect(node)
	case *sqlparser.Union:
		root, err = b.processUnion(node)
	default:
		err = errors.New("unsupported: unknown.select.statement")
	}
	if err != nil {
		return nil, err
	}

	//root.buildQuery(root)
	return root, nil
}

func (b *planBuilder) processSelect(node *sqlparser.Select) (Node, error) {
	return nil, nil
}

func (b *planBuilder) processUnion(node *sqlparser.Union) (Node, error) {
	return nil, nil
}

func (b *planBuilder) processPart(part sqlparser.SelectStatement) (Node, error) {
	switch stmt := part.(type) {
	case *sqlparser.Union:
		return b.processUnion(stmt)
	case *sqlparser.Select:
		node, err := b.processSelect(stmt)
		if err != nil {
			return nil, err
		}
		return node, nil
	case *sqlparser.ParenSelect:
		return b.processPart(stmt.Select)
	}
	panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", part))
}
