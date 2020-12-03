/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ ChildPlan = &LimitPlan{}
)

// LimitPlan represents order-by plan.
type LimitPlan struct {
	log *xlog.Log

	node      *sqlparser.Limit
	rewritten *sqlparser.Limit
	Offset    int
	Limit     int

	// type
	typ ChildType
}

// NewLimitPlan used to create LimitPlan.
func NewLimitPlan(log *xlog.Log, node *sqlparser.Limit) *LimitPlan {
	return &LimitPlan{
		log:  log,
		node: node,
		typ:  ChildTypeLimit,
	}
}

// analyze used to analyze the 'order by' is at the support level.
func (p *LimitPlan) analyze() error {
	ok := true
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		// Limit clause must be SQLVal type.
		case *sqlparser.Limit:
			return true, nil
		case *sqlparser.SQLVal:
			val := node.(*sqlparser.SQLVal)
			if val.Type != sqlparser.IntVal {
				ok = false
				return false, nil
			}
			return true, nil
		default:
			ok = false
			return false, nil
		}
	}, p.node)

	if !ok {
		return errors.New("unsupported: limit.offset.or.counts.must.be.IntVal")
	}
	return nil
}

// Build used to build distributed querys.
func (p *LimitPlan) Build() error {
	if p.node == nil {
		return nil
	}

	if err := p.analyze(); err != nil {
		return err
	}

	if p.node.Offset != nil {
		val := p.node.Offset.(*sqlparser.SQLVal)
		out, err := strconv.ParseInt(common.BytesToString(val.Val), 10, 64)
		if err != nil {
			return err
		}
		p.Offset = int(out)
	}

	if p.node.Rowcount != nil {
		val := p.node.Rowcount.(*sqlparser.SQLVal)
		out, err := strconv.ParseInt(common.BytesToString(val.Val), 10, 64)
		if err != nil {
			return err
		}
		p.Limit = int(out)
	}
	p.rewritten = &sqlparser.Limit{Rowcount: sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", p.Offset+p.Limit)))}
	return nil
}

// Type returns the type of the plan.
func (p *LimitPlan) Type() ChildType {
	return p.typ
}

// JSON returns the plan info.
func (p *LimitPlan) JSON() string {
	out, err := common.ToJSONString(p, false, "", "\t")
	if err != nil {
		return err.Error()
	}
	return out
}

// ReWritten used to re-write the limit clause.
func (p *LimitPlan) ReWritten() *sqlparser.Limit {
	return p.rewritten
}
