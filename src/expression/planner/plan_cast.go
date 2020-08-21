package planner

import (
	"expression/evaluation"
	"fmt"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// CastPlan ...
type CastPlan struct {
	arg Plan
	typ *sqlparser.ConvertType
}

// NewCastPlan new a UnaryPlan.
func NewCastPlan(arg Plan, typ *sqlparser.ConvertType) *CastPlan {
	return &CastPlan{
		arg: arg,
		typ: typ,
	}
}

// Materialize returns Evaluation by Plan.
func (p *CastPlan) Materialize() (evaluation.Evaluation, error) {
	arg, err := p.arg.Materialize()
	if err != nil {
		return nil, err
	}
	res, err := evaluation.EvalFactory("cast", arg)
	if err != nil {
		return res, err
	}
	res.(*evaluation.CastEval).SetType(p.typ)
	return res, nil
}

// String return the plan info.
func (p *CastPlan) String() string {
	buf := sqlparser.NewTrackedBuffer(nil)
	p.typ.Format(buf)
	return fmt.Sprintf("cast(%s as %s)", p.arg.String(), buf.String())
}
