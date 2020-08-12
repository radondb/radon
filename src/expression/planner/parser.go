package planner

import (
	"reflect"
	"strings"

	"expression/datum"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

func ParseExpression(expr sqlparser.Expr) (Plan, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		var column, table, database string
		column = expr.Name.String()
		if !expr.Qualifier.Name.IsEmpty() {
			table = expr.Qualifier.Name.String()
			if !expr.Qualifier.Qualifier.IsEmpty() {
				database = expr.Qualifier.Qualifier.String()
			}
		}
		return NewVariablePlan(column, table, database), nil
	case *sqlparser.SQLVal:
		val, err := datum.SQLValToDatum(expr)
		if err != nil {
			return nil, err
		}
		return NewConstantPlan(val), nil
	case *sqlparser.FuncExpr:
		return parseFuncExpr(expr)
	case *sqlparser.UnaryExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewUnaryPlan(expr.Operator, arg), nil
	case *sqlparser.BinaryExpr:
		left, err := ParseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := ParseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryPlan(expr.Operator, left, right), nil
	case *sqlparser.ComparisonExpr:
		return parseComparisonExpr(expr)
	case *sqlparser.OrExpr:
		left, err := ParseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := ParseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryPlan("or", left, right), nil
	case *sqlparser.NotExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewUnaryPlan("not", arg), nil
	case *sqlparser.AndExpr:
		left, err := ParseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := ParseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryPlan("and", left, right), nil
	case *sqlparser.ParenExpr:
		return ParseExpression(expr.Expr)
	case *sqlparser.NullVal:
		return NewConstantPlan(datum.NewDNull(true)), nil
	case sqlparser.BoolVal:
		val := int64(0)
		if expr {
			val = 1
		}
		return NewConstantPlan(datum.NewDInt(val, false)), nil
	case sqlparser.ValTuple:
		args := make([]Plan, len(expr))
		for i := range expr {
			subExpr, err := ParseExpression(expr[i])
			if err != nil {
				return nil, err
			}
			args[i] = subExpr
		}
		return NewTuplePlan(args), nil
	case *sqlparser.IntervalExpr:
		subExpr, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse expression in interval")
		}

		unit := strings.ToLower(expr.Unit)
		return NewBinaryPlan("interval", subExpr, NewConstantPlan(datum.NewDString(unit, 10))), nil
	case *sqlparser.RangeCond:
		return parseRangeCond(expr)
	case *sqlparser.IsExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewUnaryPlan(expr.Operator, arg), nil
	// TODO:
	// case *sqlparser.Subquery:
	// case *sqlparser.ExistsExpr:
	// case *sqlparser.ConvertExpr:
	// case *sqlparser.CollateExpr:
	// case *sqlparser.ConvertUsingExpr:
	case *sqlparser.GroupConcatExpr:
		// TODO: order by.
		args := make([]Plan, len(expr.Exprs)+1)
		args[0] = NewConstantPlan(datum.NewDString(expr.Separator, 10))
		for i, expr := range expr.Exprs {
			aliased, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, errors.Errorf("unsupported.argument.'%v'.of.type.'%v'", expr, reflect.TypeOf(expr))
			}
			arg, err := ParseExpression(aliased.Expr)
			if err != nil {
				return nil, err
			}
			args[i+1] = arg
		}
		return NewAggregatePlan("group_concat", expr.Distinct != "", args...), nil
	case *sqlparser.CaseExpr:
		return parseCaseExpr(expr)
	}
	return nil, nil
}

func parseRangeCond(expr *sqlparser.RangeCond) (Plan, error) {
	left, err := ParseExpression(expr.Left)
	if err != nil {
		return nil, err
	}

	from, err := ParseExpression(expr.From)
	if err != nil {
		return nil, err
	}

	to, err := ParseExpression(expr.To)
	if err != nil {
		return nil, err
	}

	return NewBinaryPlan("and",
		NewBinaryPlan(sqlparser.GreaterEqualStr, left, from),
		NewBinaryPlan(sqlparser.LessEqualStr, left, to),
	), nil
}

func parseFuncExpr(expr *sqlparser.FuncExpr) (Plan, error) {
	name := strings.ToLower(expr.Name.String())
	args := make([]Plan, len(expr.Exprs))

	for i, exp := range expr.Exprs {
		switch exp := exp.(type) {
		case *sqlparser.AliasedExpr:
			arg, err := ParseExpression(exp.Expr)
			if err != nil {
				return nil, err
			}
			args[i] = arg
		case *sqlparser.StarExpr:
			if name != "count" || expr.Distinct {
				return nil, errors.Errorf("unsupported: syntax.error.at.'%s'", name)
			}
			args[i] = NewConstantPlan(datum.NewDInt(1, false))
		default:
			return nil, errors.Errorf("unsupported.argument.%v.of.type.'nextval'", expr)
		}
	}

	if expr.IsAggregate() {
		if len(args) != 1 {
			return nil, errors.Errorf("unsupported: invalid.use.of.group.function[%s]", name)
		}
		return NewAggregatePlan(name, expr.Distinct, args...), nil
	}
	return NewFunctionPlan(name, args...), nil
}

func parseCaseExpr(expr *sqlparser.CaseExpr) (Plan, error) {
	var args []Plan
	if expr.Expr != nil {
		left, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		for _, when := range expr.Whens {
			right, err := ParseExpression(when.Cond)
			if err != nil {
				return nil, err
			}
			args = append(args, NewBinaryPlan(sqlparser.EqualStr, left, right))
			res, err := ParseExpression(when.Val)
			if err != nil {
				return nil, err
			}
			args = append(args, res)
		}
	} else {
		for _, when := range expr.Whens {
			arg, err := ParseExpression(when.Cond)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			res, err := ParseExpression(when.Val)
			if err != nil {
				return nil, err
			}
			args = append(args, res)
		}
	}
	if expr.Else != nil {
		arg, err := ParseExpression(expr.Else)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return NewFunctionPlan("case", args...), nil
}

func parseComparisonExpr(expr *sqlparser.ComparisonExpr) (Plan, error) {
	left, err := ParseExpression(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := ParseExpression(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.LessThanStr, sqlparser.GreaterThanStr, sqlparser.LessEqualStr,
		sqlparser.GreaterEqualStr, sqlparser.NotEqualStr, sqlparser.NullSafeEqualStr:
		return parseCmpOp(expr.Operator, left, right)
	case sqlparser.InStr:
		return parseInExpr(false, left, right)
	case sqlparser.NotInStr:
		return parseInExpr(true, left, right)
	case sqlparser.LikeStr, sqlparser.NotLikeStr:
		return parseLikeExpr(expr.Operator, left, right, expr.Escape)
	default:
		lLen, rLen := getArgsNum(left), getArgsNum(right)
		if lLen != 1 || rLen != 1 {
			return nil, errors.New("operand.should.contain.1.column(s)")
		}
		return NewBinaryPlan(expr.Operator, left, right), nil
	}
}

func parseCmpOp(op string, left, right Plan) (Plan, error) {
	lLen, rLen := getArgsNum(left), getArgsNum(right)
	if lLen != rLen {
		return nil, errors.Errorf("operand.should.contain.%d.column(s)", lLen)
	}
	if lLen == 1 {
		return NewBinaryPlan(op, left, right), nil
	}

	lTuple, rTuple := left.(*TuplePlan), right.(*TuplePlan)
	switch op {
	case sqlparser.EqualStr, sqlparser.NotEqualStr, sqlparser.NullSafeEqualStr:
		args := make([]Plan, lLen)
		for i := 0; i < rLen; i++ {
			var err error
			args[i], err = parseCmpOp(op, lTuple.args[i], rTuple.args[i])
			if err != nil {
				return nil, err
			}
		}
		if op == sqlparser.NotEqualStr {
			return composeBinaryPlan("or", args), nil
		}
		return composeBinaryPlan("and", args), nil
	default:
		arg1 := NewBinaryPlan(sqlparser.EqualStr, lTuple.args[0], rTuple.args[0])
		arg2 := NewBinaryPlan(op, lTuple.args[0], rTuple.args[0])
		arg3, err := parseCmpOp(op, popFirstArg(left), popFirstArg(right))
		if err != nil {
			return nil, err
		}
		return NewFunctionPlan("if", arg1, arg3, arg2), nil
	}
}

func parseInExpr(not bool, left, right Plan) (Plan, error) {
	if getArgsNum(left) == 1 {
		for _, arg := range right.(*TuplePlan).args {
			if getArgsNum(arg) != 1 {
				return nil, errors.New("operand.should.contain.1.column(s)")
			}
		}

		if not {
			return NewBinaryPlan(sqlparser.NotInStr, left, right), nil
		}
		return NewBinaryPlan(sqlparser.InStr, left, right), nil
	}

	conds := make([]Plan, getArgsNum(right))
	for i, arg := range right.(*TuplePlan).args {
		var err error
		if conds[i], err = parseCmpOp(sqlparser.EqualStr, left, arg); err != nil {
			return nil, err
		}
	}
	res := composeBinaryPlan("or", conds)

	if not {
		res = NewUnaryPlan("not", res)
	}
	return res, nil
}

func parseLikeExpr(op string, left, right Plan, escape sqlparser.Expr) (Plan, error) {
	lLen, rLen := getArgsNum(left), getArgsNum(right)
	if lLen != 1 || rLen != 1 {
		return nil, errors.New("operand.should.contain.1.column(s)")
	}

	var esc Plan
	var err error
	if escape != nil {
		esc, err = ParseExpression(escape)
		if err != nil {
			return nil, err
		}
	} else {
		esc = NewConstantPlan(datum.NewDString("\\", 10))
	}
	return NewFunctionPlan(op, left, right, esc), nil
}

func composeBinaryPlan(op string, args []Plan) Plan {
	argsLen := len(args)
	if argsLen == 0 {
		return nil
	}
	if argsLen == 1 {
		return args[0]
	}
	return NewBinaryPlan(op, composeBinaryPlan(op, args[:argsLen/2]), composeBinaryPlan(op, args[argsLen/2:]))
}
