package expression

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/evaluation"
)

// Expression interface.
type Expression interface {
	String() string
	walk(visit Visit) error
	Materialize() (evaluation.Evaluation, error)
	replace(from, to Expression) bool
}

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(plan Expression) (kontinue bool, err error)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, plans ...Expression) error {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		kontinue, err := visit(plan)
		if err != nil {
			return err
		}
		if kontinue {
			err = plan.walk(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ReplaceExpression(root, from, to Expression) Expression {
	if root == from {
		return to
	}
	root.replace(from, to)
	return root
}

// replaceExprs is a convenience function used by implementors
// of the replace method.
func replaceExpression(from, to Expression, exprs ...*Expression) bool {
	for _, expr := range exprs {
		if *expr == nil {
			continue
		}
		if *expr == from {
			*expr = to
			return true
		}
		if (*expr).replace(from, to) {
			return true
		}
	}
	return false
}

func getArgsNum(p Expression) int {
	if t, ok := p.(*TupleExpr); ok {
		return len(t.args)
	}
	return 1
}

func popFirstArg(p Expression) Expression {
	if t, ok := p.(*TupleExpr); ok {
		if len(t.args) == 2 {
			return t.args[1]
		}
		return NewTupleExpr(t.args[1:])
	}
	return nil
}

// AggregateExpr ...
type AggregateExpr struct {
	Expr     sqlparser.Expr
	Name     string
	Distinct bool
	Field    string
	args     []Expression
}

// NewAggregateExpr new a AggregateExpr.
func NewAggregateExpr(expr sqlparser.Expr, name string, distinct bool, field string, args ...Expression) *AggregateExpr {
	return &AggregateExpr{
		Expr:     expr,
		Name:     name,
		Distinct: distinct,
		Field:    field,
		args:     args,
	}
}

// Materialize returns Evaluation by Expression.
func (p *AggregateExpr) Materialize() (evaluation.Evaluation, error) {
	return nil, errors.Errorf("temporarily.unsupport")
}

func (p *AggregateExpr) replace(from, to Expression) bool {
	for i, arg := range p.args {
		if replaceExpression(from, to, &arg) {
			p.args[i] = arg
			return true
		}
	}
	return false
}

func (p *AggregateExpr) walk(visit Visit) error {
	return Walk(visit, p.args...)
}

// String return the plan info.
func (p *AggregateExpr) String() string {
	dist := ""
	if p.Distinct {
		dist = "distinct "
	}
	args := make([]string, len(p.args))
	for i, arg := range p.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s%s)", p.Name, dist, strings.Join(args, ", "))
}

// BinaryExpr ...
type BinaryExpr struct {
	name        string
	left, right Expression
}

// NewBinaryExpr new a BinaryExpr.
func NewBinaryExpr(name string, left, right Expression) *BinaryExpr {
	if name == sqlparser.EqualStr {
		_, lok := left.(*ConstantExpr)
		_, rok := right.(*VariableExpr)
		if lok && rok {
			left, right = right, left
		}
	}

	return &BinaryExpr{
		name:  name,
		left:  left,
		right: right,
	}
}

// Materialize returns Evaluation by Expression.
func (p *BinaryExpr) Materialize() (evaluation.Evaluation, error) {
	left, err := p.left.Materialize()
	if err != nil {
		return nil, err
	}
	right, err := p.right.Materialize()
	if err != nil {
		return nil, err
	}
	return evaluation.EvalFactory(p.name, left, right)
}

func (p *BinaryExpr) replace(from, to Expression) bool {
	return replaceExpression(from, to, &p.left, &p.right)
}

func (p *BinaryExpr) walk(visit Visit) error {
	return Walk(visit, p.left, p.right)
}

// String return the plan info.
func (p *BinaryExpr) String() string {
	return fmt.Sprintf("%s(%s, %s)", p.name, p.left.String(), p.right.String())
}

// CastExpr ...
type CastExpr struct {
	arg Expression
	typ *sqlparser.ConvertType
}

// NewCastExpr new a UnaryExpr.
func NewCastExpr(arg Expression, typ *sqlparser.ConvertType) *CastExpr {
	return &CastExpr{
		arg: arg,
		typ: typ,
	}
}

// Materialize returns Evaluation by Expression.
func (p *CastExpr) Materialize() (evaluation.Evaluation, error) {
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

func (p *CastExpr) replace(from, to Expression) bool {
	return replaceExpression(from, to, &p.arg)
}

func (p *CastExpr) walk(visit Visit) error {
	return Walk(visit, p.arg)
}

// String return the plan info.
func (p *CastExpr) String() string {
	buf := sqlparser.NewTrackedBuffer(nil)
	p.typ.Format(buf)
	return fmt.Sprintf("cast(%s as %s)", p.arg.String(), buf.String())
}

// ConstantExpr ...
type ConstantExpr struct {
	value datum.Datum
}

// NewConstantExpr new a ConstantExpr.
func NewConstantExpr(value datum.Datum) *ConstantExpr {
	return &ConstantExpr{
		value: value,
	}
}

// Materialize returns Evaluation by Expression.
func (p *ConstantExpr) Materialize() (evaluation.Evaluation, error) {
	return evaluation.CONST(p.value), nil
}

func (p *ConstantExpr) replace(from, to Expression) bool {
	return false
}

func (p *ConstantExpr) walk(visit Visit) error {
	return nil
}

// String return the plan info.
func (p *ConstantExpr) String() string {
	return fmt.Sprintf("%v", p.value.ValStr())
}

// FunctionExpr ...
type FunctionExpr struct {
	name string
	args []Expression
}

// NewFunctionExpr new a FunctionExpr.
func NewFunctionExpr(name string, args ...Expression) *FunctionExpr {
	return &FunctionExpr{
		name: name,
		args: args,
	}
}

// Materialize returns Evaluation by Expression.
func (p *FunctionExpr) Materialize() (evaluation.Evaluation, error) {
	evals := make([]evaluation.Evaluation, len(p.args))
	for i, arg := range p.args {
		eval, err := arg.Materialize()
		if err != nil {
			return nil, err
		}
		evals[i] = eval
	}
	return evaluation.EvalFactory(p.name, evals...)
}

func (p *FunctionExpr) replace(from, to Expression) bool {
	for i, arg := range p.args {
		if replaceExpression(from, to, &arg) {
			p.args[i] = arg
			return true
		}
	}
	return false
}

func (p *FunctionExpr) walk(visit Visit) error {
	return Walk(visit, p.args...)
}

// String return the plan info.
func (p *FunctionExpr) String() string {
	result := make([]string, len(p.args))
	for i, arg := range p.args {
		result[i] = arg.String()
	}
	str := strings.Join(result, ", ")
	return fmt.Sprintf("%s(%s)", p.name, str)
}

// TupleExpr ...
type TupleExpr struct {
	args []Expression
}

// NewTupleExpr new a TupleExpr.
func NewTupleExpr(args []Expression) *TupleExpr {
	return &TupleExpr{
		args: args,
	}
}

// Materialize returns Evaluation by Expression.
func (p *TupleExpr) Materialize() (evaluation.Evaluation, error) {
	evals := make([]evaluation.Evaluation, len(p.args))
	for i, arg := range p.args {
		eval, err := arg.Materialize()
		if err != nil {
			return nil, err
		}
		evals[i] = eval
	}
	return evaluation.TUPLE(evals...), nil
}

func (p *TupleExpr) replace(from, to Expression) bool {
	for i, arg := range p.args {
		if replaceExpression(from, to, &arg) {
			p.args[i] = arg
			return true
		}
	}
	return false
}

func (p *TupleExpr) walk(visit Visit) error {
	return Walk(visit, p.args...)
}

// String return the plan info.
func (p *TupleExpr) String() string {
	result := make([]string, len(p.args))
	for i, arg := range p.args {
		result[i] = arg.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(result, ", "))
}

// UnaryExpr ...
type UnaryExpr struct {
	name string
	arg  Expression
}

// NewUnaryExpr new a UnaryExpr.
func NewUnaryExpr(name string, arg Expression) *UnaryExpr {
	return &UnaryExpr{
		name: name,
		arg:  arg,
	}
}

// Materialize returns Evaluation by Expression.
func (p *UnaryExpr) Materialize() (evaluation.Evaluation, error) {
	eval, err := p.arg.Materialize()
	if err != nil {
		return nil, err
	}
	return evaluation.EvalFactory(p.name, eval)
}

func (p *UnaryExpr) replace(from, to Expression) bool {
	return replaceExpression(from, to, &p.arg)
}

func (p *UnaryExpr) walk(visit Visit) error {
	return Walk(visit, p.arg)
}

// String return the plan info.
func (p *UnaryExpr) String() string {
	return fmt.Sprintf("%s(%s)", p.name, p.arg.String())
}

// VariableExpr ..
type VariableExpr struct {
	column   string
	table    string
	database string
}

// NewVariableExpr new a VariableExpr.
func NewVariableExpr(column, table, database string) *VariableExpr {
	return &VariableExpr{
		column:   column,
		table:    table,
		database: database,
	}
}

// Materialize returns Evaluation by Expression.
func (p *VariableExpr) Materialize() (evaluation.Evaluation, error) {
	return evaluation.VAR(p.String()), nil
}

func (p *VariableExpr) replace(from, to Expression) bool {
	return false
}

func (p *VariableExpr) walk(visit Visit) error {
	return nil
}

// String return the plan info.
func (p *VariableExpr) String() string {
	str := fmt.Sprintf("`%s`", p.column)
	if p.table != "" {
		str = fmt.Sprintf("`%s`.%s", p.table, str)
		if p.database != "" {
			str = fmt.Sprintf("`%s`.%s", p.database, str)
		}
	}
	return str
}

// ParseExpression used to parse the expr to Expression.
func ParseExpression(expr sqlparser.Expr) (Expression, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		var column, tb, db string
		column = expr.Name.String()
		if !expr.Qualifier.Name.IsEmpty() {
			tb = expr.Qualifier.Name.String()
			if !expr.Qualifier.Qualifier.IsEmpty() {
				db = expr.Qualifier.Qualifier.String()
			}
		}
		return NewVariableExpr(column, tb, db), nil
	case *sqlparser.SQLVal:
		val, err := datum.SQLValToDatum(expr)
		if err != nil {
			return nil, err
		}
		return NewConstantExpr(val), nil
	case *sqlparser.FuncExpr:
		return parseFuncExpr(expr)
	case *sqlparser.UnaryExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewUnaryExpr(expr.Operator, arg), nil
	case *sqlparser.BinaryExpr:
		left, err := ParseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := ParseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryExpr(expr.Operator, left, right), nil
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
		return NewBinaryExpr("or", left, right), nil
	case *sqlparser.NotExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewUnaryExpr("not", arg), nil
	case *sqlparser.AndExpr:
		left, err := ParseExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := ParseExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		return NewBinaryExpr("and", left, right), nil
	case *sqlparser.ParenExpr:
		return ParseExpression(expr.Expr)
	case *sqlparser.NullVal:
		return NewConstantExpr(datum.NewDNull(true)), nil
	case sqlparser.BoolVal:
		val := int64(0)
		if expr {
			val = 1
		}
		return NewConstantExpr(datum.NewDInt(val, false)), nil
	case sqlparser.ValTuple:
		args := make([]Expression, len(expr))
		for i := range expr {
			subExpr, err := ParseExpression(expr[i])
			if err != nil {
				return nil, err
			}
			args[i] = subExpr
		}
		return NewTupleExpr(args), nil
	case *sqlparser.IntervalExpr:
		subExpr, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "couldn't parse expression in interval")
		}

		unit := strings.ToLower(expr.Unit)
		return NewBinaryExpr("interval", subExpr, NewConstantExpr(datum.NewDString(unit, 10, false))), nil
	case *sqlparser.RangeCond:
		return parseRangeCond(expr)
	case *sqlparser.IsExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewUnaryExpr(expr.Operator, arg), nil
	case *sqlparser.ConvertExpr:
		arg, err := ParseExpression(expr.Expr)
		if err != nil {
			return nil, err
		}
		return NewCastExpr(arg, expr.Type), nil
	// TODO:
	// case *sqlparser.Subquery:
	// case *sqlparser.ExistsExpr:
	// case *sqlparser.CollateExpr:
	// case *sqlparser.ConvertUsingExpr:
	case *sqlparser.GroupConcatExpr:
		// TODO: order by.
		args := make([]Expression, len(expr.Exprs)+1)
		args[0] = NewConstantExpr(datum.NewDString(expr.Separator, 10, true))
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
		buf := sqlparser.NewTrackedBuffer(nil)
		expr.Exprs.Format(buf)
		return NewAggregateExpr(expr, "group_concat", expr.Distinct != "", buf.String(), args...), nil
	case *sqlparser.CaseExpr:
		return parseCaseExpr(expr)
	}
	return nil, errors.Errorf("unsupported.of.type.'%v'", reflect.TypeOf(expr))
}

func parseRangeCond(expr *sqlparser.RangeCond) (Expression, error) {
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

	return NewBinaryExpr("and",
		NewBinaryExpr(sqlparser.GreaterEqualStr, left, from),
		NewBinaryExpr(sqlparser.LessEqualStr, left, to),
	), nil
}

func parseFuncExpr(expr *sqlparser.FuncExpr) (Expression, error) {
	name := strings.ToLower(expr.Name.String())
	args := make([]Expression, len(expr.Exprs))

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
			args[i] = NewConstantExpr(datum.NewDInt(1, false))
		default:
			return nil, errors.Errorf("unsupported.argument.%v.of.type.'nextval'", expr)
		}
	}

	if expr.IsAggregate() {
		if len(args) != 1 {
			return nil, errors.Errorf("unsupported: invalid.use.of.group.function[%s]", name)
		}
		buf := sqlparser.NewTrackedBuffer(nil)
		expr.Exprs.Format(buf)
		return NewAggregateExpr(expr, name, expr.Distinct, buf.String(), args...), nil
	}
	return NewFunctionExpr(name, args...), nil
}

func parseCaseExpr(expr *sqlparser.CaseExpr) (Expression, error) {
	var args []Expression
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
			args = append(args, NewBinaryExpr(sqlparser.EqualStr, left, right))
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
	return NewFunctionExpr("case", args...), nil
}

func parseComparisonExpr(expr *sqlparser.ComparisonExpr) (Expression, error) {
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
		return NewBinaryExpr(expr.Operator, left, right), nil
	}
}

func parseCmpOp(op string, left, right Expression) (Expression, error) {
	lLen, rLen := getArgsNum(left), getArgsNum(right)
	if lLen != rLen {
		return nil, errors.Errorf("operand.should.contain.%d.column(s)", lLen)
	}
	if lLen == 1 {
		return NewBinaryExpr(op, left, right), nil
	}

	lTuple, rTuple := left.(*TupleExpr), right.(*TupleExpr)
	switch op {
	case sqlparser.EqualStr, sqlparser.NotEqualStr, sqlparser.NullSafeEqualStr:
		args := make([]Expression, lLen)
		for i := 0; i < rLen; i++ {
			var err error
			args[i], err = parseCmpOp(op, lTuple.args[i], rTuple.args[i])
			if err != nil {
				return nil, err
			}
		}
		if op == sqlparser.NotEqualStr {
			return composeBinaryExpr("or", args), nil
		}
		return composeBinaryExpr("and", args), nil
	default:
		arg1 := NewBinaryExpr(sqlparser.EqualStr, lTuple.args[0], rTuple.args[0])
		arg2 := NewBinaryExpr(op, lTuple.args[0], rTuple.args[0])
		arg3, err := parseCmpOp(op, popFirstArg(left), popFirstArg(right))
		if err != nil {
			return nil, err
		}
		return NewFunctionExpr("if", arg1, arg3, arg2), nil
	}
}

func parseInExpr(not bool, left, right Expression) (Expression, error) {
	if getArgsNum(left) == 1 {
		for _, arg := range right.(*TupleExpr).args {
			if getArgsNum(arg) != 1 {
				return nil, errors.New("operand.should.contain.1.column(s)")
			}
		}

		if not {
			return NewBinaryExpr(sqlparser.NotInStr, left, right), nil
		}
		return NewBinaryExpr(sqlparser.InStr, left, right), nil
	}

	conds := make([]Expression, getArgsNum(right))
	for i, arg := range right.(*TupleExpr).args {
		var err error
		if conds[i], err = parseCmpOp(sqlparser.EqualStr, left, arg); err != nil {
			return nil, err
		}
	}
	res := composeBinaryExpr("or", conds)

	if not {
		res = NewUnaryExpr("not", res)
	}
	return res, nil
}

func parseLikeExpr(op string, left, right Expression, escape sqlparser.Expr) (Expression, error) {
	lLen, rLen := getArgsNum(left), getArgsNum(right)
	if lLen != 1 || rLen != 1 {
		return nil, errors.New("operand.should.contain.1.column(s)")
	}

	var esc Expression
	var err error
	if escape != nil {
		esc, err = ParseExpression(escape)
		if err != nil {
			return nil, err
		}
	} else {
		esc = NewConstantExpr(datum.NewDString("\\", 10, true))
	}
	return NewFunctionExpr(op, left, right, esc), nil
}

func composeBinaryExpr(op string, args []Expression) Expression {
	argsLen := len(args)
	if argsLen == 0 {
		return nil
	}
	if argsLen == 1 {
		return args[0]
	}
	return NewBinaryExpr(op, composeBinaryExpr(op, args[:argsLen/2]), composeBinaryExpr(op, args[argsLen/2:]))
}
