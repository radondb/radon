package planner

import (
	"expression/evaluation"

	"github.com/pkg/errors"
)

type Plan interface {
	Walk(visit Visit) error
	String() string
}

type Visit func(plan Plan) (kontinue bool, err error)

func Walk(visit Visit, plans ...Plan) error {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		kontinue, err := visit(plan)
		if err != nil {
			return err
		}
		if kontinue {
			err = plan.Walk(visit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getArgsNum(p Plan) int {
	if t, ok := p.(*TuplePlan); ok {
		return len(t.args)
	}
	return 1
}

func popFirstArg(p Plan) Plan {
	if t, ok := p.(*TuplePlan); ok {
		if len(t.args) == 2 {
			return t.args[1]
		}
		return NewTuplePlan(t.args[1:])
	}
	return nil
}

func BuildEvaluation(p Plan) (evaluation.Evaluation, error) {
	switch p := p.(type) {
	case *VariablePlan:
		return evaluation.VAR(p.String()), nil
	case *ConstantPlan:
		return evaluation.CONST(p.value), nil
	case *TuplePlan:
		evals := make([]evaluation.Evaluation, len(p.args))
		for i, arg := range p.args {
			eval, err := BuildEvaluation(arg)
			if err != nil {
				return nil, err
			}
			evals[i] = eval
		}
		return evaluation.TUPLE(evals), nil
	case *UnaryPlan:
		eval, err := BuildEvaluation(p.arg)
		if err != nil {
			return nil, err
		}
		return evaluation.EvaluationFactory(p.name, eval)
	case *BinaryPlan:
		left, err := BuildEvaluation(p.left)
		if err != nil {
			return nil, err
		}
		right, err := BuildEvaluation(p.right)
		if err != nil {
			return nil, err
		}
		return evaluation.EvaluationFactory(p.name, left, right)
	case *FunctionPlan:
		evals := make([]evaluation.Evaluation, len(p.args))
		for i, arg := range p.args {
			eval, err := BuildEvaluation(arg)
			if err != nil {
				return nil, err
			}
			evals[i] = eval
		}
		return evaluation.EvaluationFactory(p.name, evals...)
	default:
		return nil, errors.Errorf("Unsupported expression plan:%T", p)
	}
}
