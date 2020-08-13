package evaluation

import (
	"strings"

	"github.com/pkg/errors"
)

type (
	unaryEvalCreator    func(arg Evaluation) Evaluation
	binaryEvalCreator   func(left, right Evaluation) Evaluation
	functionEvalCreator func(args ...Evaluation) Evaluation
)

var (
	unaryEvalTable = map[string]unaryEvalCreator{
		"not": NOT,
	}

	binaryEvalTable = map[string]binaryEvalCreator{
		"+":          ADD,
		"-":          SUB,
		"*":          MUL,
		"/":          DIV,
		"div":        INTDIV,
		">":          GT,
		">=":         GE,
		"=":          EQ,
		"<":          LT,
		"<=":         LE,
		"!=":         NE,
		"<=>":        SE,
		"in":         IN,
		"not in":     NOTIN,
		"regexp":     REGEXP,
		"not regexp": NOTREGEXP,
		"and":        AND,
		"or":         OR,
	}

	functionEvalTable = map[string]functionEvalCreator{
		"like":     LIKE,
		"not like": NOTLIKE,
		"if":       IF,
	}
)

// EvalFactory used to build the evaluation by the given name and args.
func EvalFactory(name string, args ...Evaluation) (Evaluation, error) {
	name = strings.ToLower(name)
	switch len(args) {
	case 1:
		if creator, ok := unaryEvalTable[name]; ok {
			return creator(args[0]), nil
		}
	case 2:
		if creator, ok := binaryEvalTable[name]; ok {
			return creator(args[0], args[1]), nil
		}
	}
	if creator, ok := functionEvalTable[name]; ok {
		return creator(args...), nil
	}
	return nil, errors.Errorf("Unsupported Expression:%v", name)
}
