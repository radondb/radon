package datum

import (
	"fmt"
	"regexp"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/cache"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

var (
	regexpCache = cache.NewLRUCache(1024)
)

type regexpVal struct {
	re *regexp.Regexp
}

func (val *regexpVal) Size() int {
	return len(val.re.String())
}

func Regexp(left, right Datum, not bool) (Datum, error) {
	if CheckNull(left, right) {
		return NewDNull(true), nil
	}

	regexpStr := right.ValStr()
	if ignoreCase(left) && ignoreCase(right) {
		regexpStr = fmt.Sprintf("(?i)%s", regexpStr)
	}

	var re *regexp.Regexp
	if rex, ok := regexpCache.Get(regexpStr); ok {
		re = rex.(*regexpVal).re
	} else {
		re = regexp.MustCompile(regexpStr)
		regexpCache.Set(regexpStr, &regexpVal{re})
	}

	match := re.MatchString(left.ValStr())
	if not {
		match = !match
	}
	res := common.TernaryOpt(match, 1, 0).(int64)
	return NewDInt(res, false), nil
}
