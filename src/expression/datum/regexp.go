package datum

import (
	"fmt"
	"regexp"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/cache"
)

var (
	regexpCache = cache.NewLRUCache(1024)
)

type regexpVal struct {
	re *regexp.Regexp
}

// Size implement the interface Value.Size() in LRUCache.
func (val *regexpVal) Size() int {
	return len(val.re.String())
}

// Regexp used to check left whether match the regexp 'right'.
// not means 'not regexp'.
func Regexp(left, right Datum, not bool) Datum {
	if CheckNull(left, right) {
		return NewDNull(true)
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

	var res int64
	if match {
		res = 1
	}
	return NewDInt(res, false)
}
