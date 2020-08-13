/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/cache"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

var (
	likeCache = cache.NewLRUCache(1024)
)

// pattern type.
const (
	patMatch = iota
	patOne
	patAny
)

// compare type.
type cmpType int

const (
	any = iota + 1
	match
	like
)

// CmpLike ...
type CmpLike struct {
	patChars   []byte
	patTyps    []byte
	ignoreCase bool
	cmpTyp     cmpType
}

// NewCmpLike new a CmpLike object.
func NewCmpLike(pattern string, escape byte, ignoreCase bool) *CmpLike {
	if strings.Compare("%", pattern) == 0 {
		return &CmpLike{ignoreCase: ignoreCase, cmpTyp: any}
	}

	length := len(pattern)
	if length == 0 {
		return &CmpLike{ignoreCase: ignoreCase, cmpTyp: like}
	}

	var (
		patLen      = 0
		lastAny     = false
		isFullMatch = true
		patChars    = make([]byte, len(pattern))
		patTyps     = make([]byte, len(pattern))
	)

	for i := 0; i < length; i++ {
		var typ byte
		b := pattern[i]
		switch b {
		case escape:
			lastAny = false
			typ = patMatch
			if i < length-1 {
				i++
				b = pattern[i]
			}
		case '_':
			isFullMatch = false
			if lastAny {
				patChars[patLen-1], patChars[patLen] = b, patChars[patLen-1]
				patTyps[patLen-1], patTyps[patLen] = patOne, patAny
				patLen++
				continue
			}
			typ = patOne
		case '%':
			if lastAny {
				continue
			}
			isFullMatch = false
			typ = patAny
			lastAny = true
		default:
			typ = patMatch
			lastAny = false
		}
		patChars[patLen] = b
		patTyps[patLen] = typ
		patLen++
	}

	patChars, patTyps = patChars[:patLen], patTyps[:patLen]
	if isFullMatch {
		return &CmpLike{patChars, patTyps, ignoreCase, match}
	}
	return &CmpLike{patChars, patTyps, ignoreCase, like}
}

// Compare use to check left whether match the pattern.
func (c *CmpLike) Compare(left Datum) bool {
	val := []byte(left.ValStr())
	switch c.cmpTyp {
	case any:
		return true
	case match:
		if c.ignoreCase {
			return bytes.EqualFold(c.patChars, val)
		}
		return bytes.Compare(c.patChars, val) == 0
	default:
		return isMatch(val, c.patChars, c.patTyps, c.ignoreCase)
	}
}

// Size implement the interface Value.Size() in LRUCache.
func (c *CmpLike) Size() int {
	return len(c.patChars) + len(c.patTyps)
}

func isMatch(val, patChars, patTyps []byte, ignoreCase bool) bool {
	idx := 0
	for i := 0; i < len(patChars); i++ {
		switch patTyps[i] {
		case patMatch:
			if idx >= len(val) || !compareByte(val[idx], patChars[i], ignoreCase) {
				return false
			}
			idx++
		case patOne:
			idx++
			if idx > len(val) {
				return false
			}
		case patAny:
			i++
			if i == len(patChars) {
				return true
			}
			for idx < len(val) {
				if compareByte(val[idx], patChars[i], ignoreCase) && isMatch(val[idx:], patChars[i:], patTyps[i:], ignoreCase) {
					return true
				}
				idx++
			}
			return false
		}
	}
	return idx == len(val)
}

func compareByte(a, b byte, ignoreCase bool) bool {
	if !ignoreCase {
		return a == b
	}
	return unicode.ToUpper(rune(a)) == unicode.ToUpper(rune(b))
}

// Like use to check left whether match right with escape.
// not means 'not like'.
func Like(left, right, escape Datum, not bool) (Datum, error) {
	if CheckNull(left, right) {
		return NewDNull(true), nil
	}

	esc := byte('\\')
	if escape != nil {
		escStr := escape.ValStr()
		if len(escStr) != 1 {
			return nil, errors.New("Incorrect.arguments.to.ESCAPE")
		}
		esc = escStr[0]
	}

	pattern := right.ValStr()
	ignoreCase := ignoreCase(left) && ignoreCase(right)
	key := fmt.Sprintf("%s|%s|%d", pattern, string(esc), common.TernaryOpt(ignoreCase, 1, 0).(int))

	var cmp *CmpLike
	if val, ok := likeCache.Get(key); ok {
		cmp = val.(*CmpLike)
	} else {
		cmp = NewCmpLike(pattern, esc, ignoreCase)
		likeCache.Set(key, cmp)
	}

	match := cmp.Compare(left)
	if not {
		match = !match
	}

	var res int64
	if match {
		res = 1
	}
	return NewDInt(res, false), nil
}
