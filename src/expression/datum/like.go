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

const (
	patternMatch = iota
	patternOne
	patternAny
)

type cmpType int

const (
	any = iota + 1
	match
	like
)

type CmpLike struct {
	patChars   []byte
	patTyps    []byte
	ignoreCase bool
	cmpTyp     cmpType
}

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
			typ = patternMatch
			if i < length-1 {
				i++
				b = pattern[i]
				if !(b == escape || b == '_' || b == '%') {
					// Invalid escape, fall back to escape byte.
					i--
					b = escape
				}
			}
		case '_':
			isFullMatch = false
			if lastAny {
				patChars[patLen-1], patChars[patLen] = b, patChars[patLen-1]
				patTyps[patLen-1], patTyps[patLen] = patternOne, patternAny
				patLen++
				continue
			}
			typ = patternOne
		case '%':
			if lastAny {
				continue
			}
			isFullMatch = false
			typ = patternAny
			lastAny = true
		default:
			typ = patternMatch
			lastAny = false
		}
		patChars[patLen] = b
		patTyps[patLen] = typ
		patLen++
	}
	if isFullMatch {
		return &CmpLike{patChars, patTyps, ignoreCase, match}
	}
	return &CmpLike{patChars, patTyps, ignoreCase, like}
}

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
	case like:
		return isMatch(val, c.patChars, c.patTyps, c.ignoreCase)
	}
	panic("unknow.cmp.type")
}

func (c *CmpLike) Size() int {
	return len(c.patChars) + len(c.patTyps)
}

func isMatch(val, patChars, patTyps []byte, ignoreCase bool) bool {
	idx := 0
	for i := 0; i < len(patChars); i++ {
		switch patTyps[i] {
		case patternMatch:
			if idx >= len(val) || !compareByte(val[idx], patChars[i], ignoreCase) {
				return false
			}
			idx++
		case patternOne:
			idx++
			if idx > len(val) {
				return false
			}
		case patternAny:
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
	key := fmt.Sprintf("%s|%s", pattern, string(esc))

	ignoreCase := ignoreCase(left) && ignoreCase(right)
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
	res := common.TernaryOpt(match, 1, 0).(int64)
	return NewDInt(res, false), nil
}
