/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"fmt"
	"math"
	"strings"

	"github.com/shopspring/decimal"
)

// TernaryOpt is ternary operator.
func TernaryOpt(condition bool, trueVal, falseVal interface{}) interface{} {
	if condition {
		return trueVal
	}
	return falseVal
}

// StrToHex convert the string to hex string.
func StrToHex(s string) string {
	var sa = make([]string, len(s))
	for _, v := range s {
		sa = append(sa, fmt.Sprintf("%02X", v))
	}
	ss := strings.Join(sa, "")
	return ss
}

// Float64ToInt64 convert float64 to int64.
func Float64ToInt64(f float64) int64 {
	if math.Abs(f) < 0.5 {
		return 0
	}
	if f < 0 {
		f -= 0.5
	} else {
		f += 0.5
	}

	res := math.Trunc(f)
	if res >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if res < float64(math.MinInt64) {
		return math.MinInt64
	}
	return int64(res)
}

// IsDecimalInf used to check whether decimal overflow.
func IsDecimalInf(d decimal.Decimal) bool {
	v, _ := d.Float64()
	if math.IsInf(v, 0) {
		return true
	}
	return false
}

// GetFloatPrefix gets prefix of string which can be successfully parsed as float.
// See https://github.com/golang/go/blob/master/src/strconv/atof.go#L175.
func GetFloatPrefix(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return "0"
	}

	var (
		prefix    = 0
		idx       = 0
		base      = 10
		sawdot    = false
		sawdigits = false
		sawexp    = false
	)

	if s[idx] == '+' || s[idx] == '-' {
		idx++
	}

	if idx+2 < len(s) && s[idx] == '0' && lower(s[idx+1]) == 'x' {
		base = 16
		idx += 2
	}

	for ; idx < len(s); idx++ {
		switch c := s[idx]; true {
		case c == '.':
			if sawdot {
				break
			}
			sawdot = true
			continue

		case '0' <= c && c <= '9':
			sawdigits = true
			prefix = idx + 1
			continue

		case base == 16 && 'a' <= lower(c) && lower(c) <= 'f':
			sawdigits = true
			prefix = idx + 1
			continue

		case base == 10 && lower(c) == 'e':
			// e12 or 1e2e3.
			if !sawdigits || sawexp {
				break
			}
			// 1e+2 or 1e-2.
			if idx+1 < len(s) && (s[idx+1] == '+' || s[idx+1] == '-') {
				idx++
			}
			sawexp = true
			continue
		}
		break
	}
	if !sawdigits {
		return "0"
	}
	return s[:prefix]
}

func lower(c byte) byte {
	return c | ('x' - 'X')
}
