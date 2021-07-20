/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package datum

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// NewMaxOrMinDec returns the max or min value decimal for given precision and fraction.
func NewMaxOrMinDec(negative bool, prec, frac int) decimal.Decimal {
	str := make([]byte, prec)
	for i := 0; i < len(str); i++ {
		str[i] = '9'
	}
	if negative {
		str[0] = '-'
	} else {
		str[0] = '+'
	}
	if frac > 0 {
		str[prec-frac-1] = '.'
	}

	dec, _ := decimal.NewFromString(string(str))
	return dec
}

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

// StrToInt convert the string to int64.
func StrToInt(s string, isCastFunc bool) (int64, error) {
	sub := GetIntPrefix(s, isCastFunc)
	return strconv.ParseInt(sub, 10, 64)
}

// StrToUint convert the string to uint64.
func StrToUint(s string, isCastFunc bool) (uint64, error) {
	sub := GetIntPrefix(s, isCastFunc)
	return strconv.ParseUint(sub, 10, 64)
}

// Float64ToUint64 convert float64 to uint64.
func Float64ToUint64(f float64) uint64 {
	res := roundFloat64(f)
	if res < 0 {
		return 0
	}
	if res >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(res)
}

// Float64ToInt64 convert float64 to int64.
func Float64ToInt64(f float64) int64 {
	res := roundFloat64(f)
	if res >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if res < float64(math.MinInt64) {
		return math.MinInt64
	}
	return int64(res)
}

func roundFloat64(f float64) float64 {
	if math.Abs(f) < 0.5 {
		return 0
	}
	if f < 0 {
		f -= 0.5
	} else {
		f += 0.5
	}
	return math.Trunc(f)
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

// GetIntPrefix gets prefix of string which can be successfully parsed as int.
func GetIntPrefix(s string, isCastFunc bool) string {
	if !isCastFunc {
		fstr := GetFloatPrefix(s)
		dec, _ := decimal.NewFromString(fstr)
		return dec.Round(0).String()
	}

	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return "0"
	}

	var (
		idx       = 0
		prefix    = 0
		sawdigits = false
	)

	if s[idx] == '+' || s[idx] == '-' {
		idx++
	}

	for ; idx < len(s); idx++ {
		if s[idx] >= '0' && s[idx] <= '9' {
			sawdigits = true
			prefix = idx + 1
			continue
		}
		break
	}
	if !sawdigits {
		return "0"
	}
	return s[:prefix]
}
