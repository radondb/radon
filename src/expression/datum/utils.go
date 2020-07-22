/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"math"

	"github.com/shopspring/decimal"
)

// isDecimalInf used to check whether decimal overflow.
func isDecimalInf(d decimal.Decimal) bool {
	v, _ := d.Float64()
	if math.IsInf(v, 0) {
		return true
	}
	return false
}

// TernaryOpt is ternary operator.
func TernaryOpt(condition bool, trueVal, falseVal interface{}) interface{} {
	if condition {
		return trueVal
	}
	return falseVal
}
