/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package datum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIField(t *testing.T) {
	tcases := []struct {
		field  *IField
		resTyp ResultType
		dec    uint32
	}{
		{
			field:  &IField{StringResult, 0, false, false},
			resTyp: RealResult,
			dec:    31,
		},
		{
			field:  &IField{TimeResult, 2, false, false},
			resTyp: DecimalResult,
			dec:    2,
		},
		{
			field:  &IField{DurationResult, 0, false, false},
			resTyp: IntResult,
			dec:    0,
		},
	}
	for _, tcase := range tcases {
		field := tcase.field
		field.ToNumeric()
		assert.Equal(t, tcase.resTyp, field.ResTyp)
		assert.Equal(t, tcase.dec, field.Decimal)
	}
}
