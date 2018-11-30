/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFuzz(t *testing.T) {
	data := []byte("select * from t")
	r := Fuzz(data)

	assert.Equal(t, 1, r)
}
