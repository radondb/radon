/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xcontext

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXContext(t *testing.T) {
	q1 := QueryTuple{Query: "select b1", Backend: "b1"}
	q2 := QueryTuple{Query: "select a2", Backend: "a2"}
	q3 := QueryTuple{Query: "select 00", Backend: "00"}
	querys := []QueryTuple{q1, q2, q3}

	sort.Sort(QueryTuples(querys))
	assert.Equal(t, querys[0], q3)
	assert.Equal(t, querys[1], q2)
	assert.Equal(t, querys[2], q1)
}
