/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatImpossibleQuery(t *testing.T) {
	querys := []string{"select a,b from A where A.id>1 group by a order by a limit 1",
		"select id,a from A union select name,a from B order by a",
		"insert into A(a,b) values(1,'a')"}
	wants := []string{"select a, b from A where 1 != 1 group by a",
		"select id, a from A union select name, a from B",
		"insert into A(a, b) values (1, 'a')"}
	for i, query := range querys {
		node, err := Parse(query)
		assert.Nil(t, err)
		buf := NewTrackedBuffer(nil)
		FormatImpossibleQuery(buf, node)
		got := buf.String()
		assert.Equal(t, wants[i], got)
	}
}
