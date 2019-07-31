/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
)

func (h *EventHandler) DeleteRow(e *canal.RowsEvent) {
	var conn *client.Conn
	cfg := h.shift.cfg

	h.wg.Add(1)
	executeFunc := func(conn *client.Conn) {
		defer h.wg.Done()
		var keep = true

		pks := e.Table.PKColumns
		for i, row := range e.Rows {
			var values []string

			// keep connection in the loop, just put conn to pool when execute the last row
			if (i + 1) == len(e.Rows) {
				keep = false
			}

			// We have pk columns.
			if len(pks) > 0 {
				for _, pk := range pks {
					v := row[pk]
					values = append(values, fmt.Sprintf("%s=%s", e.Table.Columns[pk].Name, h.ParseValue(e, pk, v)))
				}
			} else {
				for j, v := range row {
					if v == nil {
						continue
					}
					values = append(values, fmt.Sprintf("%s=%s", e.Table.Columns[j].Name, h.ParseValue(e, j, v)))
				}
			}

			query := &Query{
				sql:       fmt.Sprintf("delete from `%s`.`%s` where %s", cfg.ToDatabase, cfg.ToTable, strings.Join(values, " and ")),
				typ:       QueryType_DELETE,
				skipError: false,
			}
			h.execute(conn, keep, query)
		}
	}

	if conn = h.shift.toPool.Get(); conn == nil {
		h.shift.err <- errors.Trace(errors.Errorf("shift.delete.get.to.conn.nil.error"))
	}

	executeFunc(conn)
}
