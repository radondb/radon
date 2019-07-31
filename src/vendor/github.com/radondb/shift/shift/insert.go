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

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
)

func (h *EventHandler) InsertRow(e *canal.RowsEvent, systemTable bool) {
	var conn *client.Conn
	cfg := h.shift.cfg
	h.wg.Add(1)

	executeFunc := func(conn *client.Conn) {
		defer h.wg.Done()
		var keep = true

		for i, row := range e.Rows {
			var values []string

			// keep connection in the loop, just put conn to pool when execute the last row
			if (i + 1) == len(e.Rows) {
				keep = false
			}

			for idx, v := range row {
				values = append(values, h.ParseValue(e, idx, v))
			}

			query := &Query{
				sql:       fmt.Sprintf("insert into `%s`.`%s` values (%s)", cfg.ToDatabase, cfg.ToTable, strings.Join(values, ",")),
				typ:       QueryType_INSERT,
				skipError: systemTable,
			}
			h.execute(conn, keep, query)
		}
	}

	if conn = h.shift.toPool.Get(); conn == nil {
		h.shift.panicMe("shift.insert.get.to.conn.nil.error")
	}
	// Binlog sync.
	if e.Header != nil {
		executeFunc(conn)
	} else {
		// Backend worker for mysqldump.
		go func(conn *client.Conn) {
			executeFunc(conn)
		}(conn)
	}
}
