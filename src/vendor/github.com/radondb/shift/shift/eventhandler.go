/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"strings"
	"sync"

	"github.com/radondb/shift/xlog"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
)

type QueryType int

const (
	QueryType_INSERT      QueryType = 0
	QueryType_DELETE      QueryType = 1
	QueryType_UPDATE      QueryType = 2
	QueryType_XA_ROLLBACK QueryType = 3
)

type Query struct {
	sql       string
	typ       QueryType
	skipError bool
}

type EventHandler struct {
	wg    sync.WaitGroup
	log   *xlog.Log
	shift *Shift
	canal.DummyEventHandler
}

func NewEventHandler(log *xlog.Log, shift *Shift) *EventHandler {
	return &EventHandler{
		log:   log,
		shift: shift,
	}
}

// OnRow used to handle the Insert/Delete/Update events.
func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	cfg := h.shift.cfg

	if e.Table.Schema == cfg.FromDatabase && e.Table.Name == cfg.FromTable {
		switch e.Action {
		case canal.InsertAction:
			_, isSystem := sysDatabases[strings.ToLower(e.Table.Schema)]
			if h.shift.cfg.ToFlavor == ToMySQLFlavor ||
				h.shift.cfg.ToFlavor == ToMariaDBFlavor {
				h.InsertMySQLRow(e, isSystem)
			} else {
				h.InsertRadonDBRow(e, isSystem)
			}

		case canal.DeleteAction:
			h.DeleteRow(e)
		case canal.UpdateAction:
			h.UpdateRow(e)
		default:
			return errors.Trace(errors.Errorf("shift.handler.unsupported.event[%+v]", e))
		}
	}
	return nil
}

// OnTableChanged used to handle the QueryEvent and XAEvent.
func (h *EventHandler) OnTableChanged(schema string, table string) error {
	cfg := h.shift.cfg

	if cfg.FromDatabase == schema && cfg.FromTable == table {
		return errors.Trace(errors.Errorf("shift.cant.do.ddl[%v, %v].during.shifting...", schema, table))
	}
	return nil
}

func (h *EventHandler) OnXA(e *canal.XAEvent) error {
	// We dont handle XA ROLLBACK
	if strings.Contains(string(e.Query), "XA ROLLBACK") {
		return errors.Trace(errors.Errorf("shift.handler.unsupported.XAQueryEvent[%+v]", e))
	}
	return nil
}

func (h *EventHandler) WaitWorkerDone() {
	h.wg.Wait()
}

func (h *EventHandler) execute(conn *client.Conn, keep bool, query *Query) {
	sql := query.sql
	log := h.log
	shift := h.shift
	pool := h.shift.toPool

	switch query.typ {
	case QueryType_INSERT, QueryType_DELETE, QueryType_UPDATE:
		{
			execFn := func() {
				if _, err := conn.Execute(sql); err != nil {
					if query.skipError {
						log.Error("shift.execute.sql[%s].error:%+v", sql, err)
					} else {
						log.Error("shift.execute.sql[%s].error", sql)
						shift.err <- errors.Trace(err)
					}
				}
			}

			execFn()
			if !keep {
				pool.Put(conn)
			}
		}
	}
}
