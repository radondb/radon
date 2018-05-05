/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"strconv"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func (spanner *Spanner) logEvent(session *driver.Session, typ string, query string) error {
	if spanner.conf.Binlog.EnableBinlog {
		spanner.binlog.LogEvent(typ, session.Schema(), query)
	}
	return nil
}

func (spanner *Spanner) handleShowBinlogEvents(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var ts int64
	limit := 100
	binloger := spanner.binlog

	log := spanner.log
	ast := node.(*sqlparser.Show)
	if ast.From != "" {
		gtid, err := strconv.ParseInt(ast.From, 0, 64)
		if err != nil {
			log.Error("spanner.send.binlog.parser.gtid[%v].error:%v", ast.From, err)
			return nil, err
		}
		ts = gtid
	}
	if ast.Limit != nil {
		rowcount := ast.Limit.Rowcount
		if rowcount != nil {
			val := rowcount.(*sqlparser.SQLVal)
			out, err := strconv.ParseInt(hack.String(val.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			limit = int(out)
		}
	}

	sqlworker, err := binloger.NewSQLWorker(ts)
	if err != nil {
		log.Error("spanner.send.binlog.new.sqlworker[from:%v, ts:%v].error:%v", ast.From, ts, err)
		return nil, err
	}
	defer binloger.CloseSQLWorker(sqlworker)

	qr := &sqltypes.Result{Fields: []*querypb.Field{
		{Name: "Log_name", Type: querypb.Type_VARCHAR},
		{Name: "Pos", Type: querypb.Type_INT64},
		{Name: "GTID", Type: querypb.Type_VARCHAR},
		{Name: "Event_type", Type: querypb.Type_VARCHAR},
		{Name: "Schema", Type: querypb.Type_VARCHAR},
		{Name: "End_log_pos", Type: querypb.Type_INT64},
		{Name: "Info", Type: querypb.Type_VARCHAR},
	},
	}

	counts := 0
	for {
		event, err := sqlworker.NextEvent()
		if err != nil {
			return nil, err
		}
		if event == nil {
			break
		}
		if counts >= limit {
			break
		}
		if event != nil {
			row := []sqltypes.Value{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(event.LogName)),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", event.Pos))),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", event.Timestamp))),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(event.Type)),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(event.Schema)),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", event.EndLogPos))),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(event.Query)),
			}
			qr.Rows = append(qr.Rows, row)
			counts++
		}
	}
	return qr, nil
}
