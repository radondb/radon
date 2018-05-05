/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package main

import (
	"fmt"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func main() {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	address := fmt.Sprintf(":4407")
	client, err := driver.NewConn("mock", "mock", address, "", "")
	if err != nil {
		log.Panic("client.new.connection.error:%+v", err)
	}
	defer client.Close()

	qr, err := client.FetchAll("SELECT * FROM MOCK", -1)
	if err != nil {
		log.Panic("client.query.error:%+v", err)
	}
	log.Info("results:[%+v]", qr.Rows)
}
