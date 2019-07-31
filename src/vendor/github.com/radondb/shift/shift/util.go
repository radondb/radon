/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"github.com/radondb/shift/xlog"
)

var sysDatabases = map[string]bool{
	"sys":                true,
	"mysql":              true,
	"information_schema": true,
	"performance_schema": true,
}

func logPanicHandler(log *xlog.Log, format string, v ...interface{}) {
	log.Fatal(format, v...)
}
