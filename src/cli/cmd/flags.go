/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"bytes"

	"github.com/spf13/cobra"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	log        = xlog.NewStdLog(xlog.Level(xlog.INFO))
	localFlags = LocalFlags{}
)

// LocalFlags are flags that defined for local.
type LocalFlags struct {
	gtid         int64
	maxWorkers   int
	parallelType int
}

func executeCommand(root *cobra.Command, args ...string) (output string, err error) {
	buf := new(bytes.Buffer)
	root.SetOutput(buf)
	root.SetArgs(args)

	_, err = root.ExecuteC()
	return buf.String(), err
}
