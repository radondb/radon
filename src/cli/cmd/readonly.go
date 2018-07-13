/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"net/http"

	"xbase"

	"github.com/spf13/cobra"
)

func NewReadonlyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "readonly",
		Short: "disable/enable radon to readonly",
	}
	cmd.AddCommand(NewReadonlyEnableCommand())
	cmd.AddCommand(NewReadonlyDisableCommand())
	return cmd
}

func setReadonly(url string, readonly bool) {
	type request struct {
		ReadOnly bool `json:"readonly"`
	}

	req := &request{
		ReadOnly: readonly,
	}
	resp, cleanup, err := xbase.HTTPPut(url, &req)
	defer cleanup()

	if err != nil {
		log.Panicf("error:%+v", err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		log.Panicf("radoncli.set.readonly.to.[%v].url[%s].response.error:%+s", readonly, url, xbase.HTTPReadBody(resp))
	}
}

// enable readonly.
func NewReadonlyEnableCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "enable radon to readonly",
		Run:   readonlyEnableCommand,
	}
	return cmd
}

func readonlyEnableCommand(cmd *cobra.Command, args []string) {
	readonlyUrl := "http://127.0.0.1:8080/v1/radon/readonly"
	setReadonly(readonlyUrl, true)
}

// disable readonly.
func NewReadonlyDisableCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disable",
		Short: "disable radon readonly",
		Run:   readonlyDisableCommand,
	}
	return cmd
}

func readonlyDisableCommand(cmd *cobra.Command, args []string) {
	readonlyUrl := "http://127.0.0.1:8080/v1/radon/readonly"
	setReadonly(readonlyUrl, false)
}
