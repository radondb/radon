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

func NewTwopcCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "twopc",
		Short: "disable/enable radon to twopc",
	}
	cmd.AddCommand(NewTwopcEnableCommand())
	cmd.AddCommand(NewTwopcDisableCommand())
	cmd.PersistentFlags().StringVar(&radonHost, "radon-host", "127.0.0.1", "--radon-host=[ip]")
	return cmd
}

func setTwopc(url string, twopc bool) {
	type request struct {
		Twopc bool `json:"twopc"`
	}

	req := &request{
		Twopc: twopc,
	}
	resp, cleanup, err := xbase.HTTPPut(url, &req)
	defer cleanup()

	if err != nil {
		log.Panicf("error:%+v", err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		log.Panicf("radoncli.set.twopc.to.[%v].url[%s].response.error:%+s", twopc, url, xbase.HTTPReadBody(resp))
	}
}

// NewTwopcEnableCommand enable twopc.
func NewTwopcEnableCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "enable radon to twopc",
		Run:   twopcEnableCommand,
	}
	return cmd
}

func twopcEnableCommand(cmd *cobra.Command, args []string) {
	twopcUrl := "http://" + radonHost + ":8080/v1/radon/twopc"
	setTwopc(twopcUrl, true)
}

// NewTwopcDisableCommand disable twopc.
func NewTwopcDisableCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disable",
		Short: "disable radon twopc",
		Run:   twopcDisableCommand,
	}
	return cmd
}

func twopcDisableCommand(cmd *cobra.Command, args []string) {
	twopcUrl := "http://" + radonHost + ":8080/v1/radon/twopc"
	setTwopc(twopcUrl, false)
}
