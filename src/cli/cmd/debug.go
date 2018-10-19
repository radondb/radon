/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"fmt"

	"xbase"

	"github.com/spf13/cobra"
)

// NewDebugCommand creates new DebugCommand.
func NewDebugCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "show radon config, including configz/backendz/schemaz",
	}
	cmd.AddCommand(NewDebugConfigzCommand())
	cmd.AddCommand(NewDebugBackendzCommand())
	cmd.AddCommand(NewDebugSchemazCommand())
	cmd.PersistentFlags().StringVar(&radonHost, "radon-host", "127.0.0.1", "--radon-host=[ip]")
	return cmd
}

// NewDebugConfigzCommand is used to show config.
func NewDebugConfigzCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "configz",
		Short: "show radon configz",
		Run:   debugConfigzCommand,
	}
	return cmd
}

func debugConfigzCommand(cmd *cobra.Command, args []string) {
	configzURL := "http://" + radonHost + ":8080/v1/debug/configz"
	resp, err := xbase.HTTPGet(configzURL)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Print(resp)
}

// NewDebugBackendzCommand is used to show backend info.
func NewDebugBackendzCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backendz",
		Short: "show radon backendz",
		Run:   debugBackendzCommand,
	}
	return cmd
}

func debugBackendzCommand(cmd *cobra.Command, args []string) {
	backendzURL := "http://" + radonHost + ":8080/v1/debug/backendz"
	resp, err := xbase.HTTPGet(backendzURL)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Print(resp)
}

// NewDebugSchemazCommand is used to show schema info.
func NewDebugSchemazCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schemaz",
		Short: "show radon schemaz",
		Run:   debugSchemazCommand,
	}
	return cmd
}

func debugSchemazCommand(cmd *cobra.Command, args []string) {
	schemazURL := "http://" + radonHost + ":8080/v1/debug/schemaz"
	resp, err := xbase.HTTPGet(schemazURL)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Print(resp)
}
