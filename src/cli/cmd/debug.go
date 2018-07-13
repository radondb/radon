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

func NewDebugCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "show radon config, including configz/backendz/schemaz",
	}
	cmd.AddCommand(NewDebugConfigzCommand())
	cmd.AddCommand(NewDebugBackendzCommand())
	cmd.AddCommand(NewDebugSchemazCommand())
	return cmd
}

func NewDebugConfigzCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "configz",
		Short: "show radon configz",
		Run:   debugConfigzCommand,
	}
	return cmd
}

func debugConfigzCommand(cmd *cobra.Command, args []string) {
	configzUrl := "http://127.0.0.1:8080/v1/debug/configz"
	resp, err := xbase.HTTPGet(configzUrl)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Printf(resp)
}

func NewDebugBackendzCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backendz",
		Short: "show radon backendz",
		Run:   debugBackendzCommand,
	}
	return cmd
}

func debugBackendzCommand(cmd *cobra.Command, args []string) {
	backendzUrl := "http://127.0.0.1:8080/v1/debug/backendz"
	resp, err := xbase.HTTPGet(backendzUrl)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Printf(resp)
}

func NewDebugSchemazCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schemaz",
		Short: "show radon schemaz",
		Run:   debugSchemazCommand,
	}
	return cmd
}

func debugSchemazCommand(cmd *cobra.Command, args []string) {
	schemazUrl := "http://127.0.0.1:8080/v1/debug/schemaz"
	resp, err := xbase.HTTPGet(schemazUrl)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Printf(resp)
}
