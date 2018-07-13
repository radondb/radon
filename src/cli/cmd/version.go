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

	"build"

	"github.com/spf13/cobra"
)

func NewVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version number of radon client",
		Run:   versionCommandFn,
	}

	return cmd
}

func versionCommandFn(cmd *cobra.Command, args []string) {
	build := build.GetInfo()
	fmt.Printf("radoncli:[%+v]\n", build)
}
