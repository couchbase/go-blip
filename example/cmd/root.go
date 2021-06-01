/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:   "goblip",
	Short: "go-blip examples",
	Long: `go-blip examples`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}