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