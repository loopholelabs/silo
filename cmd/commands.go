package main

import "github.com/spf13/cobra"

var (
	rootCmd = &cobra.Command{
		Use:           "silo",
		Short:         "silo storage engine.",
		Long:          ``,
		SilenceErrors: true,
		SilenceUsage:  true,
	}
)

func init() {
	// rootCmd.PersistentFlags().StringVarP(&Input, "input", "i", "", "Input file name")
}

func Execute() error {
	return rootCmd.Execute()
}
