/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"openalex-load-go/internal/openalex/load"

	"github.com/spf13/cobra"
)

// loadToMongoCmd represents the loadToMongo command
var loadToMongoCmd = &cobra.Command{
	Use:   "loadToMongo",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("loadToMongo called")

		foldPath := fmt.Sprintf("/mnt/sata3/openalex/openalex-snapshot-v%s/data", "20241031")
		cp := load.NewWorkProject(foldPath)
		load.RuntimeToMongoFlow(cp, 30)
	},
}

func init() {
	rootCmd.AddCommand(loadToMongoCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// loadToMongoCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// loadToMongoCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}