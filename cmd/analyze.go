/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// analyzeCmd represents the analyze command
var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "直接使用原始文件的分析代码",
	Long: `直接使用原始文件的分析代码
计划使用流式分析，先使用原始文件分析，然后使用流式分析，最后使用结果进行优化。`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("analyze called")
		
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// analyzeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// analyzeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
