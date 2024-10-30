/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"openalex-load-go/internal/openalex/authors20231218"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// authors20231218Cmd represents the authors20231218 command
var authors20231218Cmd = &cobra.Command{
	Use:   "authors20231218",
	Short: "2023-12-18阶段的分析任务",
	Long:  `找到高产作者`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Msg("project start")
		// log.Print(cmd.Flags().GetString("foo"))
		projectIndex, err := cmd.Flags().GetInt("project")
		if err != nil {
			log.Error().Err(err).Msg("project flag error")
		}
		treadCount, err := cmd.Flags().GetInt("treadCount")
		if err != nil {
			log.Error().Err(err).Msg("treadCount flag error")
		}
		if treadCount < 1 || treadCount > 50 {
			log.Warn().Msg("your thread count is bad, let us set treadCount=5")
			treadCount = 5
		}

		foldPath := "/mnt/sata3/openalex/openalex-snapshot-v20231225/data"
		Version := "20231101"

		switch projectIndex {
		case 1:
			cp := authors20231218.NewAuthorProject(foldPath)
			authors20231218.AuthorsFlow(cp, treadCount, Version)
		case 2:
			cp := authors20231218.NewWorkProject(foldPath)
			authors20231218.WorksFlow(cp, treadCount, Version)
		case 3:
			authors20231218.MainOutputMap()
		}

	},
}

func init() {
	analyzeCmd.AddCommand(authors20231218Cmd)

	authors20231218Cmd.Flags().IntP("treadCount", "t", 10, "how many thread count do you want?")
	authors20231218Cmd.Flags().IntP("project", "p", 0, `What object do you want load?
	1: authors "first"
	2: works "second"
	3: id name map (concept, institution) "three"
	choose one
	`)
}
