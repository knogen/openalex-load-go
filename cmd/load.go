/*
Copyright © 2023 ider <admin@knogen.com>
*/
package cmd

import (
	"openalex-load-go/internal/openalex/load"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		os.Setenv("ELASTICSEARCH_URL", "http://192.168.50.3:9201")
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

		foldPath := "/mnt/sata3/openalex/openalex-snapshot-v20231101/data"
		Version := "20231101"
		switch projectIndex {
		case 1:
			cp := load.NewConceptProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		case 2:
			cp := load.NewInstitutionProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		case 3:
			cp := load.NewPublisherProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		case 4:
			cp := load.NewFunderProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		case 5:
			cp := load.NewSourceProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		case 6:
			cp := load.NewAuthorProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		case 7:
			cp := load.NewWorkProject(foldPath)
			load.RuntimeFlow(cp, treadCount, Version)
		default:
			log.Warn().Msg("Please set project index")
		}
	},
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	rootCmd.AddCommand(loadCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// loadCmd.PersistentFlags().String("foo", "ccwzz", "A help for foo")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	loadCmd.Flags().IntP("treadCount", "t", 5, "how many thread count do you want?")
	loadCmd.Flags().IntP("project", "p", 0, `What object do you want load?
	1: concept
	2: instiution
	3: publisher
	4: funder
	5：source
	6: auther
	7: work
	choose one
	`)
}
