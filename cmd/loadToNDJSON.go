/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"openalex-load-go/internal/openalex/load"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// loadToNDJSONCmd represents the loadToCsv command
var loadToNDJSONCmd = &cobra.Command{
	Use:   "loadToNDJSON",
	Short: "load data to ndjson",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
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

		outPath, _ := cmd.Flags().GetString("out")
		_, err = os.Stat(outPath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("The path %s does not exist.\n", outPath)
				return
			}
			log.Printf("Error checking path existence: %v\n", err)
			return
		}

		Version, _ := cmd.Flags().GetString("version")
		log.Printf("OpenAlex version: %v\n", Version)

		outFileCount, _ := cmd.Flags().GetInt("outFileCount")

		// foldPath := fmt.Sprintf("/mnt/sata3/openalex/openalex-snapshot-v%s/data", Version)
		foldPath := fmt.Sprintf("/mnt/hg02/openalex-snapshot-v%s/data", Version)

		switch projectIndex {
		case 1:
			cp := load.NewConceptProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 2:
			cp := load.NewInstitutionProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 3:
			cp := load.NewPublisherProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 4:
			cp := load.NewFunderProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 5:
			cp := load.NewSourceProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 6:
			cp := load.NewAuthorProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 7:
			cp := load.NewWorkProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		case 8:
			cp := load.NewTopicProject(foldPath)
			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
		default:
			log.Warn().Msg("Please set project index")
		}
	},
}

func init() {
	rootCmd.AddCommand(loadToNDJSONCmd)

	loadToNDJSONCmd.Flags().IntP("treadCount", "t", 5, "how many thread count do you want?")
	loadToNDJSONCmd.Flags().IntP("project", "p", 0, `What object do you want load?
	1: concept
	2: instiution
	3: publisher
	4: funder
	5：source
	6: auther
	7: work
	8: topic
	choose one
	`)
	loadToNDJSONCmd.Flags().StringP("out", "O", "/tmp/", `out data path`)
	loadToNDJSONCmd.Flags().StringP("version", "v", "20241030", `openalex version`)
	loadToNDJSONCmd.Flags().IntP("outFileCount", "c", 1, `out put file Count`)
}
