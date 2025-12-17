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

// 1. 提取工厂方法：消除重复的 switch case
// 根据 index 和路径创建一个新的 Project 实例
func createProjectLoader(index int, dataPath string) load.DataLoadInterface {
	switch index {
	case 1:
		return load.NewConceptProject(dataPath)
	case 2:
		return load.NewInstitutionProject(dataPath)
	case 3:
		return load.NewPublisherProject(dataPath)
	case 4:
		return load.NewFunderProject(dataPath)
	case 5:
		return load.NewSourceProject(dataPath)
	case 6:
		return load.NewAuthorProject(dataPath)
	case 7:
		return load.NewWorkProject(dataPath)
	case 8:
		return load.NewTopicProject(dataPath)
	case 9:
		return load.NewFieldProject(dataPath)
	case 10:
		return load.NewSubfieldsProject(dataPath)
	default:
		return nil
	}
}

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

		// --- 2. Phase 1: 处理主数据 (Data) ---
		log.Info().Msg(">>> Phase 1: Processing Main DATA")
		// foldPath := fmt.Sprintf("/mnt/sata3/openalex/openalex-snapshot-v%s/data", Version)
		foldPath := fmt.Sprintf("/mnt/hg02/openalex-snapshot-v%s/data", Version)
		cpData := createProjectLoader(projectIndex, foldPath)
		if cpData == nil {
			log.Fatal().Int("index", projectIndex).Msg("invalid project index")
		}
		processedSet := load.RuntimeToNDJSONFlow(cpData, treadCount, Version, outPath, outFileCount, nil, "Walden")

		log.Info().Int64("count", processedSet.Size()).Msg("Phase 1 Complete. Starting Legacy...")
		// --- 3. Phase 2: 处理遗留数据 (Legacy) ---
		log.Info().Msg(">>> Phase 2: Processing LEGACY DATA")
		foldPath = fmt.Sprintf("/mnt/hg02/openalex-snapshot-v%s/legacy-data", Version)
		cpLegacy := createProjectLoader(projectIndex, foldPath)
		load.RuntimeToNDJSONFlow(cpLegacy, treadCount, Version, outPath, outFileCount, processedSet, "legacy")

		log.Info().Msg(">>> All Done")
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
	9: field
	10: subfields
	choose one
	`)
	loadToNDJSONCmd.Flags().StringP("out", "O", "/tmp/", `out data path`)
	loadToNDJSONCmd.Flags().StringP("version", "v", "20241030", `openalex version`)
	loadToNDJSONCmd.Flags().IntP("outFileCount", "c", 1, `out put file Count`)
}
