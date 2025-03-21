/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// writeMapToCSV 是一个泛型函数，用于将 map 数据写入 CSV 文件
func writeMapToCSV[K comparable, V any](m map[K]V, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("无法创建文件: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	if err := writer.Write([]string{"Key", "Value"}); err != nil {
		return fmt.Errorf("无法写入表头: %w", err)
	}

	// 写入数据
	for k, v := range m {
		if err := writer.Write([]string{fmt.Sprintf("%v", k), fmt.Sprintf("%v", v)}); err != nil {
			return fmt.Errorf("无法写入数据: %w", err)
		}
	}

	return nil
}

// statisTopicCmd represents the statisTopic command
var statisTopicCmd = &cobra.Command{
	Use:   "statisTopic",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("statisTopic called")
		path := "/mnt/sata3/openalex/parse_output/"

		// 获取 path 目录下所有以 works_ 开头的 .gz 文件
		files, err := filepath.Glob(filepath.Join(path, "works_20241201*.gz"))
		if err != nil {
			fmt.Printf("Error getting files: %v\n", err)
			return
		}
		for _, file := range files {
			fmt.Println(file)
		}

		// 定义 WorkTopic 结构体
		type WorkTopic struct {
			ID              string `json:"id"`
			PublicationYear int    `json:"publication_year"`
			Topics          []struct {
				ID string `json:"id"`
			} `json:"topics"`
		}

		// 定义一个映射来存储每个主题最早出现的年份
		topicAppearYear := make(map[string]int)
		missTopicCountYear := make(map[int]int)
		existTopicCountYear := make(map[int]int)
		// 消费者
		workChan := make(chan *WorkTopic, 1000)

		var wgConsumer sync.WaitGroup
		wgConsumer.Add(1)
		go func() {
			bar := progressbar.Default(-1, "load to cache")
			defer bar.Close()
			defer wgConsumer.Done()
			for work := range workChan {
				for _, topic := range work.Topics {
					topicID := topic.ID
					if _, ok := topicAppearYear[topicID]; !ok {
						topicAppearYear[topicID] = work.PublicationYear
					} else {
						if work.PublicationYear < topicAppearYear[topicID] {
							topicAppearYear[topicID] = work.PublicationYear
						}
					}
				}
				if len(work.Topics) == 0 {
					missTopicCountYear[work.PublicationYear] += 1
				} else {
					existTopicCountYear[work.PublicationYear] += 1
				}
				bar.Add(1)
			}

			writeMapToCSV(missTopicCountYear, "/tmp/missTopicCountYear.csv")
			writeMapToCSV(existTopicCountYear, "/tmp/existTopicCountYear.csv")
			writeMapToCSV(topicAppearYear, "/tmp/topicAppearYear.csv")

		}()

		// 生产者
		// 读取文件并解析 JSON
		var wg sync.WaitGroup
		for _, file := range files {
			wg.Add(1)
			go func(filePath string) {
				defer wg.Done()
				file, err := os.Open(filePath)
				if err != nil {
					fmt.Printf("Error opening file %s: %v\n", filePath, err)
					return
				}
				defer file.Close()

				// 解压缩 .gz 文件
				gzReader, err := gzip.NewReader(file)
				if err != nil {
					fmt.Printf("Error decompressing file %s: %v\n", filePath, err)
					return
				}
				defer gzReader.Close()

				// 使用解压缩后的读取器创建 scanner
				scanner := bufio.NewScanner(gzReader)
				// Increase the buffer size
				const maxCapacity = 1024 * 1024 * 100 // 100MB
				buf := make([]byte, 0, maxCapacity)
				scanner.Buffer(buf, maxCapacity)

				for scanner.Scan() {
					var workTopic WorkTopic
					err := json.Unmarshal(scanner.Bytes(), &workTopic)
					if err != nil {
						fmt.Printf("Error parsing JSON from %s: %v\n", filePath, err)
						continue
					}
					workChan <- &workTopic
					// break
					// 处理解析后的 WorkTopic
					// fmt.Printf("Parsed WorkTopic from %s: %+v\n", filePath, workTopic)
				}

				if err := scanner.Err(); err != nil {
					fmt.Printf("Error reading file %s: %v\n", filePath, err)
				}
			}(file)
		}
		wg.Wait()
		close(workChan)

		wgConsumer.Wait()
	},
}

func init() {
	rootCmd.AddCommand(statisTopicCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// statisTopicCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// statisTopicCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
