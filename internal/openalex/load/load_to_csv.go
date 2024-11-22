package load

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

func RuntimeToCsvFlow(c DataLoadInterface, runtimeCount int, version, outPath string, outFileCount int) {
	jsonFilePath := fmt.Sprintf("%s_%s.json.gz-1", c.GetProjectName(), version)
	jsonFilePath = filepath.Join(outPath, jsonFilePath)
	// log.Info().Str("jsonFilePath", jsonFilePath).Msg("out file path")
	// log.Info().Int("runtionCount", runtimeCount).Msg("start")

	fileChan := make(chan string, 10000)
	for _, filePath := range c.GetProjectGzFiles() {
		fileChan <- filePath
	}
	close(fileChan)

	mergeIDSet := c.GetMergeIDsSet()

	jsonChan := make(chan []byte, 10000)
	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for i := 0; i < runtimeCount; i++ {
		// handle file
		go func() {
			for filePath := range fileChan {
				handleFileToJson(c, filePath, mergeIDSet, jsonChan)
			}
			wg.Done()
		}()
	}
	fileWg := sync.WaitGroup{}
	fileWg.Add(outFileCount)

	bar := progressbar.Default(-1)
	for i := 0; i < outFileCount; i++ {

		jsonFilePath := fmt.Sprintf("%s_%s_p%v.json.gz", c.GetProjectName(), version, i)
		jsonFilePath = filepath.Join(outPath, jsonFilePath)

		go func() {
			file, err := os.Create(jsonFilePath)
			if err != nil {
				fmt.Println("Error creating file:", err)
				return
			}
			// 创建缓冲写入器
			writer := bufio.NewWriterSize(file, 100*1024*1024)
			gzWriter := gzip.NewWriter(writer)

			line := []byte("\n")
			for row := range jsonChan {
				_, err := gzWriter.Write(row)
				if err != nil {
					log.Panic().Err(err)
				}
				_, err = gzWriter.Write(line)
				if err != nil {
					log.Panic().Err(err)
				}

				bar.Add(1)
			}
			gzWriter.Close()
			writer.Flush()
			file.Close()
			fileWg.Done()
		}()
	}

	wg.Wait()
	close(jsonChan)
	fileWg.Wait()
	log.Info().Str("project", c.GetProjectName()).Msg("project finish")

}

// get project data file
func handleFileToJson(c DataLoadInterface, filePath string, mergeIDSet *hashset.Set, jsonChan chan []byte) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Error().Err(err).Msg("file 读取失败")
		return
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 20*1024*1024)

	for scanner.Scan() {
		var obj map[string]interface{}
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			log.Panic().Err(err).Msg("file 读取失败")
			continue
		}
		c.ParseData(obj)

		// 忽略 merge id
		if mergeIDSet.Contains(obj["id"]) {
			continue
		}

		docJSON, err := json.Marshal(obj)
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal document")
		}
		jsonChan <- docJSON

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}
