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
	"github.com/klauspost/compress/zstd" // 引入 zstd 库
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

func RuntimeToNDJSONFlow(c DataLoadInterface, runtimeCount int, version, outPath string, outFileCount int) {

	fileChan := make(chan string, 10000)
	for _, filePath := range c.GetProjectGzFiles() {
		fileChan <- filePath
	}
	close(fileChan)
	log.Info().Int("file count", len(c.GetProjectGzFiles())).Msg("all files loaded")

	mergeIDSet := c.GetMergeIDsSet()
	log.Info().Int("merge id count", mergeIDSet.Size()).Msg("merge ids loaded")

	jsonChan := make(chan []byte, 10000)
	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for range runtimeCount {
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
	for i := range outFileCount {

		jsonFilePath := fmt.Sprintf("%s_%s_p%v.jsonl.zst", c.GetProjectName(), version, i)
		jsonFilePath = filepath.Join(outPath, jsonFilePath)

		go func() {
			defer fileWg.Done()

			file, err := os.Create(jsonFilePath)
			if err != nil {
				fmt.Println("Error creating file:", err)
				return
			}
			defer file.Close()

			// 创建缓冲写入器
			writer := bufio.NewWriterSize(file, 100*1024*1024)
			defer writer.Flush()

			enc, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.SpeedDefault))
			if err != nil {
				log.Panic().Err(err).Msg("zstd writer create failed")
			}
			defer enc.Close()

			line := []byte("\n")
			for row := range jsonChan {
				_, err := enc.Write(row)
				if err != nil {
					log.Panic().Err(err)
				}
				_, err = enc.Write(line)
				if err != nil {
					log.Panic().Err(err)
				}

				bar.Add(1)
			}

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
