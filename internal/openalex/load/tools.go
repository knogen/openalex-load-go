package load

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"openalex-load-go/internal/utils"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/rs/zerolog/log"
)

// set ELASTICSEARCH_URL environment variable,
func getElasticClient() *elasticsearch8.Client {
	es8, _ := elasticsearch8.NewDefaultClient()
	return es8
}

func initElastic(projectName, esIndex string, esClient *elasticsearch8.Client) {
	rootDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	mapping, err := os.ReadFile(filepath.Join(rootDir, "./data/mapping/"+projectName+".json"))
	if err != nil {
		log.Panic().Err(err).Msg("Failed to read mapping file")
	}
	settings, err := os.ReadFile(filepath.Join(rootDir, "./data/mapping/setting.json"))
	if err != nil {
		log.Panic().Err(err).Msg("Failed to read mapping file")
	}

	var settingObj, mappingObj map[string]interface{}
	err = json.Unmarshal(mapping, &mappingObj)
	if err != nil {
		log.Panic().Err(err).Msg("mapping Unmarshal fail")
	}
	err = json.Unmarshal(settings, &settingObj)
	if err != nil {
		log.Panic().Err(err).Msg("settings Unmarshal fail")
	}

	indexSettings := map[string]interface{}{
		"settings": settingObj,
		"mappings": mappingObj,
	}
	indexSettingsJSON, err := json.Marshal(indexSettings)
	if err != nil {
		log.Panic().Err(err).Msg("json Marshal fail")
	}
	res, err := esClient.Indices.Create(
		esIndex,
		esClient.Indices.Create.WithBody(bytes.NewReader(indexSettingsJSON)),
	)
	if err != nil {
		log.Panic().Err(err).Msg("elastic index build file")
	}
	log.Printf("%v+", res)
	log.Info().Msg("elastic index build success")
	defer res.Body.Close()
}

// get project data file
func handleFile(c DataLoadInterface, filePath string, mergeIDSet *hashset.Set, BulkIndexer esutil.BulkIndexer) {
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
		// mg := mergeIDSet.Values()
		// log.Panic().Any("objID", obj["id"]).Any("mergeID", mg[0]).Msg("check")

		docJSON, err := json.Marshal(obj)
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal document")
		}

		err = BulkIndexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",
				// DocumentID is the optional document ID
				DocumentID: obj["id"].(string),

				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(docJSON),

				// OnFailure is the optional callback for each failed operation
				OnFailure: func(
					ctx context.Context,
					item esutil.BulkIndexerItem,
					res esutil.BulkIndexerResponseItem, err error,
				) {
					if err != nil {
						log.Error().Err(err).Msg("bulk index error")
					} else {
						log.Error().Str("type", res.Error.Type).Str("Reason", res.Error.Reason).Msg("bulk index error")
					}
				},
			},
		)
		if err != nil {
			log.Error().Err(err).Msg("Unexpected error")
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

func RuntimeFlow(c DataLoadInterface, runtimeCount int, version string) {
	esIndexName := c.GetProjectName() + "_" + version
	esClient := getElasticClient()
	initElastic(c.GetProjectName(), esIndexName, esClient)
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     esClient,    // The Elasticsearch client
		Index:      esIndexName, // The default index name
		NumWorkers: 20,          // The number of worker goroutines (default: number of CPUs)
		FlushBytes: 5e+7,        // The flush threshold in bytes (default: 5M)
	})
	if err != nil {
		log.Panic().Err(err).Msg("Error creating the indexer")
	}

	fileChan := make(chan string, 10000)
	for _, filePath := range c.GetProjectGzFiles() {
		fileChan <- filePath
	}
	close(fileChan)

	mergeIDSet := c.GetMergeIDsSet()

	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for i := 0; i < runtimeCount; i++ {
		// handle file
		go func() {
			for filePath := range fileChan {
				handleFile(c, filePath, mergeIDSet, indexer)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	indexer.Close(context.Background())
	log.Info().Str("project", c.GetProjectName()).Msg("project finish")
	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		log.Warn().Uint64("failed", stats.NumFailed).Msg("bulkindex fail")
	}
}

func iteratorList(unKnowObj interface{}) (ret []interface{}) {
	if unKnowObj == nil {
		return
	}
	for _, item := range unKnowObj.([]interface{}) {
		ret = append(ret, item)
	}
	return
}

func shorten_url(unKnowObj interface{}, keyList []string) {
	if unKnowObj == nil {
		return
	}
	obj := unKnowObj.(map[string]interface{})
	for _, key := range keyList {
		if value, ok := obj[key]; ok {
			if value == nil {
				continue
			}
			if url, ok := value.(string); ok {
				parts := strings.Split(url, "/")
				lastPart := parts[len(parts)-1]
				obj[key] = lastPart
			} else if url, ok := value.(int); ok {
				obj[key] = strconv.Itoa(url)
			} else {
				log.Error().Msg("UnKnow Object:" + fmt.Sprintf("%v+", obj[key]))
			}
		}
	}
}

func remove_key(unKnowObj interface{}, keyList []string) {
	if unKnowObj == nil {
		return
	}
	obj := unKnowObj.(map[string]interface{})
	for _, key := range keyList {
		if _, ok := obj[key]; ok {
			delete(obj, key)
		}
	}
}

func remove_empty_key(unKnowObj interface{}) {
	if unKnowObj == nil {
		return
	}
	obj := unKnowObj.(map[string]interface{})
	for key, value := range obj {
		switch v := value.(type) {
		case []interface{}:
			if len(v) == 0 {
				delete(obj, key)
			}
		case map[string]interface{}:
			if len(v) == 0 {
				delete(obj, key)
			}
		case string:
			if v == "" {
				delete(obj, key)
			}
		case nil:
			delete(obj, key)
		}
	}
}

func shorten_doi(unKnowObj interface{}) {
	if unKnowObj == nil {
		return
	}
	obj := unKnowObj.(map[string]interface{})
	if value, ok := obj["doi"]; ok {
		if url, ok := value.(string); ok {
			obj["doi"] = strings.Replace(url, "https://doi.org/", "", 1)
		}
	}
}

func shorten_id_form_list(unKnowObj interface{}) []string {
	if unKnowObj == nil {
		return []string{}
	}
	var result []string
	if objArray, ok := unKnowObj.([]interface{}); ok {

		for _, item := range objArray {
			parts := strings.Split(item.(string), "/")
			lastPart := parts[len(parts)-1]
			result = append(result, lastPart)
		}
	} else {
		result = append(result, "aaa")
	}
	return result
}

func unAbstractInvertedIndex(unKnowObject interface{}) string {
	if unKnowObject == nil {
		return ""
	}
	// if abstractInvertedIndex, ok := unKnowObject.(map[string][]int); ok {

	wordIndex := make([][2]interface{}, 0)
	for k, v := range unKnowObject.(map[string]interface{}) {
		for _, index := range v.([]interface{}) {
			wordIndex = append(wordIndex, [2]interface{}{k, index.(float64)})
		}
	}
	sort.Slice(wordIndex, func(i, j int) bool {
		return wordIndex[i][1].(float64) < wordIndex[j][1].(float64)
	})
	result := make([]string, len(wordIndex))
	for i, item := range wordIndex {
		result[i] = item[0].(string)
	}
	return strings.Join(result, " ")
	// }
}

func getMergeIDs(projectName, dataPath string) *hashset.Set {

	projectSet := hashset.New()
	projectSet.Add("authors", "institutions", "publishers", "sources", "works")
	if !projectSet.Contains(projectName) {
		log.Warn().Str("project", projectName).Msg("mergeID miss")
	}

	// walk fold
	rootPath := path.Join(dataPath, "merged_ids/"+projectName)
	files := getSubPathGzFiles(rootPath)

	mergeIDSet := hashset.New()
	for _, file := range files {
		file, err := os.Open(file)
		if err != nil {
			log.Panic().Err(err).Msg("mergeID 读取失败")
			return nil
		}
		defer file.Close()

		gz, err := gzip.NewReader(file)
		if err != nil {
			log.Error().Err(err).Msg("file 读取失败")
			return nil
		}
		defer gz.Close()

		reader := csv.NewReader(gz)

		head, err := reader.Read()
		ID := utils.IndexOf(head, "id")

		for {
			record, err := reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Error().Err(err).Msg("mergeID 读取失败")
			}
			mergeIDSet.Add(record[ID])
		}
	}
	return mergeIDSet
}

func getSubPathGzFiles(rootPath string) []string {
	files := []string{}
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".gz") {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		log.Panic().Err(err)
	}
	return files
}
