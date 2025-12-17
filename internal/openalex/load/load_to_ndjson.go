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
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

// 修改函数签名：
// 1. 输入增加 excludeSet (用于 legacy 阶段排除已处理 ID，第一阶段传 nil 即可)
// 2. 返回 *hashset.Set (返回本次运行所有处理成功的 ID，供下一阶段使用)
func RuntimeToNDJSONFlow(c DataLoadInterface, runtimeCount int, version, outPath string, outFileCount int, excludeSet *ShardedSet, outFileSuffix string) *ShardedSet {

	// 1. 初始化当前阶段的收集器
	currentSet := NewShardedSet()

	// 如果是 Phase 1，excludeSet 可能是 nil，我们只在 Phase 2 用它来过滤
	// 但为了代码统一，如果 nil 就不检查
	hasExcludeSet := excludeSet != nil

	// 合并 mergeIDSet 和 excludeSet 的逻辑
	// 注意：这里我们不直接修改 c.GetMergeIDsSet()，而是在 worker 中同时检查两个集合
	mergeIDSet := c.GetMergeIDsSet()
	// mergeIDSet := hashset.New()
	log.Info().Int("merge_id_count", mergeIDSet.Size()).Msg("merge filters loaded")
	if hasExcludeSet {
		log.Info().Int64("exclude_set_count", excludeSet.Size()).Msg("legacy filters loaded")
	}

	fileChan := make(chan string, 10000)
	// 加载文件列表
	files := c.GetProjectGzFiles()
	// files = files[:20]
	for _, filePath := range files {
		fileChan <- filePath
	}
	close(fileChan)
	log.Info().Int("file_count", len(files)).Msg("all files loaded")

	// 核心通道
	jsonChan := make(chan []byte, 10000)

	// 3. 启动处理 Workers
	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for range runtimeCount {
		go func() {
			defer wg.Done()
			for filePath := range fileChan {
				// 传递 idChan 进去
				handleFileToJson(c, filePath, mergeIDSet, excludeSet, currentSet, jsonChan)
			}
		}()
	}

	// 4. 启动写入协程 (Writer Workers)
	fileWg := sync.WaitGroup{}
	fileWg.Add(outFileCount)
	bar := progressbar.Default(-1)

	for i := range outFileCount {
		// 修改文件名，增加 distinct 标记或其他命名逻辑，防止覆盖（如果 data 和 legacy 输出到同目录）
		// 建议调用方控制 outPath 区分，或者文件名加上前缀
		jsonFilePath := fmt.Sprintf("%s_%s_p%v_%s.jsonl.zst", c.GetProjectName(), version, i, outFileSuffix)
		jsonFilePath = filepath.Join(outPath, jsonFilePath)

		go func(path string) {
			defer fileWg.Done()
			file, err := os.Create(path)
			if err != nil {
				log.Error().Err(err).Msg("create file error")
				return
			}
			defer file.Close()

			writer := bufio.NewWriterSize(file, 100*1024*1024)
			defer writer.Flush()

			enc, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.SpeedDefault))
			if err != nil {
				log.Panic().Err(err).Msg("zstd writer create failed")
			}
			defer enc.Close()

			lineSep := []byte("\n")
			for row := range jsonChan {
				enc.Write(row)
				enc.Write(lineSep)
				bar.Add(1)
			}
		}(jsonFilePath)
	}

	// 5. 等待所有任务完成
	wg.Wait()       // 等待处理完成
	close(jsonChan) // 关闭数据通道
	fileWg.Wait()   // 等待写入完成
	bar.Close()
	log.Info().Str("project", c.GetProjectName()).Int64("total_unique_processed", currentSet.Size()).Msg("batch finish")

	return currentSet
}

// handleFileToJson 核心逻辑优化
// 参数说明：
// - mergeIDSet: 官方提供的合并 ID 列表 (hashset)
// - excludeSet: 上一阶段处理过的 ID 集合 (用于 Legacy 阶段过滤) (*ShardedSet)
// - currentSet: 当前阶段要收集的 ID 集合 (用于 Phase 1 返回给 Phase 2) (*ShardedSet)
func handleFileToJson(c DataLoadInterface, filePath string, mergeIDSet *hashset.Set, excludeSet, currentSet *ShardedSet, jsonChan chan []byte) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Error().Err(err).Msg("open file failed")
		return
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Error().Err(err).Msg("gzip reader failed")
		return
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 20*1024*1024)

	// 本地 buffer：用于批量提交 ID 到 currentSet，减少锁竞争
	// 存储的是 uint64 hash，而不是 string
	localHashBuffer := make([]uint64, 0, 1000)

	for scanner.Scan() {
		var obj map[string]interface{}
		// 性能优化：对于海量小对象，考虑使用 json-iterator/go 或 gjson
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			log.Error().Err(err).Msg("json unmarshal failed")
			continue
		}

		c.ParseData(obj)

		id, ok := obj["id"].(string)
		if !ok {
			continue // 没有 ID 的数据无法处理
		}

		// 核心过滤逻辑：
		// 1. 检查是否在官方 Merge 列表中
		// 2. 检查是否在上一轮 (legacy check) 的排除列表中
		if mergeIDSet.Contains(id) || excludeSet.Contains(id) {
			continue
		}

		docJSON, err := json.Marshal(obj)
		if err != nil {
			log.Error().Err(err).Msg("json marshal failed")
			continue
		}

		jsonChan <- docJSON

		idHash := hashID(id)
		localHashBuffer = append(localHashBuffer, idHash)

		// 如果 buffer 满了，批量写入全局 ShardedSet
		if len(localHashBuffer) >= 1000 {
			currentSet.AddBatch(localHashBuffer)
			localHashBuffer = localHashBuffer[:0] // 清空切片但保留容量
		}
	}
	// 处理剩余的 buffer
	if len(localHashBuffer) > 0 {
		currentSet.AddBatch(localHashBuffer)
	}
}
