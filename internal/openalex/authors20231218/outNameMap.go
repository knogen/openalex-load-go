package authors20231218

// 先分析 authors 文件，筛选出高创作者，记录需要的值，然后再分析 works 文件，记录详细的内容到每一年
// works: publication_year, concepts,  authorships->{author->id,countries,institutions,}
// authors: x_concepts, affiliations, counts_by_year, works_count, id
// affiliations 字段在 author 的导出文件中是没有的，我们需要从 works 中获取
// 作者数实在太多，统计高产作者的统计阈值
import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"openalex-load-go/internal/openalex/load"
	"os"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/rs/zerolog/log"
)

type ExportProject struct {
	*load.BaseProject
}

func NewExportProject(dataPath string, typeName string) *ExportProject {
	BaseProject := load.NewBaseProject(typeName, dataPath)
	return &ExportProject{BaseProject}
}

func (c *ExportProject) ParseData(obj map[string]interface{}) {

}

// get project data file
func handleExportFile(c load.DataLoadInterface, filePath string, mergeIDSet *hashset.Set, outChan chan idMapStruct) {
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

		// 忽略 merge id
		ID := shorten_url(obj["id"].(string))
		if mergeIDSet.Contains(ID) {
			continue
		}

		outChan <- idMapStruct{
			ID:   ID,
			Name: obj["display_name"].(string),
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

type idMapStruct struct {
	ID   string
	Name string
}

func ExportFlow(c load.DataLoadInterface, runtimeCount int, version string, flag string) {

	fileChan := make(chan string, 10000)
	outChan := make(chan idMapStruct, 10000)
	for _, filePath := range c.GetProjectGzFiles() {
		fileChan <- filePath
	}
	close(fileChan)
	log.Info().Msg("开始获取 mergeID")
	mergeIDSet := c.GetMergeIDsSet()
	// test
	// mergeIDSet := hashset.New()

	log.Info().Msg("获取完成 mergeID， 开始处理 concept")
	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for i := 0; i < runtimeCount; i++ {
		// handle file
		go func() {
			for filePath := range fileChan {
				handleExportFile(c, filePath, mergeIDSet, outChan)
			}
			wg.Done()
		}()
	}

	// 收集者

	outSync := sync.WaitGroup{}
	outSync.Add(1)

	go func() {
		result := []idMapStruct{}
		for obj := range outChan {
			result = append(result, obj)
		}

		parentPath := "/mnt/sas/home/ider/workspace/jupyter-lab/openalex_author_analysis/data/"
		dumpJsonGzipCsv(parentPath+flag+"_name_map.csv.json.gz", result)

		outSync.Done()
	}()

	wg.Wait()
	close(outChan)
	outSync.Wait()

	<-time.After(10 * time.Second)
	log.Info().Msg("全部处理完成")
}

func MainOutputMap() {

	foldPath := "/mnt/sata3/openalex/openalex-snapshot-v20231225/data"
	Version := "20231101"
	cp := NewExportProject(foldPath, "concepts")
	ExportFlow(cp, 10, Version, "concepts")
	cp = NewExportProject(foldPath, "institutions")
	ExportFlow(cp, 10, Version, "institutions")
}
