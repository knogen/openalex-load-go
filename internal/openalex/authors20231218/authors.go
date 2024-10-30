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

type AuthorProject struct {
	*load.BaseProject
}

func NewAuthorProject(dataPath string) *AuthorProject {
	BaseProject := load.NewBaseProject("authors", dataPath)
	return &AuthorProject{BaseProject}
}

func (c *AuthorProject) ParseData(obj map[string]interface{}) {

}

// get project data file
func handleAuthorsFile(c load.DataLoadInterface, filePath string, mergeIDSet *hashset.Set, outChan chan AuthorStruct) {
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
		var obj AuthorStruct
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			log.Panic().Err(err).Msg("file 读取失败")
			continue
		}

		// 忽略 merge id
		obj.ID = shorten_url(obj.ID)
		if mergeIDSet.Contains(obj.ID) {
			continue
		}

		// 处理 obj 满足条件的放行
		upper_paper_count := 24
		sendFlag := false
		if obj.WorksCount > 100 {
			sendFlag = true
		} else {
			totalWorkCount := 0
			for _, stats := range obj.CountsByYear {
				if stats.WorksCount >= upper_paper_count {
					sendFlag = true
					break
				}
				totalWorkCount += stats.WorksCount
			}
			// 近10年前的作者创作数超过 upper_paper_count 的
			if obj.WorksCount-totalWorkCount >= upper_paper_count {
				sendFlag = true
			}
		}
		if sendFlag {
			// 处理concept, 只保留最高的 lv0, lv1
			for _, item := range obj.XConcepts {
				item.ID = shorten_url(item.ID)
			}
			outChan <- obj
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

func AuthorsFlow(c load.DataLoadInterface, runtimeCount int, version string) {

	fileChan := make(chan string, 10000)
	outChan := make(chan AuthorStruct, 10000)
	for _, filePath := range c.GetProjectGzFiles() {
		fileChan <- filePath
		// if i > 20 {
		// 	break
		// }
	}
	close(fileChan)
	log.Info().Msg("开始获取 mergeID")
	mergeIDSet := c.GetMergeIDsSet()
	// test
	// mergeIDSet := hashset.New()

	log.Info().Msg("获取完成 mergeID， 开始处理 authors")
	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for i := 0; i < runtimeCount; i++ {
		// handle file
		go func() {
			for filePath := range fileChan {
				handleAuthorsFile(c, filePath, mergeIDSet, outChan)
			}
			wg.Done()
		}()
	}

	// 收集者

	outSync := sync.WaitGroup{}
	outSync.Add(1)

	go func() {
		result := []AuthorStruct{}
		for obj := range outChan {
			result = append(result, obj)
		}
		// 将对象转换为 JSON
		// jsonData, err := json.Marshal(result)
		// if err != nil {
		// 	log.Panic().Err(err).Msg("json 转换失败")
		// }

		dumpJsonGzipCsv("/mnt/sas/home/ider/workspace/jupyter-lab/openalex_author_analysis/data/authors.json.gz", result)
		// // 创建一个新的文件
		// file, err := os.Create("/mnt/sas/home/ider/workspace/jupyter-lab/openalex_author_analysis/data/authors.json.gz")
		// if err != nil {

		// }
		// defer file.Close()

		// // 创建一个新的 gzip.Writer
		// gzipWriter := gzip.NewWriter(file)
		// defer gzipWriter.Close()

		// // 将 JSON 数据写入 gzip.Writer
		// _, err = gzipWriter.Write(jsonData)
		// if err != nil {
		// 	log.Panic().Err(err).Msg("json 写入失败")
		// }
		outSync.Done()
	}()

	wg.Wait()
	close(outChan)
	outSync.Wait()

	<-time.After(10 * time.Second)
	log.Info().Msg("全部处理完成")
}

func MainAuthors() {

	foldPath := "/mnt/sata3/openalex/openalex-snapshot-v20231225/data"
	Version := "20231101"
	cp := NewAuthorProject(foldPath)
	AuthorsFlow(cp, 10, Version)
}
