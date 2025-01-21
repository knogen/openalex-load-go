package load

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/panjf2000/ants/v2"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

func RuntimeToMongoFlow(c DataLoadInterface, runtimeCount int) {
	// log.Info().Str("jsonFilePath", jsonFilePath).Msg("out file path")
	// log.Info().Int("runtionCount", runtimeCount).Msg("start")

	pool, _ := ants.NewPool(runtimeCount)
	defer pool.Release()

	log.Info().Msg(projectConf.MongoUrl)
	client := newMongoDataBase(projectConf.MongoUrl, projectConf.Version)
	// client.initIndex()

	if c.GetProjectName() != "works" && c.GetProjectName() != "concepts" {
		log.Info().Str("project", c.GetProjectName()).Msg("unsupport the project")
		os.Exit(0)
	}

	if c.GetProjectName() == "works" {
		flieList := c.GetProjectGzFiles()
		fileChan := make(chan string, len(flieList))
		for _, filePath := range flieList {
			fileChan <- filePath
		}
		close(fileChan)

		mergeIDSet := c.GetMergeIDsSet()

		workChan := make(chan *worksMongo, 100000)
		wg := sync.WaitGroup{}
		for i := 0; i < runtimeCount; i++ {
			wg.Add(1)
			// progress file
			pool.Submit(func() {
				for filePath := range fileChan {
					handleWorksToMongo(filePath, mergeIDSet, workChan)
				}
				wg.Done()
			})
		}

		go func() {
			wg.Wait()
			close(workChan)
		}()

		bar := progressbar.Default(-1, "load to cache")
		workCache := []*worksMongo{}
		linksInIDMap := make(map[int64]int32)
		for obj := range workChan {
			workCache = append(workCache, obj)
			for _, linksOut := range obj.ReferencedWorks {
				linksInIDMap[linksOut] += 1
			}
			bar.Add(1)
		}
		bar.Close()

		bar = progressbar.Default(int64(len(workCache)), "save to mongo")
		workInsertCache := []*worksMongo{}
		for _, obj := range workCache {
			obj.LinksInWorksCount = linksInIDMap[obj.ID]
			workInsertCache = append(workInsertCache, obj)
			if len(workInsertCache) > 40000 {
				client.Insert_many(workInsertCache)
				workInsertCache = []*worksMongo{}
			}
			bar.Add(1)
		}
		if len(workInsertCache) > 0 {
			client.Insert_many(workInsertCache)
		}
		bar.Close()

		wg.Wait()
		log.Info().Str("project", c.GetProjectName()).Msg("project finish")
	} else if c.GetProjectName() == "concepts" {
		flieList := c.GetProjectGzFiles()
		fileChan := make(chan string, len(flieList))
		for _, filePath := range flieList {
			fileChan <- filePath
		}
		close(fileChan)

		mergeIDSet := c.GetMergeIDsSet()

		conceptsChan := make(chan *conceptsMongo, 100000)
		wg := sync.WaitGroup{}
		for i := 0; i < runtimeCount; i++ {
			wg.Add(1)
			// progress file
			pool.Submit(func() {
				for filePath := range fileChan {
					handleConceptsToMongo(filePath, mergeIDSet, conceptsChan)
				}
				wg.Done()
			})
		}

		go func() {
			wg.Wait()
			close(conceptsChan)
		}()

		bar := progressbar.Default(-1, "load to cache")
		conceptsCache := []*conceptsMongo{}
		for obj := range conceptsChan {
			conceptsCache = append(conceptsCache, obj)
			bar.Add(1)
		}
		bar.Close()

		bar = progressbar.Default(int64(len(conceptsCache)), "save to mongo")
		conceptsInsertCache := []*conceptsMongo{}
		for _, obj := range conceptsCache {
			conceptsInsertCache = append(conceptsInsertCache, obj)
			if len(conceptsInsertCache) > 40000 {
				client.Insert_many_concepts(conceptsInsertCache)
				conceptsInsertCache = []*conceptsMongo{}
			}
			bar.Add(1)
		}
		if len(conceptsInsertCache) > 0 {
			client.Insert_many_concepts(conceptsInsertCache)
		}
		bar.Close()

		wg.Wait()
		log.Info().Str("project", c.GetProjectName()).Msg("project finish")
	}
}

// get project data file
func handleWorksToMongo(filePath string, mergeIDSet *hashset.Set, workChan chan *worksMongo) {
	// log.Debug().Str("file", filePath).Msg("start file decode")
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
		var obj worksJson
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			log.Panic().Err(err).Msg("file 读取失败")
			continue
		}

		ID := extractUrlID(obj.ID)

		// 忽略 merge id
		if mergeIDSet.Contains(fmt.Sprintf("W%d", ID)) {
			continue
		}

		var referencedWorks []int64

		for _, ids := range obj.ReferencedWorks {
			referencedWorks = append(referencedWorks, extractUrlID(ids))
		}

		conceptLv0 := []string{}
		conceptLv1 := []string{}
		conceptLv2 := []string{}
		for _, item := range obj.Concepts {
			if item.Level == 0 && item.Score > 0 {
				conceptLv0 = append(conceptLv0, item.DisplayName)
			}
			if item.Level == 1 && item.Score > 0 {
				conceptLv1 = append(conceptLv1, item.DisplayName)
			}
			if item.Level == 2 && item.Score > 0 {
				conceptLv2 = append(conceptLv2, item.DisplayName)
			}
		}

		workChan <- &worksMongo{
			ID:                   ID,
			PublicationYear:      int32(obj.PublicationYear),
			ReferencedWorksCount: obj.ReferencedWorksCount,
			ReferencedWorks:      referencedWorks,
			ConceptsLv0:          conceptLv0,
			ConceptsLv1:          conceptLv1,
			ConceptsLv2:          conceptLv2,
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

// get project data file
func handleConceptsToMongo(filePath string, mergeIDSet *hashset.Set, conceptsChan chan *conceptsMongo) {
	// log.Debug().Str("file", filePath).Msg("start file decode")
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
		var obj conceptsMongo
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			log.Panic().Err(err).Msg("file 读取失败")
			continue
		}

		obj.ID = extractUrlIDToString(obj.ID)

		for i := range obj.Ancestors {
			obj.Ancestors[i].ID = extractUrlIDToString(obj.Ancestors[i].ID)
		}

		// 忽略 merge id
		if mergeIDSet.Contains(fmt.Sprintf("W%s", obj.ID)) {
			continue
		}

		conceptsChan <- &obj

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

func extractUrlID(url string) int64 {
	re := regexp.MustCompile(`/W(\d+)$`)
	matches := re.FindStringSubmatch(url)
	if len(matches) > 1 {
		// matches[1] 包含匹配的数字部分
		num, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			log.Warn().Str("url", url).Err(err).Msg("转换错误:")
		}
		return num
	} else {
		log.Warn().Str("url", url).Msg("转换错误:")
	}
	return 0
}

func extractUrlIDToString(url string) string {
	re := regexp.MustCompile(`(\d+)$`)
	matches := re.FindStringSubmatch(url)
	if len(matches) > 1 {
		return matches[1]
	} else {
		log.Warn().Str("url", url).Msg("转换错误:")
	}
	return "none"
}

type mongoDataBase struct {
	client   *mongo.Client
	database *mongo.Database
}

func newMongoDataBase(mongoUrl string, version string) *mongoDataBase {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUrl))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to mongo")
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to ping mongo")
	}

	return &mongoDataBase{
		client:   client,
		database: client.Database("openalex_v" + version),
	}
}

// func (c *mongoDataBase) initIndex() {
// 	mods := []mongo.IndexModel{
// 		{Keys: bson.M{"publication_year": 1}},
// 	}
// 	_, err := c.database.Collection("works").Indexes().CreateMany(ctx, mods)
// 	if err != nil {
// 		log.Warn().Err(err).Msg("failed to create index")
// 	}

// }

func (c *mongoDataBase) close() {
	err := c.client.Disconnect(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to disconnect from mongo")
	}
}

func (c *mongoDataBase) Insert_many(items []*worksMongo) {

	opts := options.InsertMany().SetOrdered(false)

	interfaceList := make([]interface{}, len(items))
	for i, page := range items {
		interfaceList[i] = page
	}

	_, err := c.database.Collection("works").InsertMany(ctx, interfaceList, opts)

	if err != nil {
		log.Warn().Err(err).Msg("failed to insert many")
	}

}

func (c *mongoDataBase) Insert_many_concepts(items []*conceptsMongo) {

	opts := options.InsertMany().SetOrdered(false)

	interfaceList := make([]interface{}, len(items))
	for i, page := range items {
		interfaceList[i] = page
	}

	_, err := c.database.Collection("concepts").InsertMany(ctx, interfaceList, opts)

	if err != nil {
		log.Warn().Err(err).Msg("failed to insert many")
	}

}
