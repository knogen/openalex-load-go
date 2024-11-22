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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

func RuntimeToMongoFlow(c DataLoadInterface, runtimeCount int) {
	// log.Info().Str("jsonFilePath", jsonFilePath).Msg("out file path")
	// log.Info().Int("runtionCount", runtimeCount).Msg("start")

	log.Info().Msg(projectConf.MongoUrl)
	client := newMongoDataBase(projectConf.MongoUrl, projectConf.Version)
	client.initIndex()

	if c.GetProjectName() != "works" {
		log.Info().Str("project", c.GetProjectName()).Msg("unsupport the project")
		os.Exit(0)
	}

	flieList := c.GetProjectGzFiles()
	fileChan := make(chan string, len(flieList))
	for _, filePath := range flieList {
		fileChan <- filePath
	}
	close(fileChan)

	mergeIDSet := c.GetMergeIDsSet()

	workChan := make(chan worksMongo, 100000)
	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for i := 0; i < runtimeCount; i++ {
		// handle file
		go func() {
			for filePath := range fileChan {
				handleFileToMongo(c, filePath, mergeIDSet, workChan)
			}
			wg.Done()
		}()
	}

	bar := progressbar.Default(-1)

	workCache := []worksMongo{}
	for obj := range workChan {
		workCache = append(workCache, obj)
		if len(workCache) > 40000 {
			client.Insert_many(workCache)
			workCache = []worksMongo{}
		}
		bar.Add(1)
	}

	if len(workCache) > 0 {
		client.Insert_many(workCache)
	}

	wg.Wait()
	log.Info().Str("project", c.GetProjectName()).Msg("project finish")

}

// get project data file
func handleFileToMongo(c DataLoadInterface, filePath string, mergeIDSet *hashset.Set, workChan chan worksMongo) {
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

		var referencedWorks []int

		for _, ids := range obj.ReferencedWorks {
			referencedWorks = append(referencedWorks, extractUrlID(ids))
		}

		workChan <- worksMongo{
			ID:                   ID,
			PublicationYear:      obj.PublicationYear,
			ReferencedWorksCount: obj.ReferencedWorksCount,
			ReferencedWorks:      referencedWorks,
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

func extractUrlID(url string) int {
	re := regexp.MustCompile(`/W(\d+)$`)
	matches := re.FindStringSubmatch(url)
	if len(matches) > 1 {
		// matches[1] 包含匹配的数字部分
		num, err := strconv.Atoi(matches[1])
		if err != nil {
			log.Warn().Str("url", url).Err(err).Msg("转换错误:")
		}
		return num
	} else {
		log.Warn().Str("url", url).Msg("转换错误:")
	}
	return 0
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

func (c *mongoDataBase) initIndex() {
	mods := []mongo.IndexModel{
		{Keys: bson.M{"publication_year": 1}},
	}
	_, err := c.database.Collection("works").Indexes().CreateMany(ctx, mods)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create index")
	}

}

func (c *mongoDataBase) close() {
	err := c.client.Disconnect(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to disconnect from mongo")
	}
}

func (c *mongoDataBase) Insert_many(items []worksMongo) {

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
