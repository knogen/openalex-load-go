package authors20231218

// 先分析 authors 文件，筛选出高创作者，记录需要的值，然后再分析 works 文件，记录详细的内容到每一年
// works: publication_year, concepts,  authorships->{author->id,countries,institutions,}
// authors: x_concepts, affiliations, counts_by_year, works_count, id
// 一个work有多个 concept, 但是我这里做了减法，只去score最高的lv0和lv1,有且仅有保留2个
// 国家的定义，这里就用作者国家，这里只用最后一次国家或者最后一次多个国家，放弃计算时间的影响
// 机构的定义， 将作者扩散到多个机构
// 学科的定义，将作者扩散到学科，这里可以直接用作者里的 x_concept，这个方法只用score最高的lv0和lv1, 或者自己按年度发表论文进行统计推理论文，这部分再思考
// 论文学科取 score >= 0.3 lv0,lv1,lv2 三层，论文保留了 0.3 < 的父节点，这里我们要过滤掉
// 一篇文章多个学科，所以学科间只能横向比较

// 统计4个内容
// annal_country_author_count, annal_country_article_count, annal_instrtution_author_count, annale_instrtution_paper_count
// annal_country_author_count_curves, annal_instrtution_author_count_curves
import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"openalex-load-go/internal/openalex/load"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/rs/zerolog/log"
)

var (
	ConceptScoreLine = 0.3
)

type WorkProject struct {
	*load.BaseProject
}

func NewWorkProject(dataPath string) *WorkProject {
	BaseProject := load.NewBaseProject("works", dataPath)
	return &WorkProject{BaseProject}
}

func (c *WorkProject) ParseData(obj map[string]interface{}) {

}

// get project data file
func handleWorksFile(c load.DataLoadInterface, filePath string, mergeIDSet *hashset.Set, authorSet *hashset.Set, authorInfoChan chan authorInfoMark, articleInfoChan chan articleInfoMark) {
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
		var obj WorkStruct
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			log.Panic().Err(err).Msg("file 读取失败")
			continue
		}

		obj.ID = shorten_url(obj.ID)
		// 忽略 merge id
		if mergeIDSet.Contains(obj.ID) {
			continue
		}

		// 找出 top1 的 concept
		lv0ConceptIDs := []string{}
		lv1ConceptIDs := []string{}
		lv2ConceptIDs := []string{}

		for _, conceptItem := range obj.Concepts {
			if conceptItem.Level == 0 && conceptItem.Score >= ConceptScoreLine {
				lv0ConceptIDs = append(lv0ConceptIDs, shorten_url(conceptItem.ID))
			}
			if conceptItem.Level == 1 && conceptItem.Score >= ConceptScoreLine {
				lv1ConceptIDs = append(lv1ConceptIDs, shorten_url(conceptItem.ID))
			}
			if conceptItem.Level == 2 && conceptItem.Score >= ConceptScoreLine {
				lv2ConceptIDs = append(lv2ConceptIDs, shorten_url(conceptItem.ID))
			}
		}

		articleInstitution := []string{}
		articleCountry := []string{}
		for _, authorItem := range obj.Authorships {
			authorItem.Author.ID = shorten_url(authorItem.Author.ID)
			// 处理全部作者
			// if authorSet.Contains(authorItem.Author.ID) {
			// 给作者加分, 添加年份信息
			InstitutionIDList := []string{}
			// InstitutionCountryList := []string{}
			for _, institutionItem := range authorItem.Institutions {
				InstitutionIDList = append(InstitutionIDList, shorten_url(institutionItem.ID))
				// InstitutionCountryList = append(InstitutionCountryList, shorten_url(institutionItem.CountryCode))
			}
			authorInfoChan <- authorInfoMark{
				AuthorID:    authorItem.Author.ID,
				Year:        obj.PublicationYear,
				Institution: InstitutionIDList,
				Country:     authorItem.Countries,
				Concept0:    lv0ConceptIDs,
				Concept1:    lv1ConceptIDs,
				Concept2:    lv2ConceptIDs,
			}
			articleInstitution = append(articleInstitution, InstitutionIDList...)
			articleCountry = append(articleCountry, authorItem.Countries...)
		}

		articleInfoChan <- articleInfoMark{
			ArticleID:   obj.ID,
			Year:        obj.PublicationYear,
			Institution: DeduplicateStrings(articleInstitution),
			Country:     DeduplicateStrings(articleCountry),
			Concept0:    lv0ConceptIDs,
			Concept1:    lv1ConceptIDs,
			Concept2:    lv2ConceptIDs,
		}
	}

	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("file 读取失败")
		return
	}

}

func ReadAuthorCollect() []AuthorStruct {
	file, err := os.Open("/mnt/sas/home/ider/workspace/jupyter-lab/openalex_author_analysis/data/authors.json.gz")
	if err != nil {
		log.Panic().Err(err).Msg("文件打开失败")
	}
	defer file.Close()

	// 创建 gzip.Reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		log.Panic().Err(err).Msg("文件读取失败")
	}
	defer gzipReader.Close()

	// 创建bufio.Scanner
	scanner := bufio.NewScanner(gzipReader)

	var authors []AuthorStruct
	// 迭代每一行
	for scanner.Scan() {
		// 获取一行数据
		line := scanner.Text()

		// 创建一个新的对象来存储解析后的数据
		var obj AuthorStruct

		// 解析JSON
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			log.Panic().Err(err).Msg("json unmarshal 失败")
		}
		authors = append(authors, obj)
		// 使用解析后的数据
	}

	// 检查是否有错误发生
	if err := scanner.Err(); err != nil {
		log.Panic().Err(err).Msg("文件解析失败")
	}

	return authors
}

// 传输用户
type authorInfoMark struct {
	AuthorID    string
	Year        int
	Institution []string
	Country     []string
	Concept0    []string
	Concept1    []string
	Concept2    []string
}

type articleInfoMark struct {
	ArticleID   string
	Year        int
	Institution []string
	Country     []string
	Concept0    []string
	Concept1    []string
	Concept2    []string
}

type authorResultClooect struct {
	Source      AuthorStruct
	WorkCount   map[int]int
	Institution map[int][]string
	Country     map[int][]string
	Concept0    map[int][]string
	Concept1    map[int][]string
	Concept2    map[int][]string
}

func idToInt64(ID string) (i64 int64) {
	str := ID[1:]
	// 将剩余的字符串转换为 int64 类型
	i64, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		// 错误处理
		log.Warn().Str("author ID", ID).Msg("author ID unregular")
		return
	}
	return
}

func addMapSet(key string, value int64, mapSet map[string]*hashset.Set) {

	if set, ok := mapSet[key]; ok {
		set.Add(value)
	} else {
		set = hashset.New()
		set.Add(value)
		mapSet[key] = set
	}
}

func mapSetToMapCount(mapSet map[string]*hashset.Set) map[string]int {
	resultMap := make(map[string]int)
	for key, set := range mapSet {
		resultMap[key] = set.Size()
	}
	return resultMap
}

// 统计出3个类型
// 作者
func WorksFlow(c load.DataLoadInterface, runtimeCount int, version string) {

	// 获取作者
	log.Info().Msgf("Start to get author")
	authors := ReadAuthorCollect()
	log.Info().Msgf("author count: %d", len(authors))

	// 作者生成set, 在遍历论文的时候使用
	authorSet := hashset.New()
	for _, authorItem := range authors {
		authorSet.Add(authorItem.ID)
	}
	// 作者生成 map
	authorMap := make(map[string]AuthorStruct)
	for _, authorItem := range authors {
		authorMap[authorItem.ID] = authorItem

	}

	fileChan := make(chan string, 10000)
	authorInfoChan := make(chan authorInfoMark, 10000)
	articleInfoChan := make(chan articleInfoMark, 10000)

	for _, filePath := range c.GetProjectGzFiles() {
		// test
		fileChan <- filePath

	}
	close(fileChan)

	mergeIDSet := c.GetMergeIDsSet()
	// test
	// mergeIDSet := hashset.New()

	log.Info().Msg("mergeIDSet处理完成")

	wg := sync.WaitGroup{}
	wg.Add(runtimeCount)
	for i := 0; i < runtimeCount; i++ {
		// handle file
		go func() {
			for filePath := range fileChan {
				handleWorksFile(c, filePath, mergeIDSet, authorSet, authorInfoChan, articleInfoChan)
			}
			wg.Done()
		}()
	}

	retWg := sync.WaitGroup{}
	retWg.Add(2)
	// 处理作者信息
	go func() {
		// 作者数量
		// country, year, AuthorCount
		countryYearAuthorSetMap := make(map[string]*hashset.Set)
		// country, year, Concept0, AuthorCount
		countryYearConcept0AuthorSetMap := make(map[string]*hashset.Set)
		// country, year, Concept1, AuthorCount
		countryYearConcept1AuthorSetMap := make(map[string]*hashset.Set)
		// country, year, Concept2, AuthorCount
		// countryYearConcept2AuthorSetMap := make(map[string]*hashset.Set)
		// country, AuthorCount
		countryAuthorSetMap := make(map[string]*hashset.Set)
		// institution, year, AuthorCount
		institutionYearAuthorSetMap := make(map[string]*hashset.Set)
		// institution, year,Concept0, AuthorCount
		institutionYearConcept0AuthorSetMap := make(map[string]*hashset.Set)
		// institution, year,Concept1, AuthorCount
		institutionYearConcept1AuthorSetMap := make(map[string]*hashset.Set)
		// institution, year,Concept2, AuthorCount
		// institutionYearConcept2AuthorSetMap := make(map[string]*hashset.Set)
		// institution, AuthorCount
		institutionAuthorSetMap := make(map[string]*hashset.Set)
		// year, AuthorCount
		yearAuthorSetMap := make(map[string]*hashset.Set)
		// 作者署名次数
		AuthorSet := hashset.New()

		// 署名次数
		// country, year, SignCount
		countryYearSignCountMap := make(map[string]int)
		// country, year, Concept0, SignCount
		countryYearConcept0SignCountMap := make(map[string]int)
		// country, year, Concept1, SignCount
		countryYearConcept1SignCountMap := make(map[string]int)
		// country, year, Concept2, SignCount
		// countryYearConcept2SignCountMap := make(map[string]int)
		// country, SignCount
		countrySignCountMap := make(map[string]int)
		// institution, year, SignCount
		institutionYearSignCountMap := make(map[string]int)
		// institution, year,Concept0, SignCount
		institutionYearConcept0SignCountMap := make(map[string]int)
		// institution, year,Concept1, SignCount
		institutionYearConcept1SignCountMap := make(map[string]int)
		// institution, year,Concept2, SignCount
		// institutionYearConcept2SignCountMap := make(map[string]int)
		// institution, SignCount
		institutionSignCountMap := make(map[string]int)
		// year, SignCount
		yearSignCountMap := make(map[string]int)
		// 作者署名次数
		SignCount := 0

		resultCollectMap := make(map[string]authorResultClooect)
		for authorInfo := range authorInfoChan {
			authorIDInt64 := idToInt64(authorInfo.AuthorID)

			for _, countryCode := range authorInfo.Country {
				key := fmt.Sprintf("%s_%d", countryCode, authorInfo.Year)
				countryYearSignCountMap[key] += 1
				addMapSet(key, authorIDInt64, countryYearAuthorSetMap)

				for _, conceptCode := range authorInfo.Concept0 {
					key := fmt.Sprintf("%s_%s_%d", countryCode, conceptCode, authorInfo.Year)
					countryYearConcept0SignCountMap[key] += 1
					addMapSet(key, authorIDInt64, countryYearConcept0AuthorSetMap)
				}
				for _, conceptCode := range authorInfo.Concept1 {
					key := fmt.Sprintf("%s_%s_%d", countryCode, conceptCode, authorInfo.Year)
					countryYearConcept1SignCountMap[key] += 1
					addMapSet(key, authorIDInt64, countryYearConcept1AuthorSetMap)
				}
				// for _, conceptCode := range authorInfo.Concept2 {
				// 	key := fmt.Sprintf("%s_%s_%d", countryCode, conceptCode, authorInfo.Year)
				// 	countryYearConcept2SignCountMap[key] += 1
				// 	addMapSet(key, authorIDInt64, countryYearConcept2AuthorSetMap)
				// }
				countrySignCountMap[countryCode] += 1
				addMapSet(countryCode, authorIDInt64, countryAuthorSetMap)
			}

			for _, institutionCode := range authorInfo.Institution {
				key := fmt.Sprintf("%s_%d", institutionCode, authorInfo.Year)
				institutionYearSignCountMap[key] += 1
				addMapSet(key, authorIDInt64, institutionYearAuthorSetMap)

				for _, conceptCode := range authorInfo.Concept0 {
					key := fmt.Sprintf("%s_%s_%d", institutionCode, conceptCode, authorInfo.Year)
					institutionYearConcept0SignCountMap[key] += 1
					addMapSet(key, authorIDInt64, institutionYearConcept0AuthorSetMap)
				}
				for _, conceptCode := range authorInfo.Concept1 {
					key := fmt.Sprintf("%s_%s_%d", institutionCode, conceptCode, authorInfo.Year)
					institutionYearConcept1SignCountMap[key] += 1
					addMapSet(key, authorIDInt64, institutionYearConcept1AuthorSetMap)
				}
				// for _, conceptCode := range authorInfo.Concept2 {
				// 	key := fmt.Sprintf("%s_%s_%d", institutionCode, conceptCode, authorInfo.Year)
				// 	institutionYearConcept2SignCountMap[key] += 1
				// 	addMapSet(key, authorIDInt64, institutionYearConcept2AuthorSetMap)
				// }
				institutionSignCountMap[institutionCode] += 1
				addMapSet(institutionCode, authorIDInt64, institutionAuthorSetMap)
			}

			key := fmt.Sprintf("%d", authorInfo.Year)
			yearSignCountMap[key] += 1
			addMapSet(key, authorIDInt64, yearAuthorSetMap)

			SignCount += 1
			AuthorSet.Add(authorIDInt64)

			// 排除非高产作者
			if !authorSet.Contains(authorInfo.AuthorID) {
				continue
			}
			// 高产作者文章数的统计
			if _, ok := resultCollectMap[authorInfo.AuthorID]; !ok {
				resultCollectMap[authorInfo.AuthorID] = authorResultClooect{
					Source:      authorMap[authorInfo.AuthorID],
					WorkCount:   make(map[int]int),
					Institution: make(map[int][]string),
					Country:     make(map[int][]string),
					Concept0:    make(map[int][]string),
					Concept1:    make(map[int][]string),
					Concept2:    make(map[int][]string),
				}
			}
			resultCollectMap[authorInfo.AuthorID].WorkCount[authorInfo.Year] += 1

			for _, conceptCode := range authorInfo.Concept0 {
				resultCollectMap[authorInfo.AuthorID].Concept0[authorInfo.Year] =
					append(resultCollectMap[authorInfo.AuthorID].Concept0[authorInfo.Year], conceptCode)
			}
			for _, conceptCode := range authorInfo.Concept1 {
				resultCollectMap[authorInfo.AuthorID].Concept1[authorInfo.Year] =
					append(resultCollectMap[authorInfo.AuthorID].Concept1[authorInfo.Year], conceptCode)
			}
			for _, conceptCode := range authorInfo.Concept2 {
				resultCollectMap[authorInfo.AuthorID].Concept2[authorInfo.Year] =
					append(resultCollectMap[authorInfo.AuthorID].Concept2[authorInfo.Year], conceptCode)
			}

			if len(authorInfo.Institution) > 0 {
				resultCollectMap[authorInfo.AuthorID].Institution[authorInfo.Year] =
					append(resultCollectMap[authorInfo.AuthorID].Institution[authorInfo.Year], authorInfo.Institution...)
			}
			if len(authorInfo.Country) > 0 {
				resultCollectMap[authorInfo.AuthorID].Country[authorInfo.Year] =
					append(resultCollectMap[authorInfo.AuthorID].Country[authorInfo.Year], authorInfo.Country...)
			}

		}
		parentPath := "/mnt/sas/home/ider/workspace/jupyter-lab/openalex_author_analysis/data/"
		// 署名次数
		{
			dumpJsonGzipData(parentPath+"countryYearSignCountMap.json.gz", countryYearSignCountMap)
			dumpJsonGzipData(parentPath+"countryYearConcept0SignCountMap.json.gz", countryYearConcept0SignCountMap)
			dumpJsonGzipData(parentPath+"countryYearConcept1SignCountMap.json.gz", countryYearConcept1SignCountMap)
			// dumpJsonGzipData(parentPath+"countryYearConcept2SignCountMap.json.gz", countryYearConcept2SignCountMap)
			dumpJsonGzipData(parentPath+"countrySignCountMap.json.gz", countrySignCountMap)
			dumpJsonGzipData(parentPath+"institutionYearSignCountMap.json.gz", institutionYearSignCountMap)
			dumpJsonGzipData(parentPath+"institutionYearConcept0SignCountMap.json.gz", institutionYearConcept0SignCountMap)
			dumpJsonGzipData(parentPath+"institutionYearConcept1SignCountMap.json.gz", institutionYearConcept1SignCountMap)
			// dumpJsonGzipData(parentPath+"institutionYearConcept2SignCountMap.json.gz", institutionYearConcept2SignCountMap)
			dumpJsonGzipData(parentPath+"institutionSignCountMap.json.gz", institutionSignCountMap)
			dumpJsonGzipData(parentPath+"yearSignCountMap.json.gz", yearSignCountMap)
			dumpJsonGzipData(parentPath+"SignCount.json.gz", SignCount)
		}

		// 作者次数
		{
			dumpJsonGzipData(parentPath+"countryYearAuthorCountMap.json.gz", mapSetToMapCount(countryYearAuthorSetMap))
			dumpJsonGzipData(parentPath+"countryYearConcept0AuthorCountMap.json.gz", mapSetToMapCount(countryYearConcept0AuthorSetMap))
			dumpJsonGzipData(parentPath+"countryYearConcept1AuthorCountMap.json.gz", mapSetToMapCount(countryYearConcept1AuthorSetMap))
			// dumpJsonGzipData(parentPath+"countryYearConcept2AuthorCountMap.json.gz", mapSetToMapCount(countryYearConcept2AuthorSetMap))
			dumpJsonGzipData(parentPath+"countryAuthorCountMap.json.gz", mapSetToMapCount(countryAuthorSetMap))
			dumpJsonGzipData(parentPath+"institutionYearAuthorCountMap.json.gz", mapSetToMapCount(institutionYearAuthorSetMap))
			dumpJsonGzipData(parentPath+"institutionYearConcept0AuthorCountMap.json.gz", mapSetToMapCount(institutionYearConcept0AuthorSetMap))
			dumpJsonGzipData(parentPath+"institutionYearConcept1AuthorCountMap.json.gz", mapSetToMapCount(institutionYearConcept1AuthorSetMap))
			// dumpJsonGzipData(parentPath+"institutionYearConcept2AuthorCountMap.json.gz", mapSetToMapCount(institutionYearConcept2AuthorSetMap))
			dumpJsonGzipData(parentPath+"institutionAuthorCountMap.json.gz", mapSetToMapCount(institutionAuthorSetMap))
			dumpJsonGzipData(parentPath+"yearAuthorCountMap.json.gz", mapSetToMapCount(yearAuthorSetMap))
			dumpJsonGzipData(parentPath+"AuthorCount.json.gz", AuthorSet.Size())
		}

		log.Info().Int("author count", len(resultCollectMap)).Msg("author info collect done")

		// 最后过滤一遍，不符合要求的作者过滤

		upper_paper_count := 24
		resultCollecList := []authorResultClooect{}
		for _, authorResult := range resultCollectMap {
			if authorResult.Source.WorksCount >= 100 {
				resultCollecList = append(resultCollecList, authorResult)
			} else {
				// 过滤历史年份
				for _, workCount := range authorResult.WorkCount {
					if workCount >= upper_paper_count {
						resultCollecList = append(resultCollecList, authorResult)
						break
					}
				}
			}
		}
		log.Info().Int("author filter count", len(resultCollecList)).Msg("author info filter done")

		dumpJsonGzipCsv(parentPath+"authors_collect.json.gz", resultCollecList)

		log.Info().Msg("author handle successful")
		retWg.Done()
	}()

	// 处理 article 信息
	go func() {

		// country, year, articleCount
		countryYearArticleCountMap := make(map[string]int)

		// country, year,Concept0, articleCount
		countryYearConcept0ArticleCountMap := make(map[string]int)

		// country, year,Concept1, articleCount
		countryYearConcept1ArticleCountMap := make(map[string]int)

		// country, year,Concept2, articleCount
		// countryYearConcept2ArticleCountMap := make(map[string]int)

		// country, articleCount
		countryArticleCountMap := make(map[string]int)

		// institution, year, articleCount
		institutionYearArticleCountMap := make(map[string]int)

		// institution, year,Concept0, articleCount
		institutionYearConcept0ArticleCountMap := make(map[string]int)

		// institution, year,Concept1, articleCount
		institutionYearConcept1ArticleCountMap := make(map[string]int)

		// institution, year,Concept2, articleCount
		// institutionYearConcept2ArticleCountMap := make(map[string]int)

		// institution, articleCount
		institutionArticleCountMap := make(map[string]int)

		// year, articleCount
		yearArticleCountMap := make(map[string]int)

		ArticleCount := 0

		//
		for articleInfo := range articleInfoChan {

			for _, countryCode := range articleInfo.Country {
				key := fmt.Sprintf("%s_%d", countryCode, articleInfo.Year)
				countryYearArticleCountMap[key] += 1
				for _, conceptCode := range articleInfo.Concept0 {
					key := fmt.Sprintf("%s_%s_%d", countryCode, conceptCode, articleInfo.Year)
					countryYearConcept0ArticleCountMap[key] += 1
				}
				for _, conceptCode := range articleInfo.Concept1 {
					key := fmt.Sprintf("%s_%s_%d", countryCode, conceptCode, articleInfo.Year)
					countryYearConcept1ArticleCountMap[key] += 1
				}
				// for _, conceptCode := range articleInfo.Concept2 {
				// 	key := fmt.Sprintf("%s_%s_%d", countryCode, conceptCode, articleInfo.Year)
				// 	countryYearConcept2ArticleCountMap[key] += 1
				// }
				countryArticleCountMap[countryCode] += 1
			}

			for _, institutionCode := range articleInfo.Institution {
				key := fmt.Sprintf("%s_%d", institutionCode, articleInfo.Year)
				institutionYearArticleCountMap[key] += 1
				for _, conceptCode := range articleInfo.Concept0 {
					key := fmt.Sprintf("%s_%s_%d", institutionCode, conceptCode, articleInfo.Year)
					institutionYearConcept0ArticleCountMap[key] += 1
				}
				for _, conceptCode := range articleInfo.Concept1 {
					key := fmt.Sprintf("%s_%s_%d", institutionCode, conceptCode, articleInfo.Year)
					institutionYearConcept1ArticleCountMap[key] += 1
				}
				// for _, conceptCode := range articleInfo.Concept2 {
				// 	key := fmt.Sprintf("%s_%s_%d", institutionCode, conceptCode, articleInfo.Year)
				// 	institutionYearConcept2ArticleCountMap[key] += 1
				// }
				institutionArticleCountMap[institutionCode] += 1
			}

			key := fmt.Sprintf("%d", articleInfo.Year)
			yearArticleCountMap[key] += 1
			ArticleCount += 1
		}

		log.Info().Int("article count", ArticleCount).Msg("article info collect done")

		{
			parentPath := "/mnt/sas/home/ider/workspace/jupyter-lab/openalex_author_analysis/data/"
			dumpJsonGzipData(parentPath+"countryYearArticleCountMap.json.gz", countryYearArticleCountMap)
			dumpJsonGzipData(parentPath+"countryYearConcept0ArticleCountMap.json.gz", countryYearConcept0ArticleCountMap)
			dumpJsonGzipData(parentPath+"countryYearConcept1ArticleCountMap.json.gz", countryYearConcept1ArticleCountMap)
			// dumpJsonGzipData(parentPath+"countryYearConcept2ArticleCountMap.json.gz", countryYearConcept2ArticleCountMap)
			dumpJsonGzipData(parentPath+"countryArticleCountMap.json.gz", countryArticleCountMap)
			dumpJsonGzipData(parentPath+"institutionYearArticleCountMap.json.gz", institutionYearArticleCountMap)
			dumpJsonGzipData(parentPath+"institutionYearConcept0ArticleCountMap.json.gz", institutionYearConcept0ArticleCountMap)
			dumpJsonGzipData(parentPath+"institutionYearConcept1ArticleCountMap.json.gz", institutionYearConcept1ArticleCountMap)
			// dumpJsonGzipData(parentPath+"institutionYearConcept2ArticleCountMap.json.gz", institutionYearConcept2ArticleCountMap)
			dumpJsonGzipData(parentPath+"institutionArticleCountMap.json.gz", institutionArticleCountMap)
			dumpJsonGzipData(parentPath+"yearArticleCountMap.json.gz", yearArticleCountMap)
			dumpJsonGzipData(parentPath+"ArticleCount.json.gz", ArticleCount)
		}
		log.Info().Msg("article handle successful")
		retWg.Done()
	}()

	wg.Wait()

	close(authorInfoChan)
	close(articleInfoChan)
	retWg.Wait()

	<-time.After(30 * time.Second)
	log.Info().Msg("全部处理完成")
}

func MainWorks() {

	foldPath := "/mnt/sata3/openalex/openalex-snapshot-v20231225/data"
	Version := "20231101"
	cp := NewWorkProject(foldPath)
	WorksFlow(cp, 10, Version)
}
