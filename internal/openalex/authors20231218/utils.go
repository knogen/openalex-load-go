package authors20231218

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

func shorten_url(url string) string {
	parts := strings.Split(url, "/")
	lastPart := parts[len(parts)-1]
	return lastPart
}

// DeduplicateStrings 去除字符串切片中的重复项
func DeduplicateStrings(strings []string) []string {
	seen := make(map[string]struct{}) // 使用空结构体作为值类型，因为它不占用空间
	var result []string

	for _, s := range strings {
		if _, exists := seen[s]; !exists {
			result = append(result, s)
			seen[s] = struct{}{}
		}
	}

	return result
}

func dumpJsonGzipCsv[T interface{}](file_name string, data []T) {
	// 创建一个新的文件
	file, err := os.Create(file_name)
	if err != nil {
		log.Panic().Err(err).Msg("文件创建失败")
	}
	defer file.Close()

	// 创建一个新的 gzip.Writer
	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	// 创建bufio.Writer
	bw := bufio.NewWriter(gzipWriter)
	defer bw.Flush()

	// 迭代对象
	for _, obj := range data {
		// 将对象序列化为JSON
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			log.Panic().Err(err).Msg("json 转换失败")
		}

		// 写入序列化后的JSON到文件
		if _, err := bw.Write(jsonBytes); err != nil {
			log.Panic().Err(err).Msg("buffer 写入失败")
		}

		// 写入换行符，以便每个对象占据一行
		if _, err := bw.WriteString("\n"); err != nil {
			log.Panic().Err(err).Msg("buffer 写入失败")
		}
	}

}

func dumpJsonGzipData(file_name string, data interface{}) {
	// 将对象转换为 JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Panic().Err(err).Msg("json 转换失败")
	}

	// 创建一个新的文件
	file, err := os.Create(file_name)
	if err != nil {
		log.Panic().Err(err).Msg("文件创建失败")
	}
	defer file.Close()

	// 创建一个新的 gzip.Writer
	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	// 将 JSON 数据写入 gzip.Writer
	_, err = gzipWriter.Write(jsonData)
	if err != nil {
		log.Panic().Err(err).Msg("json 写入失败")
	}

}

type AuthorStruct struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	// DisplayNameAlternatives []string `json:"display_name_alternatives"`
	WorksCount   int `json:"works_count"`
	Affiliations []struct {
		Institution struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
			CountryCode string `json:"country_code"`
		} `json:"institution"`
		Years []int `json:"years"`
	} `json:"affiliations"`
	XConcepts []struct {
		ID          string  `json:"id"`
		DisplayName string  `json:"display_name"`
		Level       int     `json:"level"`
		Score       float64 `json:"score"`
	} `json:"x_concepts"`
	CountsByYear []struct {
		Year         int `json:"year"`
		WorksCount   int `json:"works_count"`
		CitedByCount int `json:"cited_by_count"`
	} `json:"counts_by_year"`
}

type WorkStruct struct {
	ID              string       `json:"id"`
	PublicationYear int          `json:"publication_year"`
	PublicationDate string       `json:"publication_date"`
	Authorships     []Authorship `json:"authorships"`
	Concepts        []Concept    `json:"concepts"`
}

type Authorship struct {
	Author       Author        `json:"author"`
	Institutions []Institution `json:"institutions"`
	Countries    []string      `json:"countries"`
}

type Author struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

type Institution struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	CountryCode string `json:"country_code"`
	Type        string `json:"type"`
}

type Concept struct {
	ID          string  `json:"id"`
	DisplayName string  `json:"display_name"`
	Level       int     `json:"level"`
	Score       float64 `json:"score"`
}

type AnnalCountryAuthorCount struct {
	Country      string `json:"country"`
	AuthorCounts []int  `json:"authorCounts"`
	Years        []int  `json:"years"`
}

type AnnalCountryArticleCount struct {
	Country       string `json:"country"`
	ArticleCounts []int  `json:"articleCounts"`
	Years         []int  `json:"years"`
}

type AnnalInstrtutionAuthorCount struct {
	InstrtutionID   string `json:"instrtutionID"`
	InstrtutionName string `json:"instrtutionName"`
	AuthorCounts    []int  `json:"authorCounts"`
	Years           []int  `json:"years"`
}

type AnnalInstrtutionArticleCount struct {
	InstrtutionID   string `json:"instrtutionID"`
	InstrtutionName string `json:"instrtutionName"`
	Years           []int  `json:"years"`
}

type AnnalCountryArticleCountCurve struct {
	Country       string `json:"country"`
	Year          int    `json:"year"`
	AuthorCounts  []int  `json:"authorCounts"`
	ArticleCounts []int  `json:"articleCounts"`
}

type AnnalInstrtutionArticleCountCurve struct {
	InstrtutionID   string `json:"instrtutionID"`
	InstrtutionName string `json:"instrtutionName"`
	Year            int    `json:"year"`
	AuthorCounts    []int  `json:"authorCounts"`
	ArticleCounts   []int  `json:"articleCounts"`
}
