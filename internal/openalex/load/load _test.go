package load

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog/log"
)

func getTestData(fileName string) []map[string]interface{} {
	rootDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	path := filepath.Join(rootDir, "../../../testdata")
	path = filepath.Join(path, fileName+".txt")

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 创建一个 scanner 以逐行读取文件
	scanner := bufio.NewScanner(file)

	// 创建一个数组以保存前10行
	var lines []map[string]interface{}

	// 逐行读取文件
	// for i := 0; scanner.Scan() && i < 10; i++ {
	// 	lines = append(lines, scanner.Bytes())
	// }
	for scanner.Scan() {
		var obj map[string]interface{}
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			log.Panic().Err(err).Msg("file 读取失败")
			continue
		}
		lines = append(lines, obj)
	}
	return lines
}

func TestConceptProject(t *testing.T) {
	data := getTestData("concepts")
	cp := NewConceptProject("")
	for _, obj := range data {

		cp.ParseData(obj)

		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}

		// fmt.Println(string(prettyJSON))
		t.Errorf("Test passed: %s", prettyJSON)
	}
}

func TestInstitutionProject(t *testing.T) {
	data := getTestData("institutions")
	cp := NewInstitutionProject("")
	for _, obj := range data {

		cp.ParseData(obj)
		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}

		// fmt.Println(string(prettyJSON))
		t.Errorf("Test passed: %s", prettyJSON)
	}
}

func TestPublisherProject(t *testing.T) {
	data := getTestData("publishers")
	cp := NewPublisherProject("")
	for _, obj := range data {
		cp.ParseData(obj)
		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}
		t.Errorf("Test passed: %s", prettyJSON)
	}
}

func TestFunderProject(t *testing.T) {
	data := getTestData("funders")
	cp := NewFunderProject("")
	for _, obj := range data {
		cp.ParseData(obj)
		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}
		t.Errorf("Test passed: %s", prettyJSON)
	}
}

func TestSourceProject(t *testing.T) {
	data := getTestData("sources")
	cp := NewSourceProject("")
	for _, obj := range data {
		cp.ParseData(obj)
		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}
		t.Errorf("Test passed: %s", prettyJSON)
	}
}

func TestAuthorProject(t *testing.T) {
	data := getTestData("authors")
	cp := NewAuthorProject("")
	for _, obj := range data {
		cp.ParseData(obj)
		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}
		t.Errorf("Test passed: %s", prettyJSON)
	}
}

func TestWorkProject(t *testing.T) {
	data := getTestData("works")
	cp := NewWorkProject("")
	for _, obj := range data {
		cp.ParseData(obj)
		prettyJSON, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			panic(err)
		}
		t.Errorf("Test passed: %s", prettyJSON)
	}
}
