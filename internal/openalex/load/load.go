package load

import (
	"path"
	"strings"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/rs/zerolog/log"
)

func init() {
	// log.Info().Msg("start")
}

type DataLoadInterface interface {
	GetProjectName() string
	GetMergeIDsSet() *hashset.Set
	ParseData(obj map[string]interface{})
	GetProjectGzFiles() []string
}

type BaseProject struct {
	DataPath    string
	ProjectName string
}

// get project data file
func (c *BaseProject) GetProjectGzFiles() []string {
	rootPath := path.Join(c.DataPath, c.ProjectName)
	files := getSubPathGzFiles(rootPath)
	return files
}

func (c *BaseProject) GetProjectName() string {
	return c.ProjectName
}

func (c *BaseProject) GetMergeIDsSet() *hashset.Set {
	return getMergeIDs(c.ProjectName, c.DataPath)
}

// func (c *BaseProject) parseData(obj map[string]interface{}) {
// 	log.Panic().Msg("please rewrite parseData method")
// }

// get a base project
func NewBaseProject(projectName, dataPath string) *BaseProject {
	log.Info().Str("project", projectName).Msg("start")
	return &BaseProject{
		DataPath:    dataPath,
		ProjectName: projectName,
	}
}

type ConceptProject struct {
	*BaseProject
}
type InstitutionProject struct {
	*BaseProject
}
type PublisherProject struct {
	*BaseProject
}
type FunderProject struct {
	*BaseProject
}
type SourceProject struct {
	*BaseProject
}
type AuthorProject struct {
	*BaseProject
}
type WorkProject struct {
	*BaseProject
}

func NewConceptProject(dataPath string) *ConceptProject {
	BaseProject := NewBaseProject("concepts", dataPath)
	return &ConceptProject{BaseProject}
}

func NewInstitutionProject(dataPath string) *InstitutionProject {
	BaseProject := NewBaseProject("institutions", dataPath)
	return &InstitutionProject{BaseProject}
}

func NewPublisherProject(dataPath string) *PublisherProject {
	BaseProject := NewBaseProject("publishers", dataPath)
	return &PublisherProject{BaseProject}
}

func NewFunderProject(dataPath string) *FunderProject {
	BaseProject := NewBaseProject("funders", dataPath)
	return &FunderProject{BaseProject}
}

func NewSourceProject(dataPath string) *SourceProject {
	BaseProject := NewBaseProject("sources", dataPath)
	return &SourceProject{BaseProject}
}

func NewAuthorProject(dataPath string) *AuthorProject {
	BaseProject := NewBaseProject("authors", dataPath)
	return &AuthorProject{BaseProject}
}

func NewWorkProject(dataPath string) *WorkProject {
	BaseProject := NewBaseProject("works", dataPath)
	return &WorkProject{BaseProject}
}

func (c *ConceptProject) ParseData(obj map[string]interface{}) {

	shorten_url(obj, []string{"id", "wikidata"})
	shorten_url(obj["ids"].(map[string]interface{}), []string{"openalex", "wikidata", "wikipedia"})
	remove_key(obj, []string{"image_url", "image_thumbnail_url", "works_api_url", "related_concepts"})
	if _, ok := obj["ancestors"]; ok {
		for _, item := range obj["ancestors"].([]interface{}) {
			shorten_url(item, []string{"id", "wikidata"})
		}
	}
	if _, ok := obj["related_concepts"]; ok {
		for _, item := range obj["related_concepts"].([]interface{}) {
			shorten_url(item, []string{"id", "wikidata"})
		}
	}
	remove_empty_key(obj)
}

func (c *InstitutionProject) ParseData(obj map[string]interface{}) {

	shorten_url(obj, []string{"id", "ror", "wikidata"})
	shorten_url(obj["ids"], []string{"openalex", "wikidata", "ror", "wikipedia"})
	remove_key(obj, []string{"image_url", "image_thumbnail_url", "works_api_url", "associated_institutions", "x_concepts"})
	remove_empty_key(obj)
	remove_empty_key(obj["geo"])

	if value, ok := obj["lineage"]; ok {
		obj["lineage"] = shorten_id_form_list(value)
	}
	for _, csItem := range iteratorList(obj["roles"]) {
		shorten_url(csItem, []string{"id"})
	}
}

func (c *PublisherProject) ParseData(obj map[string]interface{}) {

	shorten_url(obj, []string{"id"})
	shorten_url(obj["ids"], []string{"openalex", "wikidata", "ror", "wikipedia"})
	remove_key(obj, []string{"image_url", "sources_api_url", "x_concepts", "image_thumbnail_url"})

	if value, ok := obj["lineage"]; ok {
		obj["lineage"] = shorten_id_form_list(value)
	}
	for _, csItem := range iteratorList(obj["roles"]) {
		shorten_url(csItem, []string{"id"})
	}
	remove_empty_key(obj)
}

func (c *FunderProject) ParseData(obj map[string]interface{}) {

	shorten_doi(obj["ids"])
	shorten_url(obj, []string{"id", "ror", "wikidata"})
	shorten_url(obj["ids"], []string{"openalex", "ror", "wikidata"})
	remove_key(obj, []string{"image_url", "image_thumbnail_url", "works_api_url", "x_concepts"})

	for _, csItem := range iteratorList(obj["roles"]) {
		shorten_url(csItem, []string{"id"})
	}
	remove_empty_key(obj)
}

func (c *SourceProject) ParseData(obj map[string]interface{}) {

	shorten_url(obj, []string{"id", "ror", "wikidata"})
	shorten_url(obj["ids"], []string{"openalex", "wikidata"})
	remove_key(obj, []string{"works_api_url", "x_concepts"})
	remove_empty_key(obj)
}

func (c *AuthorProject) ParseData(obj map[string]interface{}) {

	remove_empty_key(obj)
	shorten_url(obj, []string{"id", "orcid"})
	shorten_url(obj["ids"], []string{"openalex", "orcid"})
	shorten_url(obj["last_known_institution"], []string{"id", "ror"})
	remove_key(obj, []string{"works_api_url", "x_concepts"})

	if lki, ok := obj["last_known_institution"]; ok {
		lkiObj := lki.(map[string]interface{})
		if value, ok := lkiObj["lineage"]; ok {
			lkiObj["lineage"] = shorten_id_form_list(value)
		}
	}

}

func (c *WorkProject) ParseData(obj map[string]interface{}) {
	remove_key(obj, []string{"apc_list", "apc_paid", "ngrams_url", "cited_by_api_url", "sustainable_development_goals"})
	remove_key(obj["ids"], []string{"openalex", "doi"})
	remove_empty_key(obj)
	remove_empty_key(obj["biblio"])
	remove_empty_key(obj["open_access"])

	shorten_doi(obj)
	shorten_doi(obj["ids"])
	shorten_url(obj, []string{"id", "orcid"})
	shorten_url(obj["ids"], []string{"pmid"})

	for _, key := range []string{"corresponding_institution_ids", "corresponding_author_ids", "related_works"} {

		cache := []string{}
		for _, item := range iteratorList(obj[key]) {
			parts := strings.Split(item.(string), "/")
			lastPart := parts[len(parts)-1]
			cache = append(cache, lastPart)
		}
		if len(cache) > 0 {
			obj[key] = cache
		}

	}

	for _, asItem := range iteratorList(obj["authorships"]) {
		if asObj, ok := asItem.(map[string]interface{}); ok {
			shorten_url(asObj["author"], []string{"id", "orcid"})
			remove_key(asObj, []string{"raw_affiliation_string"})
			remove_empty_key(asObj["author"])
			remove_empty_key(asObj)

			for _, isItem := range iteratorList(asObj["institutions"]) {
				if isObj, ok := isItem.(map[string]interface{}); ok {
					shorten_url(isObj, []string{"id", "ror"})
					remove_empty_key(isObj)

					if leObj, ok := isObj["lineage"]; ok {
						cache := []string{}
						for _, leItem := range leObj.([]interface{}) {
							parts := strings.Split(leItem.(string), "/")
							lastPart := parts[len(parts)-1]
							cache = append(cache, lastPart)
						}
						isObj["lineage"] = cache
					}
				}

			}

		}
	}

	for _, csItem := range iteratorList(obj["concepts"]) {
		shorten_url(csItem, []string{"id"})
		remove_empty_key(csItem)
		remove_key(csItem, []string{"wikidata"})

		// unife score to float64
		csObj := csItem.(map[string]interface{})
		var floatNum float32
		switch v := csObj["score"].(type) {
		case int:
			floatNum = float32(v)
		case float32:
			floatNum = v
		case float64:
			floatNum = float32(v)
		}
		csObj["score"] = floatNum

	}

	for _, loItem := range iteratorList(obj["locations"]) {
		shorten_doi(loItem)
		if sourceObj, ok := loItem.(map[string]interface{}); ok {
			remove_empty_key(sourceObj["source"])
			remove_empty_key(sourceObj)

			shorten_url(sourceObj["source"], []string{"id", "host_organization", "publisher_id"})

			if sourceObj["source"] != nil {
				subSource := sourceObj["source"].(map[string]interface{})
				if linageValue, ok := subSource["host_institution_lineage"]; ok {
					subSource["host_institution_lineage"] = shorten_id_form_list(linageValue)
				}
				if linageValue, ok := subSource["host_organization_lineage"]; ok {
					subSource["host_organization_lineage"] = shorten_id_form_list(linageValue)
				}
				if linageValue, ok := subSource["publisher_lineage"]; ok {
					subSource["publisher_lineage"] = shorten_id_form_list(linageValue)
				}
			}

		}
	}

	{
		loItem := obj["primary_location"]
		if loItem != nil {
			shorten_doi(loItem)
			if sourceObj, ok := loItem.(map[string]interface{}); ok {
				remove_empty_key(sourceObj["source"])
				remove_empty_key(sourceObj)
				shorten_url(sourceObj["source"], []string{"id", "host_organization", "publisher_id"})
				if sourceObj["source"] != nil {

					subSource := sourceObj["source"].(map[string]interface{})
					if linageValue, ok := subSource["host_institution_lineage"]; ok {
						subSource["host_institution_lineage"] = shorten_id_form_list(linageValue)
					}
					if linageValue, ok := subSource["host_organization_lineage"]; ok {
						subSource["host_organization_lineage"] = shorten_id_form_list(linageValue)
					}
					if linageValue, ok := subSource["publisher_lineage"]; ok {
						subSource["publisher_lineage"] = shorten_id_form_list(linageValue)
					}
				}

			}
		}
	}

	if _, ok := obj["referenced_works"]; ok {
		obj["referenced_works"] = shorten_id_form_list(obj["referenced_works"])
	}
	if _, ok := obj["abstract_inverted_index"]; ok {
		obj["abstract"] = unAbstractInvertedIndex(obj["abstract_inverted_index"])
		remove_key(obj, []string{"abstract_inverted_index"})
	}

}

func Main() {
	// mergeIDSet := getMergeIDs("sources", foldPath)
	// log.Info().Int("size", mergeIDSet.Size()).Msg("start")

	foldPath := "/mnt/sata3/openalex/openalex-snapshot-v20231101/data"
	Version := "20231101"
	cp := NewConceptProject(foldPath)
	RuntimeFlow(cp, 10, Version)

	// cp := NewFunderProject(foldPath)
	// RuntimeFlow(cp, 1, Version)

	// es := getElasticClient()
	// initElastic("works", "test", es)
}
