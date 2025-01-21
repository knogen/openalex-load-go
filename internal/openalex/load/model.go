package load

type worksJson struct {
	ID                   string   `json:"id" bson:"_id"`
	PublicationYear      int32    `json:"publication_year"`
	ReferencedWorksCount int32    `json:"referenced_works_count"`
	ReferencedWorks      []string `json:"referenced_works"`
	Concepts             []struct {
		ID          string  `json:"id"`
		Wikidata    string  `json:"wikidata"`
		DisplayName string  `json:"display_name"`
		Level       int     `json:"level"`
		Score       float64 `json:"score"`
	} `json:"concepts"`
}

type worksMongo struct {
	ID                   int64    `json:"id" bson:"_id"`
	PublicationYear      int32    `json:"publication_year" bson:"publication_year"`
	ReferencedWorksCount int32    `json:"referenced_works_count" bson:"referenced_works_count,omitempty"`
	ReferencedWorks      []int64  `json:"referenced_works" bson:"referenced_works,omitempty"`
	LinksInWorksCount    int32    `json:"-" bson:"links_in_works,omitempty"` //require computing
	ConceptsLv0          []string `json:"-" bson:"Concepts_lv0,omitempty"`   //require computing
	ConceptsLv1          []string `json:"-" bson:"Concepts_lv1,omitempty"`   //require computing
	ConceptsLv2          []string `json:"-" bson:"Concepts_lv2,omitempty"`   //require computing
}

// type worksMongo struct {
// 	ID                   int   `json:"id" bson:"_id"`
// 	PublicationYear      int   `json:"publication_year" bson:"y"`
// 	ReferencedWorksCount int   `json:"referenced_works_count" bson:"c"`
// 	ReferencedWorks      []int `json:"referenced_works" bson:"o,omitempty"`
// }

type conceptsMongo struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Level       int    `json:"level"`
	Description string `json:"description"`
	Ancestors   []struct {
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
		Level       int    `json:"level"`
	} `json:"ancestors"`
}
