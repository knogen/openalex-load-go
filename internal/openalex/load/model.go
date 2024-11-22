package load

type worksJson struct {
	ID                   string   `json:"id" bson:"_id"`
	PublicationYear      int      `json:"publication_year"`
	ReferencedWorksCount int      `json:"referenced_works_count"`
	ReferencedWorks      []string `json:"referenced_works"`
}

type worksMongo struct {
	ID                   int   `json:"id" bson:"_id"`
	PublicationYear      int   `json:"publication_year" bson:"publication_year"`
	ReferencedWorksCount int   `json:"referenced_works_count" bson:"referenced_works_count"`
	ReferencedWorks      []int `json:"referenced_works" bson:"referenced_works,omitempty"`
}
