package wikite

type (
	SentenceRecord struct {
		Links []string `json:"links"`
		Text  string   `json:"text"`
	}

	ArticleRecord struct {
		Revision  int              `json:"revision"`
		Id        int              `json:"id"`
		Sentences []SentenceRecord `json:"sentences"`
	}

	ReferenceRecord struct {
		ArticleId int    `json:"article_id"`
		Text      string `json:"text"`
		Reference string `json:"reference"`
	}
)
