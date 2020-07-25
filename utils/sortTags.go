package utils

type SortTags []TagKv

type TagKv struct {
	TagK string
	TagV string
}

func NewSortTags(m map[string]string) SortTags {
	sm := make(SortTags, 0, len(m))
	for k, v := range m {
		sm = append(sm, TagKv{k, v})
	}
	return sm
}

func (sm SortTags) Len() int {
	return len(sm)
}

func (sm SortTags) Less(i, j int) bool {
	return sm[i].TagK < sm[j].TagK
}

func (sm SortTags) Swap(i, j int) {
	sm[i], sm[j] = sm[j], sm[i]
}
