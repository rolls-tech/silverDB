package utils


type SortMap []Point

type Point struct {
	T int64
	V float64
}

func NewSortMap(m map[int64]float64) SortMap {
	sm:=make(SortMap,0,len(m))
	for k,v:=range m {
		sm=append(sm,Point{k,v})
	}
	return sm
}

func (sm SortMap) Len() int {
	return len(sm)
}

func (sm SortMap) Less(i,j int) bool {
	return sm[i].T < sm[j].T
}

func (sm SortMap) Swap(i,j int) {
	sm[i],sm[j]=sm[j],sm[i]
}
