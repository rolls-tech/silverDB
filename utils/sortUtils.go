package utils


type SortMap []Point

type Point struct {
	T int64
	V []byte
}

func NewSortMap(m map[int64][]byte) SortMap {
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

func mergeSort(leftArr []Point,rightArr []Point) []Point {
	//初始化一个数组
	result:=make([]Point, len(leftArr)+len(rightArr))
	i:=0
	for len(leftArr) > 0 && len(rightArr) > 0 {
		//比较子表元素第一个元素，将较小的放入待初始化合并的数组
		if leftArr[0].T <= rightArr[0].T {
			result[i]=leftArr[0]
			i++
			leftArr=leftArr[1:]
		} else {
			result[i]=rightArr[0]
			i++
			rightArr=rightArr[1:]
		}
	}
	//将剩下的左边元素给result
	for len(leftArr) > 0 {
		result[i]=leftArr[0]
		i++
		leftArr=leftArr[1:]
	}
	//将剩下的右边元素给result
	for len(rightArr) > 0 {
		result[i]=rightArr[0]
		i++
		rightArr=rightArr[1:]
	}
	return result
}

func spiltArr(arr []Point) []Point {
	if len(arr) < 2 {
		return arr
	}
	//切分数组
	mid := len(arr) / 2
	leftArr := arr[0:mid]
	rightArr := arr[mid:]
	//递归切分、合并
	return mergeSort(spiltArr(leftArr), spiltArr(rightArr))
}

func CombineSort(arr []Point) []Point {
	result:=spiltArr(arr)
	return result
}