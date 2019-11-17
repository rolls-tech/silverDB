package storage

type Stat struct {
	Count int64
	KeySize int64
	ValueSize int64
}

func (s *Stat) Addstat(k string,v []byte){
     s.Count+=1
     s.KeySize+=int64(len(k))
     s.ValueSize+=int64(len(v))
}

func (s *Stat) Delstat(k string,v []byte){
	s.Count-=1
	s.KeySize-=int64(len(k))
	s.ValueSize-=int64(len(v))
}




















