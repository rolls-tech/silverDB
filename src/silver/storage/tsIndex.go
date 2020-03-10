package storage

import (
	"sort"
	"sync"
	"time"
)

type tagsFieldKeys struct {
	tags map[string]string
	indexFieldKey map[string]bool
}

type memTsIndex struct {
	index map[string]map[string]*tagsFieldKeys
	ttl time.Duration
	size int64
	count int64
	maxSize int64
}

type tsIndex struct {
	mu sync.RWMutex
	indexDir []string
	inMemIndex  *memTsIndex
}

func NewInvertedTsIndex(ttl int) *memTsIndex{
	inTsIndex:=&memTsIndex{
		index:   make(map[string]map[string]*tagsFieldKeys,0),
		ttl:     time.Duration(ttl) * time.Second,
		size:    0,
		count:   0,
		maxSize: 0,
	}
	if ttl > 0 {
		go inTsIndex.expire()
	}
	return inTsIndex
}

func NewTsIndex(ttl int,indexDir []string) *tsIndex{
	tsIndex:=&tsIndex{
		mu:       sync.RWMutex{},
		indexDir: indexDir,
		inMemIndex: NewInvertedTsIndex(ttl)  ,
	}
	return tsIndex
}

func (n *tsIndex) writeIndex(tableName,fieldKey string,tags map[string]string,walCh chan bool) {
	for {
		ok:= <- walCh
		if ok {
		//	go n.writeTsIndex(tableName,fieldKey,tags)
		}
	}
}

/*
func (n *tsIndex) readTsIndex(tableName string,tags map[string]string) *tagsFieldKeys {
	n.mu.RLock()
	defer n.mu.RUnlock()
	kvSet,ok:=n.inMemIndex.index[tableName]
	if !ok {
		return nil
	}
	var tempTags []string
	var tempTagKv []string
	for tagK,tagV:=range tags {
		tempTags=append(tempTags,tagK+tagV)
	}
	sort.Strings(tempTags)
	tempTagKv=combine(0, len(tempTags),tempTags,"",tempTagKv)
	tagKv:=tempTagKv[0]
	if kvSet !=nil {
		for _,kv:=range kvSet {
			fk,ok:=kv[tagKv]
			if ok {
				return fk
			} else {
				return nil
			}
		}
	}
	return nil
}

func (n *tsIndex) writeTsIndex(tableName,fieldKey string,tags map[string]string) {
	  n.mu.Lock()
	  defer n.mu.Unlock()
      kvSet,ok:=n.inMemIndex.index[tableName]
	  fk:=make(map[string]bool)
	  fk[fieldKey]=true
	  tk:=&tagsFieldKeys{
		tags:          tags,
		indexFieldKey: fk,
	  }
      if !ok {
      	var ts []string
      	if tags != nil {
			ts=generateTagPrefix(tags)
		}
        for i,kv:=range ts {
        	kvSet[i][kv]=tk
		}
	  } else {
	  	var tempTags []string
	  	var tempTagKv []string
	    for tagK,tagV:=range tags {
	    	tempTags=append(tempTags,tagK+tagV)
		}
	    sort.Strings(tempTags)
	    tempTagKv=combine(0, len(tempTags),tempTags,"",tempTagKv)
	    tagKv:=tempTagKv[0]
        for _,kv:=range kvSet {
        	 fk,ok:=kv[tagKv]
        	 if ok {
        	    _,ok:=fk.indexFieldKey[fieldKey]
        	    if ok {
					return
			    } else {
			    	fk.indexFieldKey[fieldKey]=true
				}
			 } else {
			 	kv[tagKv]=tk
			 }
		}
	  }
}

*/

func (n *tsIndex) readIndexFile() {


}

func (n *tsIndex) writeIndexFile() {


}

func (index *memTsIndex)  expire() {

}

func generateTagPrefix(tags map[string]string) []string {
    var ts []string
	var tagSet []string
	if tags != nil {
		for tagK,tagV:=range tags {
			ts=append(ts,tagK+tagV)
		}
	}
    sort.Strings(ts)
    tsLen:= len(ts)
    for i := 1; i <= tsLen; i++ {
    	tagSet=combine(0,i,ts,"",tagSet)
	}
	return tagSet
}

func combine (index,n int,ts []string,temp string,tagSet []string) []string {
	if n==1 {
		for i:=index; i< len(ts); i++ {
           a:=temp+ts[i]
           tagSet=append(tagSet,a)
	    }
	} else {
		for i:=index; i< len(ts); i++ {
			b:=temp+ts[i]
			tagSet=combine(i+1,n-1,ts,b,tagSet)
		}
	}
	return tagSet
}










