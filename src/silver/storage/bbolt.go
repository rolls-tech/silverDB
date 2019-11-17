package storage

import (
	"github.com/boltdb/bolt"
	"log"
	"sync"
)

type bbolt struct {
	 c map[string][]byte
	 mutex sync.RWMutex
     FilePath string
     FileName string
	 DbName string
	 OpBucket string
     buckets map[string]*bucket
	Stat
}

type bucket struct {
	minKey string
	maxKey string
    minTime int64
    maxTime int64
	Stat
}


func (b *bbolt) Set(k string, v []byte) error {
	db:=b.openDB()
	defer db.Close()
	if err:= db.Update(func(tx *bolt.Tx) error {
		t, err := tx.CreateBucketIfNotExists([]byte(b.OpBucket))
		if err != nil {
			return err
		}
		if err:=t.Put([]byte(k),v); err!=nil {
			return err
		}
		b.Addstat(k,v)
		return nil
	}); err !=nil {
		return err
	}
	return nil
}

func (b *bbolt) Get(k string) ([]byte,*bolt.DB,error) {
	db:=b.openDB()
	var v []byte
	if err:=db.View(func(tx *bolt.Tx) error {
      v=tx.Bucket([]byte(b.OpBucket)).Get([]byte(k))
      return nil
	});err!= nil {
		log.Println(err)
	}
    return v,db,nil
}

func (b *bbolt) Del(k string) (*bolt.DB,error) {
	db:=b.openDB()
	var v []byte
	if err:=db.View(func(tx *bolt.Tx) error {
		v=tx.Bucket([]byte(b.OpBucket)).Get([]byte(k))
		err:=tx.Bucket([]byte(b.OpBucket)).Delete([]byte(k))
		if err !=nil {
			log.Println(err)
		}
		b.Delstat(k,v)
		return nil
	});err!= nil {
		log.Println(err)
	}
	return db,nil
}

func (b *bbolt) GetStat() Stat {
	return b.Stat
}


func NewBoltdb(dataPath string,dataBase string,table string) *bbolt {
    return &bbolt{
		c:        make(map[string][]byte),
		mutex:    sync.RWMutex{},
		FilePath: dataPath,
		FileName: "",
		DbName:   dataBase,
		OpBucket: table,
		buckets:  make(map[string]*bucket),
		Stat:     Stat{},
	}
}

func (b *bbolt) openDB() *bolt.DB {
    db,err := bolt.Open(b.FilePath+b.DbName+".db",777,nil)
    if err !=nil {
    	log.Println(err.Error())
	}
	return db
}


func (b *bbolt) updateIndex(k string,v []byte){
	if t,exist:=b.buckets[b.OpBucket]; !exist {
		b.buckets[b.OpBucket]= &bucket {
			minKey:k,
		}
	} else {
		t.Addstat(k,v)
		if t.minKey > k {
			t.minKey=k
		}
		if t.maxKey < k {
			t.maxKey=k
		}
	}
}


func (b *bbolt) recoverBuckets(){

}

func (b *bbolt) recoverStats(){

}