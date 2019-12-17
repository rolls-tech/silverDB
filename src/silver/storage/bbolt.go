package storage

import (
	"github.com/boltdb/bolt"
	"log"
	"math/rand"
	"silver/result"
	"sync"
	"time"
)

type bbolt struct {
	c             map[string][]byte
	mutex         sync.RWMutex
	DataDir       []string
	DataFileIndex map[*bucket]string
	Stat
}

func (b *bbolt) SetKv(key string, value []byte) error {
	return nil
}

func (b *bbolt) GetKv(key string) ([]byte, error) {
	return nil,nil
}

func (b *bbolt) DelKv(key string) error {
	return nil
}

func (b *bbolt) SetDBandKV(dataBase, table, k string, v []byte) error {
	dataFile, _ := b.setDataFile(dataBase, table, k)
	db := b.openDB(dataFile)
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		t, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return err
		}
		if err := t.Put([]byte(k), v); err != nil {
			return err
		}
		b.Addstat(k, v)
		return nil
	}); err != nil {
		return err
	}
	b.updateBucketIndex(dataBase, table, k, dataFile)
	return nil
}

func (b *bbolt) GetDBandKV(dataBase, table, k string) ([]byte, *bolt.DB, error) {
	dataFile, _ := b.getDataFile(dataBase, table, k)
	if dataFile != "" {
		db := b.openDB(dataFile)
		var v []byte
		if err := db.View(func(tx *bolt.Tx) error {
			v = tx.Bucket([]byte(table)).Get([]byte(k))
			return nil
		}); err != nil {
			log.Println(err)
		}
		return v, db, nil
	}
	return nil,nil,nil
}

func (b *bbolt) DelDBandKV(dataBase, table, k string) (*bolt.DB, error) {
	dataFile, _ := b.getDataFile(dataBase, table, k)
	if dataFile !="" {
		db := b.openDB(dataFile)
		var v []byte
		var c *bolt.Cursor
		if err := db.View(func(tx *bolt.Tx) error {
			v = tx.Bucket([]byte(table)).Get([]byte(k))
			err := tx.Bucket([]byte(table)).Delete([]byte(k))
			t := tx.Bucket([]byte(table))
			c = t.Cursor()
			if err != nil {
				log.Println(err)
			}
			b.Delstat(k, v)
			return nil
		}); err != nil {
			log.Println(err)
		}
		b.deleteBucketIndex(dataBase, table, k, c)
		return db, nil
	}
	return nil,nil
}

func (b *bbolt) SetTSData(dataBase,table,rowKey,k string,v []byte,dataTime int64) error {
	return nil
}

func (b *bbolt) GetTimeRangeData(table,rowKey,k string,startTime,endTime int64) ([]*result.TsField, *bolt.DB,error) {
	return nil,nil,nil
}

func (b *bbolt) DelTSData(dataBase,table,rowKey,k string,startTime,endTime int64) (*bolt.DB, error) {
	return nil,nil
}

func (b *bbolt) GetStorageFile(string,string,int64,int64) []string {
	return nil
}

type bucket struct {
	mutex      sync.RWMutex
	BucketName string
	DdName     string
	MinKey     string
	MaxKey     string
	MinTime    int64
	MaxTime    int64
	DataFile   string
}

func (b *bbolt) GetStat() Stat {
	return b.Stat
}

func NewBolt(dataDir []string) *bbolt {
	return &bbolt{
		c:             make(map[string][]byte),
		mutex:         sync.RWMutex{},
		DataDir:       dataDir,
		DataFileIndex: make(map[*bucket]string),
		Stat:          Stat{},
	}
}

func (b *bbolt) openDB(dataFile string) *bolt.DB {
	db, err := bolt.Open(dataFile, 777, nil)
	if err != nil {
		log.Println(err.Error())
	}
	return db
}

func (b *bbolt) updateBucketIndex(dbName, bucketName string, key string, dataFile string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	var bc bucket
	if len(b.DataFileIndex) == 0 {
		bc.BucketName = bucketName
		bc.DdName = dbName
		bc.MinKey = key
		bc.MaxKey = key
		bc.DataFile = dataFile
		b.DataFileIndex[&bc] = dataFile
	} else {
		for buc, _ := range b.DataFileIndex {
			if buc.DdName == dbName {
				if buc.BucketName == bucketName {
					if buc.MaxKey <= key {
						buc.MaxKey = key
					}
					if buc.MinKey >= key {
						buc.MinKey = key
					}
				} else {
					bc.BucketName = bucketName
					bc.DdName = dbName
					bc.MinKey = key
					bc.MaxKey = key
					bc.DataFile = dataFile
					b.DataFileIndex[&bc] = dataFile
				}
			} else {
				bc.BucketName = bucketName
				bc.DdName = dbName
				bc.MinKey = key
				bc.MaxKey = key
				bc.DataFile = dataFile
				b.DataFileIndex[&bc] = dataFile
			}
		}
	}
}

func (b *bbolt) deleteBucketIndex(dbName, bucketName string, key string, c *bolt.Cursor) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for buc, _ := range b.DataFileIndex {
		if buc.BucketName == bucketName && buc.DdName == dbName {
			if buc.MaxKey <= key {
				k, _ := c.Last()
				buc.MaxKey = string(k)
			}
			if buc.MinKey >= key {
				k, _ := c.First()
				buc.MinKey = string(k)
			}
		}
	}
}

func (b *bbolt) setDataFile(dbName, bucketName string, key string) (string, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	var dataFile string
	var n int
	if len(b.DataFileIndex) == 0 {
		rand.Seed(time.Now().Unix())
		n = rand.Intn(len(b.DataDir))
		dataFile = b.DataDir[n] + dbName + ".db"
		return dataFile,nil
	} else {
		for buc, _ := range b.DataFileIndex {
			if buc.DdName == dbName {
				dataFile = buc.DataFile
				return dataFile, nil
			}
		}
		rand.Seed(time.Now().Unix())
		n = rand.Intn(len(b.DataDir))
		dataFile = b.DataDir[n] + dbName + ".db"
	}
	return dataFile, nil
}

func (b *bbolt) getDataFile(dbName, bucketName string, key string) (string, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	var dataFile string
	if len(b.DataFileIndex) == 0 {
		return "",nil
	} else {
		for buc, _ := range b.DataFileIndex {
			if buc.DdName == dbName {
				dataFile = buc.DataFile
				return dataFile, nil
			}
		}
	}
	return dataFile, nil
}


