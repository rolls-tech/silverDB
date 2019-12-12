package storage

import (
	"github.com/boltdb/bolt"
	"silver/result"
	"sync"
)

type inMemory struct {
	c     map[string][]byte
	mutex sync.RWMutex
	Stat
}

func (c *inMemory) SetKv(key string,value []byte) error {
    return nil
}

func (c *inMemory) GetKv(key string) ([]byte,error) {
       return nil,nil
}

func (c *inMemory) DelKv(key string) error {
return nil
}

func (c *inMemory) SetDBandKV(database, table, k string, v []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var key string
	key = database + table + k
	tmp, exist := c.c[key]
	if exist {
		c.Delstat(key, tmp)
	}
	c.c[key] = v
	c.Addstat(key, v)
	return nil
}

func (c *inMemory) GetDBandKV(database, table, k string) ([]byte, *bolt.DB, error) {
	c.mutex.RLock()
	defer c.mutex.RLock()
	var key string
	key = database + table + k
	return c.c[key], nil, nil
}

func (c *inMemory) DelDBandKV(database, table, k string) (*bolt.DB, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var key string
	key = database + table + k
	v, exist := c.c[key]
	if exist {
		delete(c.c, key)
		c.Delstat(key, v)
	}
	return nil, nil
}

func (c inMemory)  SetTSData(database,table,rowKey,key string,value []byte,dataTime int64) error {
	return nil
}

func (c *inMemory) GetTimeRangeData(wg *sync.WaitGroup,database,table,rowKey,key string,startTime,endTime int64,data []*result.TsField) ([]*result.TsField,error) {
	return nil,nil
}

func (c *inMemory) DelTSData(database,table,rowkey,key string,dataTime,endTime int64) (*bolt.DB,error) {
	return nil,nil
}


func (c *inMemory) GetStat() Stat {
	return c.Stat
}

func NewInMemory() *inMemory {
	return &inMemory{
		c:     make(map[string][]byte, 0),
		mutex: sync.RWMutex{},
		Stat:  Stat{},
	}
}
