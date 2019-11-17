package storage

import (
	"github.com/boltdb/bolt"
	"sync"
)

type inMemory struct {
	c map[string][]byte
	mutex sync.RWMutex
	Stat
}

func (c *inMemory) Set(k string,v []byte) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	tmp,exist:=c.c[k]
	if exist {
		c.Delstat(k,tmp)
	}
	c.c[k]=v
	c.Addstat(k,v)
	return nil
}

func (c *inMemory) Get(k string) ([]byte,*bolt.DB,error){
	c.mutex.RLock()
	defer c.mutex.RLock()
	return c.c[k],nil,nil
}

func (c *inMemory) Del(k string) (*bolt.DB,error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	v,exist:=c.c[k]
	if exist{
		delete(c.c,k)
		c.Delstat(k,v)
	}
	return nil,nil
}

func (c *inMemory) GetStat() Stat {
	return c.Stat
}

func NewInMemory() *inMemory{
	return &inMemory{
		c:     make(map[string][]byte),
		mutex: sync.RWMutex{},
		Stat:  Stat{},
	}
}





































