package cache

import "log"

type Cache interface {
	Set(string,[]byte) error
	Get(string) ([]byte,error)
	Del(string) error
	GetStat() Stat
}

func New(typ string,dataPath string,dataBase string,table string) Cache {

	var c Cache
	if typ == "inmemory" {
		c= newInMemoryCache()
	}
	if typ == "boltdb" {
		c= newBoltdb(dataPath,dataBase,table)
	}
	if c == nil {
		panic("unknown cache type"+typ)
	}
	log.Println(typ,"ready to serve")
	return  c

}