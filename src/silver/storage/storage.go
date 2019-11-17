package storage

import (
	"github.com/boltdb/bolt"
	"log"
)

type Storage interface {
	Set(string,[]byte) error
	Get(string) ([]byte,*bolt.DB,error)
	Del(string) (*bolt.DB,error)
	GetStat() Stat
}

func New(typ string,dataPath string,dataBase string,table string) Storage {

	var s Storage
	if typ == "inmemory" {
		s= NewInMemory()
	}
	if typ == "boltdb" {
		s= NewBoltdb(dataPath,dataBase,table)
	}
	if s == nil {
		panic("unknown storage type"+typ)
	}
	log.Println(typ,"ready to serve")
	return  s

}