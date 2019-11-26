package storage

import (
	"github.com/boltdb/bolt"
	"log"
)

type Storage interface {
	Set(string, string, string, []byte) error
	Get(string, string, string) ([]byte, *bolt.DB, error)
	Del(string, string, string) (*bolt.DB, error)
	GetStat() Stat
}

func New(typ string, dataPath []string) Storage {
	var s Storage
	if typ == "memory" {
		s = NewInMemory()
	}
	if typ == "bolt" {
		s = NewBolt(dataPath)
	}
	if s == nil {
		panic("unknown storage type" + typ)
	}
	log.Println(typ, "ready to serve")
	return s

}
