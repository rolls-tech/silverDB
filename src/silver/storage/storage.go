package storage

import (
	"github.com/boltdb/bolt"
	"log"
)

type Storage interface {
	ReadTsData(string,string,string,string,map[string]string,int64,int64) (map[int64][]byte,[]*bolt.DB)
	WriteTsData(string,string,string,string,map[string]string,map[int64][]byte) error
}

func New(typ string, tss *tss) Storage {
	var s Storage
	if typ == "tsStorage" {
		s=NewTsKv(15,tss)
	}
	if s == nil {
		panic("unknown storage type" + typ)
	}
	log.Println(typ, "ready to serve")
	return s
}
