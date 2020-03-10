package storage

import (
	"log"
	"silver/metadata"
)

type Storage interface {
	ReadTsData(string,string,string,string,map[string]string,int64,int64) map[int64]float64
	WriteTsData(string,string,string,string,map[string]string,map[int64]float64) error

}

func New(typ string, walDir string,indexDir []string,tss *tss,meta *metadata.Meta,flushCount int64) Storage {
	var s Storage
	if typ == "tsStorage" {
		s=NewTsKv(3,walDir,indexDir,tss,meta,false,true,flushCount)
	}
	if s == nil {
		panic("unknown storage type" + typ)
	}
	log.Println(typ, "ready to serve")
	return s
}
