package storage

import (
	"github.com/boltdb/bolt"
	"log"
	"silver/result"
	"sync"
)

type Storage interface {
	SetKv(string,[]byte) error
	GetKv(string) ([]byte,error)
	DelKv(string) error
	SetDBandKV(string,string,string,[]byte) error
	GetDBandKV(string,string,string) ([]byte,*bolt.DB,error)
	DelDBandKV(string,string,string) (*bolt.DB,error)
	SetTSData(string,string,string,string,[]byte,int64) error
	GetTimeRangeData(*sync.WaitGroup,string,string,string,string,int64,int64,[]*result.TsField) ([]*result.TsField,error)
	DelTSData(string,string,string,string,int64,int64) (*bolt.DB,error)
	GetStat() Stat
}

func New(typ string, dataPath []string) Storage {
	var s Storage
	if typ == "kvCache" {
		s = NewInMemory()
	}
	if typ == "dbStorage" {
		s = NewBolt(dataPath)
	}
	if typ == "tsStorage" {
		s = NewTss(dataPath)
	}
	if s == nil {
		panic("unknown storage type" + typ)
	}
	log.Println(typ, "ready to serve")
	return s

}
