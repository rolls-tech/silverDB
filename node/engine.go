package node

import (
	"log"
	"silverDB/config"
	"silverDB/node/point"
	"silverDB/node/storage"
)



type Storage interface {
	ReadTsData(string,string,string,string,map[string]string,int64,int64) map[int64]float64
	WriteTsData(*point.WritePoint,string,[]byte,int,int64,int64) error

}


type engine struct {
	wal *storage.WalBuffer
}

func (e *engine) ReadTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) map[int64]float64 {
	return nil
}


func (e *engine) WriteTsData(wp *point.WritePoint,tagKv string,data []byte,dataLen int ,timestamp,id int64) error {
    er:=e.wal.WriteData(wp,tagKv,data,dataLen,timestamp,id)
    if er !=nil {
    	log.Println("write data failed !" ,e)
	}
	return er
}


func NewStorage(config config.NodeConfig) Storage {
	var s Storage
	s = &engine{
		wal:storage.NewWalBuffer(config),
	}
	log.Println("silverDB storage service ready to serve")
	return s
}


