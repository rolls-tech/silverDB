package node

import (
	"log"
	"silver/config"
	"silver/metastore"
	"silver/node/point"
	"silver/node/storage"
	"strings"
)



type Storage interface {
	ReadTsData(*point.ReadPoint,string,chan *point.ReadPoint)
	WriteTsData(*point.WritePoint,string,[]byte,int,int64,int64) error

}


type engine struct {
	wal *storage.WalBuffer
	index *storage.Index
	buffer *storage.DataBuffer
}

func (e *engine) ReadTsData(readPoint *point.ReadPoint,tagKv string,c chan *point.ReadPoint) {
    tagsMetrics,er:=e.index.ReadData(readPoint,tagKv)
    if er !=nil {
    	log.Println("read index data failed !",er)
    	c <- &point.ReadPoint{}
	}
    if tagsMetrics != nil {
	   for tagKv,metrics:=range tagsMetrics {
	   	   rp:=&point.ReadPoint{
			   DataBase:             readPoint.DataBase,
			   TableName:            readPoint.TableName,
			   Tags:                 make(map[string]string,0),
			   Metrics:              make(map[string]*point.Value,0),
			   StartTime:            readPoint.StartTime,
			   EndTime:              readPoint.EndTime,
		   }

	   	   if metrics !=nil && len(metrics) > 0 {
	   	   	   for _,metric:=range metrics {
				   kv:=e.buffer.ReadData(rp.DataBase,rp.TableName,tagKv,metric,rp.StartTime,rp.EndTime)
				   if kv != nil {
				   	   _,ok:=rp.Metrics[metric]
				   	   if !ok {
				   	   	  rp.Metrics[metric]=&point.Value{
							  Kv: make(map[int64]float64),
						  }
					   }
					   rp.Metrics[metric].Kv=kv
				   }
			   }
		   }
	   	   if tagKv != "" {
	   	   	  tags:=strings.Split(tagKv,";")
	   	   	  if len(tags) > 0 {
	   	   	  	 for _,tag:=range tags {
	   	   	  	 	if tag != "" {
						tagK:=strings.Split(tag,"=")[0]
						tagV:=strings.Split(tag,"=")[1]
						rp.Tags[tagK]=tagV

					}
				 }
			  }
		   }
	   	   c <- rp
	   }
    }
}


func (e *engine) WriteTsData(wp *point.WritePoint,tagKv string,data []byte,dataLen int ,timestamp,id int64) error {
    e.wal.WriteData(wp,tagKv,data,dataLen,timestamp,id)
    //er:=e.buffer.WriteData(wp,tagKv)
    //er=e.index.WriteData(wp,tagKv)*/
	return nil
}


func NewStorage(config config.NodeConfig,listener1 *metastore.Listener,register1 *metastore.Register) Storage {
	var s Storage
	s = &engine {
		wal:    storage.NewWalBuffer(config),
		/*index:  storage.NewIndex(config.Flush.TTL,config.IndexDir),
		buffer: storage.NewDataBuffer(config,listener1,register1),*/
	}
	log.Println("silverDB storage ready to service")
	return s
}


