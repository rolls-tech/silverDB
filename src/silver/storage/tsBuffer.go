package storage

import (
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
	"time"
)

type wData struct {
	wp *WPoint
	created time.Time
}

type tsBufferData struct {
	mutex     sync.RWMutex
	buffer     map[string]*wData
	size int64
	maxSize int64
	count int64
	ttl time.Duration
	tss *tss
}

func (b *tsBufferData) writeTsBuffer(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64][]byte) error {
	seriesKey:=dataBase+tableName+tagKv
	tsValue:=&Value{
		Kv:value,
	}
	serializeValue,e:=proto.Marshal(tsValue)
	if e != nil {
		log.Println("serialize value failed",e)
	}
	valueSize:=int64(len(serializeValue))
	b.mutex.Lock()
	defer b.mutex.Unlock()
	_,ok:=b.buffer[seriesKey]
	if !ok {
	   skSize:=int64(len(seriesKey))
	   fieldKeyValue:=make(map[string]*Value,0)
	   fieldKeyValue[fieldKey]=tsValue
	   wp:=&WPoint{
		   DataBase:             dataBase,
		   TableName:            tableName,
		   Tags:                 tags,
		   Value:                fieldKeyValue,
	   }
	   wd:=&wData{
		   wp:      wp,
		   created: time.Now(),
	   }
	   serializeWp,e:=proto.Marshal(wp)
	   serializeT,e:=wd.created.MarshalBinary()
	   if e !=nil {
	   	  log.Println("serialize wPoint or wData failed",e)
	   	  return e
	   }
	   wpSize:=int64(len(serializeWp))
	   wdSize:=wpSize+int64(len(serializeT))
	   b.buffer[seriesKey]=wd
	   b.size=wdSize+skSize+valueSize
	   if value != nil {
			for _,_=range value {
				b.count+=1
			}
		}
	}else {
		v,ok:=b.buffer[seriesKey].wp.Value[fieldKey]
		b.buffer[seriesKey].created=time.Now()
		if !ok {
			b.buffer[seriesKey].wp.Value[fieldKey]=tsValue
			b.size+=valueSize
		} else {
			if value != nil {
				for kt,vv:=range value {
					_,exist:=v.Kv[kt]
					if exist {
						b.size-=int64(len(v.Kv[kt]))
						v.Kv[kt]=vv
						b.size+=int64(len(vv))
					}else {
						v.Kv[kt]=vv
						b.size=kt+int64(len(vv))
						b.count+=1
					}
				}
			}
		}
	}
	return nil
}

func (b *tsBufferData) readTsBuffer(dataBase,tableName,tagKv,fieldKey string,tags map[string]string,startTime,endTime int64) map[int64][]byte{
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	seriesKey:=dataBase+tableName+tagKv
	readTsBufferKv:=make(map[int64][]byte,0)
	wd,ok:=b.buffer[seriesKey]
	if ok {
		tsValue,ok:=wd.wp.Value[fieldKey]
		if ok {
			if tsValue.Kv != nil {
				for kt,vv:=range tsValue.Kv {
					if kt >= startTime && kt <= endTime {
						readTsBufferKv[kt]=vv
					}
				}
				return readTsBufferKv
			}
		}
	}
	return nil
}

func NewTsBufferData(ttl int,tss *tss) *tsBufferData{
	tsBuffer:=&tsBufferData{
		mutex:   sync.RWMutex{},
		buffer:   make(map[string]*wData,0),
		size:    0,
		maxSize: 0,
		count:0,
		ttl: time.Duration(ttl) * time.Second,
		tss: tss}
	if ttl > 0 {
		go tsBuffer.expire()
	}
	return tsBuffer

}

func (b *tsBufferData) expire() {
	for {
		time.Sleep(b.ttl)
		b.mutex.RLock()
		if b.buffer != nil {
			for seriesKey,wd:=range b.buffer {
				b.mutex.RUnlock()
				if wd.created.Add(b.ttl).Before(time.Now()) {
					b.tss.writeTsData(wd.wp)
					delete(b.buffer,seriesKey)
				}
				b.mutex.RLock()
			}
		}
		b.mutex.RUnlock()
	}
}

