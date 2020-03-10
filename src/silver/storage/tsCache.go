package storage

import (
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
	"time"
)

type rData struct {
	rPoint map[string]*Value
	created time.Time
	startTime int64
	endTime int64
}

type tsCacheData struct {
	mutex     sync.RWMutex
	cache     map[string]*rData
	size int64
	maxSize int64
	count int64
	ttl time.Duration
	tss *tss
}


func (c *tsCacheData) writeTsCache(dataBase,tableName,tagKv,fieldKey string,tags map[string]string,
	startTime,endTime int64,value map[int64]float64) {
	  uniqueKey:=dataBase+tableName+tagKv+fieldKey
	  tsValue:=&Value{
		  Kv:      value,
	  }
	 serializeValue,e:=proto.Marshal(tsValue)
	  if e != nil {
		log.Println("serialize value failed",e)
	  }
	  valueSize:=int64(len(serializeValue))
	  ukSize:=int64(len(uniqueKey))
	  tnSize:=int64(len(tableName))
	  c.mutex.Lock()
	  defer c.mutex.Unlock()
	  rd,ok:=c.cache[tableName]
	  if !ok {
	  	 rd:=&rData {
			 rPoint:    make(map[string]*Value,0),
			 created:   time.Now(),
			 startTime: startTime,
			 endTime:   endTime,
		 }
	     rd.rPoint[uniqueKey]=tsValue
         c.cache[tableName]=rd
         c.size+=valueSize+ukSize+tnSize
         if value != nil {
			  for _,_=range value {
				 c.count+=1
			 }
		  }
	  } else {
	  	 v,ok:=rd.rPoint[uniqueKey]
	  	 if !ok {
             v.Kv=value
             c.size+=valueSize+ukSize
		 } else {
		    if value != nil {
		       for kt,vv:=range value {
				   _,exist:=v.Kv[kt]
				   if exist {
					 //  c.size-=int64(len(v.Kv[kt]))
					   v.Kv[kt]=vv
					  // c.size+=int64(len(vv))
				   }else {
				   	   if kt > rd.endTime {
                           rd.endTime=kt
					   }
				   	   if kt < rd.startTime {
				   	   	   rd.startTime=kt
					   }
					   v.Kv[kt]=vv
					  // c.size=kt+int64(len(vv))
					   c.count+=1
				   }
			   }
			}
		 }
	  }
}

func NewtsCacheData(ttl int,tss *tss) *tsCacheData {
	tsCache:=&tsCacheData {
		mutex:   sync.RWMutex{},
		cache:   make(map[string]*rData,0),
		size:    0,
		maxSize: 0,
		count:0,
		ttl: time.Duration(ttl) * time.Second,
	    tss: tss}
	if ttl > 0 {
		go tsCache.expire()
	}
	return tsCache

}

func (c *tsCacheData) expire() {
   for {
   	  time.Sleep(c.ttl)
   	  c.mutex.RLock()
   	  if c.cache != nil {
		  for tk,rd:=range c.cache {
			  if rd.created.Add(c.ttl).Before(time.Now()) {
			  	  c.mutex.Lock()
                  delete(c.cache,tk)
			  	  c.mutex.Unlock()
			  }
		  }
	  }
      c.mutex.RUnlock()
   }
}




























