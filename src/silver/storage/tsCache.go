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
	startTime,endTime int64,value map[int64][]byte) {
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
					   c.size-=int64(len(v.Kv[kt]))
					   v.Kv[kt]=vv
					   c.size+=int64(len(vv))
				   }else {
				   	   if kt > rd.endTime {
                           rd.endTime=kt
					   }
				   	   if kt < rd.startTime {
				   	   	   rd.startTime=kt
					   }
					   v.Kv[kt]=vv
					   c.size=kt+int64(len(vv))
					   c.count+=1
				   }
			   }
			}
		 }
	  }
}

/*
func  (c *tsCacheData) readTsCache(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) map[int64][]byte {
	uniqueKey:=dataBase+tableName+tagKv+fieldKey
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	rd,ok:=c.cache[tableName]
	if !ok {
		bv:=c.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
        sv:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
        kv:=mergeMap(bv,sv)
        c.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
        return kv
	}
	v,ok:=rd.rPoint[uniqueKey]
	if !ok {
		bv:=c.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		sv:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		kv:=mergeMap(bv,sv)
		c.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
		return kv
	}
	if startTime >= rd.startTime && endTime <= rd.endTime {
		return v.Kv
	}
	if startTime < rd.startTime && endTime >= rd.startTime && endTime < rd.endTime {
		cv:=c.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,rd.startTime,endTime)
		bv:=c.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		sv:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,rd.startTime)
		tempKv:=mergeMap(sv,cv)
		kv:=mergeMap(bv,tempKv)
		c.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
		return kv
	}
	if startTime >= rd.startTime && startTime <=rd.endTime && endTime > rd.endTime {
		cv:=c.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,rd.endTime)
		bv:=c.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		sv:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,rd.endTime,endTime)
		tempKv:=mergeMap(sv,cv)
		kv:=mergeMap(bv,tempKv)
		c.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
		return kv
	}
	if startTime < rd.startTime && endTime > rd.endTime {
		cv:=c.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,rd.startTime,rd.endTime)
		bv:=c.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		sv1:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,rd.startTime)
		sv2:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,rd.endTime,endTime)
		tempKv1:=mergeMap(sv1,sv2)
		tempKv2:=mergeMap(tempKv1,cv)
		kv:=mergeMap(bv,tempKv2)
		c.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
		return kv
	}
	bv:=c.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
	sv:=c.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
	kv:=mergeMap(bv,sv)
	c.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
	return kv
}  */

func mergeMap (bv map[int64][]byte,sv map[int64][]byte) map[int64][]byte {
	kv:=make(map[int64][]byte,0)
	for k,v:=range sv {
		kv[k]=v
	}
	for k,v:=range bv {
		kv[k]=v
	}
	return kv
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
			  c.mutex.RUnlock()
			  if rd.created.Add(c.ttl).Before(time.Now()) {
                  delete(c.cache,tk)
			  }
			  c.mutex.RLock()
		  }
	  }
      c.mutex.RUnlock()
   }
}




























