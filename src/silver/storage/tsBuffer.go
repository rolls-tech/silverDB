package storage

import (
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
	snapshot *tsBufferData
	snapshotSize uint64
	snapshotting bool
	lastSnapshot time.Time
	ttl time.Duration
	tss *tss
}


func (b *tsBufferData) writeData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64]float64,walCh chan bool) {
	for {
		ok:= <- walCh
		if ok {
			go b.writeTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,value)
		}
	}
}


func (b *tsBufferData) writeTsBuffer(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64]float64) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	seriesKey:=dataBase+tableName+tagKv
	tsValue:= &Value{
		Kv:value,
	}
	if value != nil {
		for _,_=range value {
			b.count+=1
		}
	}
	/*serializeValue,e:=proto.Marshal(tsValue)
	if e != nil {
		log.Println("serialize value failed",e)
	}
	valueSize:=int64(len(serializeValue))
	*/
	_,ok:=b.buffer[seriesKey]
	if !ok {
	  // skSize:=int64(len(seriesKey))
	   fieldKeyValue:=make(map[string]*Value,0)
	   fieldKeyValue[fieldKey]=tsValue
	   wp:=&WPoint{
		   DataBase:             dataBase,
		   TableName:            tableName,
		   Tags:                 tags,
		   Value:                fieldKeyValue,
	   }
	   wd:=&wData {
		   wp:      wp,
		   created: time.Now(),
	   }
	   /*
	   serializeWp,e:=proto.Marshal(wp)
	   serializeT,e:=wd.created.MarshalBinary()
	   if e !=nil {
	   	  log.Println("serialize wPoint or wData failed",e)
	   	  return e
	   }
	   wpSize:=int64(len(serializeWp))
	   wdSize:=wpSize+int64(len(serializeT))
	   b.size=wdSize+skSize+valueSize
	   */
		b.buffer[seriesKey]=wd
	}else {
		v,ok:=b.buffer[seriesKey].wp.Value[fieldKey]
		b.buffer[seriesKey].created=time.Now()
		if !ok {
			b.buffer[seriesKey].wp.Value[fieldKey]=tsValue
			//b.size+=valueSize
		} else {
			if value != nil {
				for kt,vv:=range value {
					//_,exist:=v.Kv[kt]
					//if exist {
						//b.size-=int64(len(v.Kv[kt]))
						v.Kv[kt]=vv
						//b.size+=int64(len(vv))
					//}else {
						//v.Kv[kt]=vv
						//b.size=kt+int64(len(vv))
						//b.count+=1
					//}
				}
			}
		}
	}
	return nil
}

func (b *tsBufferData) readTsBuffer(dataBase,tableName,tagKv,fieldKey string,tags map[string]string,startTime,endTime int64) map[int64]float64{
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	seriesKey:=dataBase+tableName+tagKv
	readTsBufferKv:=make(map[int64]float64,0)
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


func (b *tsBufferData) readTsSnapshot(dataBase,tableName,tagKv,fieldKey string,tags map[string]string,startTime,endTime int64) map[int64]float64{
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	seriesKey:=dataBase+tableName+tagKv
	readTsBufferKv:=make(map[int64]float64,0)
	wd,ok:=b.snapshot.buffer[seriesKey]
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


func NewTsBufferData(ttl int,tss *tss,flushCount int64) *tsBufferData {
	tsBuffer:=&tsBufferData {
		mutex:   sync.RWMutex{},
		buffer:   make(map[string]*wData,0),
		size:    0,
		maxSize: 0,
		count:0,
		snapshotting:false,
		ttl: time.Duration(ttl) * time.Second,
		tss: tss}
	if ttl > 0 {
	 //	go tsBuffer.expire()
	}
	if flushCount > 0 {
		go tsBuffer.flush(flushCount)
	}
	return tsBuffer
}


func (b *tsBufferData) flush(flushCount int64) {
	for {
		time.Sleep(b.ttl)
		if b.count >= flushCount && b.snapshotting == false {
			b.snapshot=b
			b.snapshotting=true
			b.lastSnapshot=time.Now()
			for _,wd:=range b.snapshot.buffer {
				b.snapshot.tss.writeTsData(wd.wp)
			}
			b.buffer=make(map[string]*wData,0)
			b.count=0
			b.snapshotting=false
		}
		if b.buffer != nil {
			b.snapshot=b
			b.snapshotting=true
			b.lastSnapshot=time.Now()
			for seriesKey,wd:=range b.snapshot.buffer {
				if wd.created.Add(b.ttl).Before(time.Now()) {
					b.snapshot.tss.writeTsData(wd.wp)
					b.mutex.Lock()
					delete(b.buffer,seriesKey)
					b.mutex.Unlock()
				}
			}
			b.snapshotting=false
		}

	}
}


func (b *tsBufferData) expire() {
	for {
		time.Sleep(b.ttl)
		if b.buffer != nil {
			//b.mutex.RLock()
			for seriesKey,wd:=range b.buffer {
				if wd.created.Add(b.ttl).Before(time.Now()) {
					b.tss.writeTsData(wd.wp)
				    b.mutex.Lock()
					delete(b.buffer,seriesKey)
					b.mutex.Unlock()
				}
			}
		    //b.mutex.RUnlock()
		}
	}
}

