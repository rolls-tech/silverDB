package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"silver/compress"
	"silver/metadata"
	"silver/util"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)


const sep=string(os.PathSeparator)

type tsKv struct {
	*tsWal
	*metadata.Meta
	IsIndexed bool
	IsCompressed bool
}

func (k *tsKv) ReadTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) map[int64]float64 {
	kv:=k.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
	return kv
}


func (k *tsKv) WriteTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64]float64) error {
		e:=k.writeTsData(dataBase,tableName,tagKv,fieldKey,tags,value)
	    if e !=nil {
		   log.Println(e)
		   return e
	    }
		_,ok:=k.MetaData[dataBase+tableName]
		if !ok {
			e=k.PutNode("/silver/metaData/"+dataBase+"/"+tableName+"/"+k.NodeAddr,"")
		}
	return e
}

type tss struct {
	mutex     sync.RWMutex
	dataDir   []string
	isCompressed bool
}

type points struct {
	maxTime int64
	minTime int64
	c compress.Chunk
	fieldKey string
	count int
	tableFile string
}

func NewPoints(maxTime,minTime int64,c compress.Chunk,fieldKey,tableFile string,count int) *points {
	return &points{
		maxTime:  maxTime,
		minTime:  minTime,
		c:        c,
		fieldKey: fieldKey,
		count: count,
		tableFile: tableFile,
	}
}

func NewTsKv(ttl int,walDir string,indexDir []string,tss *tss,meta *metadata.Meta,isIndexed,isCompressed bool,flushCount int64) *tsKv {
	tsKv:=&tsKv {
		tsWal:NewTsWal(ttl,walDir,indexDir,tss,flushCount),
		Meta:meta,
		IsIndexed: isIndexed,
		IsCompressed:isCompressed,
	}
	if tsKv.IsIndexed == true {
		tsKv.tsIndex=NewTsIndex(ttl,tss.dataDir)
	}
	if tsKv.IsCompressed == true {
		tsKv.tss.isCompressed= true
	}
	return tsKv
}

func NewTss(dataDir []string ) *tss {
	tss:=&tss{
		mutex:   sync.RWMutex{},
		dataDir: dataDir,
	}
	return tss
}

func (t *tss) tsCompress(wp *WPoint) []*points {
	var chunkList []*points
	var wg sync.WaitGroup
	if wp.Value !=nil {
		for fieldKey,value:=range wp.Value {
			wg.Add(1)
			go func(chunkList *[]*points) {
				cl:=t.deltaCompress(wp.DataBase,wp.TableName,value.Kv,fieldKey)
				*chunkList=append(*chunkList,cl...)
				defer wg.Done()
			}(&chunkList)
		}
		wg.Wait()
	}
	return chunkList
}

func (t *tss) deltaCompress(dataBase,tableName string,kv map[int64]float64,fieldKey string) []*points {
	tableFileKv:=make(map[string]map[int64]float64,0)
	pointList:=make([]*points,0)
	tableFileKv=t.setTableFile(dataBase,tableName,kv,tableFileKv)
	if tableFileKv !=nil {
		for tableFile,kv:=range tableFileKv {
			c := compress.NewXORChunk()
			app, err := c.Appender()
			if err != nil {
				log.Println(err)
			}
			if kv != nil {
				sm := util.NewSortMap(kv)
				sort.Sort(sm)
				count:=sm.Len()
				for _, p := range sm {
					app.Append(p.T, p.V)
				}
				p:=NewPoints(sm[count-1].T,sm[0].T,c,fieldKey,tableFile,count)
				pointList=append(pointList,p)
			}
		}
	}
	return pointList
}

func (t *tss) writeTsData (wp  *WPoint) {
	var tagKv string
	if wp.Tags !=nil {
		for tagK,tagV:=range wp.Tags {
			tagKv+=tagK+tagV
		}
	}
	switch t.isCompressed {
	case true:
		// 压缩
		chunkList:=t.tsCompress(wp)
		for _,c:=range chunkList {
			go func(c *points) {
				kv:=make(map[int64][]byte)
				vv:=make([]byte,0)
				sMt,_:=time.Unix(c.maxTime,0).MarshalBinary()
				vv=append(vv,sMt...)
				vv=append(vv,c.c.Bytes()...)
				kv[c.minTime]=vv
				t.setKv(c.tableFile,tagKv,c.fieldKey,kv)
			}(c)
		}
	case false:
		// 非压缩
		dataBase:=wp.DataBase
		tableName:=wp.TableName
		if wp.Value != nil {
			for fieldKey,value:=range wp.Value {
				go func(value *Value) {
					tableFileKv:=make(map[string]map[int64]float64,0)
					tableFileKv=t.setTableFile(dataBase,tableName,value.Kv,tableFileKv)
					if tableFileKv != nil {
						for tableFile,Kv:=range tableFileKv {
							go func(Kv *map[int64]float64 ) {
								value:=make(map[int64][]byte,0)
								for tt,vv:=range *Kv {
									value[tt]=util.Float64ToByte(vv)
								}
								t.setKv(tableFile,tagKv,fieldKey,value)
							}(&Kv)
						}
					}
				}(value)
			}
		}
	}
}

func (t *tss) setKv(tableFile,tagKv,fieldKey string,Kv map[int64][]byte) {
		db := t.openDB(tableFile)
		defer db.Close()
		if err := db.Batch(func(tx *bolt.Tx) error {
			rootTable, err := tx.CreateBucketIfNotExists([]byte(tagKv))
			if err != nil {
				return err
			}
			table, err := rootTable.CreateBucketIfNotExists([]byte(fieldKey))
			if err != nil {
				return err
			}
			if Kv != nil {
				for k,v:=range Kv {
					err = table.Put(util.IntToByte(k),v)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			log.Println(err)
		}
}

func (t *tss) setTableFile(dataBase,tableName string,Kv map[int64]float64,
	tableFileKv map[string]map[int64]float64) map[string]map[int64]float64 {
	var tableFile string
	if Kv != nil {
		for k,v:=range Kv {
			tableFile=t.scanDataDir(dataBase,tableName,k)
			if ! util.CheckFileIsExist(tableFile) {
				db:=t.openDB(tableFile)
				db.Close()
			}
			value,exist := tableFileKv[tableFile]
			if exist {
                 value[k]=v
			} else {
				kv:=make(map[int64]float64)
				kv[k]=v
				tableFileKv[tableFile]=kv
			}
		}
	}
	return tableFileKv
}

func (t *tss) scanDataDir(dataBase,tableName string,dt int64) string {
	var tableFile string
	et:=getEndTime(dt,24*time.Hour)
	dataTime := strconv.FormatInt(dt,10)
	endTime := strconv.FormatInt(et,10)
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist == true {
		fileList,err:=ioutil.ReadDir(dataBaseDir)
		if err !=nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			tableFile=dataBaseDir+sep+tableName+"-"+dataTime+"-"+endTime+".db"
			return tableFile
		}

		for _,file:=range fileList {
			tableInfo:=strings.Split(file.Name(),"-")
			tn:=tableInfo[0]
			st:=tableInfo[1]
			et:=strings.Split(tableInfo[2],".")[0]
			if strings.Compare(tn,tableName) == 0 {
				if strings.Compare(dataTime,st) >= 0  && strings.Compare(dataTime,et) < 0 {
					tableFile=dataBaseDir+sep+file.Name()
					return tableFile
				}
				if strings.Compare(dataTime,st) < 0 {
					newDt,_:= strconv.ParseInt(st, 10, 64)
					newSt:=getStartTime(newDt,24*time.Hour)
					newStartTime:= strconv.FormatInt(newSt,10)
					tableFile=dataBaseDir+sep+tableName+"-"+newStartTime+"-"+st+".db"
					return tableFile
				}
				if strings.Compare(dataTime,et) >= 0 {
					newDt,_:= strconv.ParseInt(et, 10, 64)
					newEt:=getEndTime(newDt,24*time.Hour)
					newEndTime:= strconv.FormatInt(newEt,10)
					tableFile=dataBaseDir+sep+tableName+"-"+et+"-"+newEndTime+".db"
					return tableFile
				}
			} else {
				tableFile=dataBaseDir+sep+tableName+"-"+dataTime+"-"+endTime+".db"
				return tableFile
			}
		}
	}
	rand.Seed(time.Now().Unix())
	n := rand.Intn(len(t.dataDir))
	err:=os.MkdirAll(t.dataDir[n]+dataBase,os.ModePerm)
	if err !=nil {
		log.Println(err)
	}
	tableFile=t.dataDir[n]+dataBase+sep+tableName+"-"+dataTime+"-"+endTime+".db"
	return tableFile
}

func (t *tss) dataBaseDirIsExist(dataBase string) (string,bool) {
	for _,dir:= range t.dataDir {
		if util.CheckFileIsExist(dir+dataBase) {
			return dir+dataBase,true
		}
	}
	return "",false
}

func getEndTime (dataTime int64,durationTime time.Duration) int64 {
	endTime:=dataTime+durationTime.Nanoseconds()
	return endTime
}

func getStartTime(dataTime int64,durationTime time.Duration) int64 {
	startTime:=dataTime- durationTime.Nanoseconds()
	return startTime
}

func (t *tss) openDB(dataFile string) *bolt.DB {
	db, err := bolt.Open (dataFile, 777, nil)
	if err != nil {
		log.Println(err.Error())
	}
	return db
}

func (t *tss) deCompress(v []byte,value *Value,startTime int64,endTime int64){
	c:=compress.NewBXORChunk(v)
	it:=c.Iterator(nil)
	for it.Next() {
		tt,vv:=it.At()
		if tt >= startTime && tt <= endTime {
			value.Kv[tt]=vv
		}
	}
}

func(t *tss) getMaxTime(v []byte) []byte {
	maxTime:=v[:15]
	return maxTime
}

func(t *tss) scanCompressKv(tableFile,tagKv,fieldKey string, value *Value,startTime,endTime int64) *bolt.DB {
	db:=t.openDB(tableFile)
	if err:= db.View(func(tx *bolt.Tx) error {
		rootTable := tx.Bucket([]byte(tagKv))
		if rootTable !=nil {
			c:=rootTable.Cursor()
			k,v:=c.Seek([]byte(fieldKey))
			if k != nil && v == nil {
				subB:=rootTable.Bucket(k)
				_ = subB.ForEach(func(k, v []byte) error {
					minTime := k
					if bytes.Compare(minTime,util.IntToByte(startTime)) >= 0 {
						t.deCompress(v[15:],value,startTime, endTime)
					}
					return nil
				})
			}
		}
		return nil
	}); err != nil {
		log.Println(err)
	}
	return db
}

func (t *tss) readTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) [][]byte {
	sTime := strconv.FormatInt(startTime, 10)
	eTime := strconv.FormatInt(endTime, 10)
	tableFileList:=t.getTableFile(dataBase,tableName,sTime,eTime)
	dataSet:=make([][]byte,0)
	var wg sync.WaitGroup
	if tableFileList != nil {
		for k,_:=range tableFileList {
			tableFile:=k
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				value:=&Value {
					Kv: make(map[int64]float64,0),
				}
				if t.isCompressed == true {
					db:=t.scanCompressKv(tableFile,tagKv,fieldKey,value,startTime,endTime)
					defer db.Close()
				}
				if t.isCompressed ==false {
					db:=t.scanKv(tableFile,tagKv,fieldKey,value,startTime,endTime)
					defer db.Close()
				}
				buf,e:=proto.Marshal(value)
				if e !=nil {
					log.Println(e.Error())
				}
				dst:=make([]byte,len(buf))
				copy(dst,buf)
				dataSet= append(dataSet, dst)
				wg.Done()
			}(&wg)
			wg.Wait()
		}
	}
	return dataSet
}

func (t *tss) scanKv(tableFile,tagKv,fieldKey string, value *Value,startTime,endTime int64) *bolt.DB {
	db:=t.openDB(tableFile)
	if err:= db.View(func(tx *bolt.Tx) error {
		rootTable := tx.Bucket([]byte(tagKv))
		if rootTable !=nil {
			c:=rootTable.Cursor()
			k,v:=c.Seek([]byte(fieldKey))
			if k != nil && v == nil {
				subB:=rootTable.Bucket(k)
				subC:=subB.Cursor()
				for key,vv:=subC.Seek(util.IntToByte(startTime)); key != nil && bytes.Compare(key,util.IntToByte(endTime)) <= 0; key,vv=subC.Next() {
					    value.Kv[int64(binary.BigEndian.Uint64(key))]=util.ByteToFloat64(vv)
				}
			}
		}
		return nil
	}); err != nil {
		log.Println(err)
	}
	return db
}


func (t *tss) getTableFile(dataBase,tableName,startTime,endTime string) map[string]bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var tableFile string
	tableFileList:=make(map[string]bool,0)
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist == true {
		fileList,err:=ioutil.ReadDir(dataBaseDir)
		if err !=nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			return nil
		}
		for _,file:=range fileList {
			if !strings.HasSuffix(file.Name(),"lock") {
				tableInfo:=strings.Split(file.Name(),"-")
				tn:=tableInfo[0]
				st:=tableInfo[1]
				et:=strings.Split(tableInfo[2],".")[0]
				if strings.Compare(tn,tableName) == 0 {
					if strings.Compare(startTime,st) >= 0 && strings.Compare(startTime,et) < 0 && strings.Compare(endTime,et) <= 0 {
						tableFile=dataBaseDir+sep+file.Name()
						tableFileList[tableFile]=true
					}
					if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,st) >0 && strings.Compare(endTime,et) < 0 {
						tableFileList1:=t.getTableFile(dataBase,tableName,startTime,st)
						tableFileList2:=t.getTableFile(dataBase,tableName,st,endTime)
						for tableFileK1,tableFileV1:=range tableFileList1 {
							tableFileList[tableFileK1]=tableFileV1
						}
						for tableFileK2,tableFileV2:=range tableFileList2 {
							tableFileList[tableFileK2]=tableFileV2
						}
					}
					if strings.Compare(startTime,st) >= 0 &&  strings.Compare(startTime,et) < 0 && strings.Compare(endTime,et) > 0 {
						tableFileList1:=t.getTableFile(dataBase,tableName,startTime,et)
						tableFileList2:=t.getTableFile(dataBase,tableName,et,endTime)
						for tableFileK1,tableFileV1:=range tableFileList1 {
							tableFileList[tableFileK1]=tableFileV1
						}
						for tableFileK2,tableFileV2:=range tableFileList2 {
							tableFileList[tableFileK2]=tableFileV2
						}
					}
					if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,et) >= 0 {
						tableFileList1:=t.getTableFile(dataBase,tableName,startTime,st)
						tableFileList2:=t.getTableFile(dataBase,tableName,et,endTime)
						tableFile=dataBaseDir+sep+file.Name()
						tableFileList[tableFile]=true
						for tableFileK1,tableFileV1:=range tableFileList1 {
							tableFileList[tableFileK1]=tableFileV1
						}
						for tableFileK2,tableFileV2:=range tableFileList2 {
							tableFileList[tableFileK2]=tableFileV2
						}
					}
				}
			}
		}
	}
	return tableFileList
}


func  (k *tsKv) readTsCache(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) map[int64]float64 {
	var value Value
	kv:=make(map[int64]float64)
	bv:=k.readTsSnapshot(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
	dataSet:=k.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
	if len(dataSet) != 0 {
		for _,data:=range dataSet {
			if len(data) !=0 {
				_=proto.Unmarshal(data,&value)
				kv=mergeMap(kv,value.Kv)
			}
		}
	}
	kv=mergeMap(bv,kv)
	//k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
	return kv
	//uniqueKey:=dataBase+tableName+tagKv+fieldKey
	//k.Cache.mutex.RLock()
	//rd,ok:=k.Cache.cache[tableName]
	//k.Cache.mutex.RUnlock()
	/*
	if !ok {
		bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			sv,dbList:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			kv:=mergeMap(bv,sv)
			//k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
			log.Println(k.Cache.cache)
			return kv,dbList
	} else {
		v,ok:=rd.rPoint[uniqueKey]
		if !ok {
			bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			sv,dbList:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			kv:=mergeMap(bv,sv)
			k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
			return kv,dbList
		}
		if startTime >= rd.startTime && endTime <= rd.endTime {
			return v.Kv,nil
		}
		if startTime < rd.startTime && endTime >= rd.startTime && endTime < rd.endTime {
			var dbList []*bolt.DB
			cv,dbList1:=k.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,rd.startTime,endTime)
			dbList=append(dbList,dbList1...)
			bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			sv,dbList2:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,rd.startTime)
			dbList=append(dbList,dbList2...)
			tempKv:=mergeMap(sv,cv)
			kv:=mergeMap(bv,tempKv)
			k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
			return kv,dbList
		}
		if startTime >= rd.startTime && startTime <=rd.endTime && endTime > rd.endTime {
			var dbList []*bolt.DB
			cv,dbList1:=k.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,rd.endTime)
			dbList=append(dbList,dbList1...)
			bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			sv,dbList2:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,rd.endTime,endTime)
			tempKv:=mergeMap(sv,cv)
			kv:=mergeMap(bv,tempKv)
			k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
			dbList=append(dbList,dbList2...)
			return kv,dbList
		}
		if startTime < rd.startTime && endTime > rd.endTime {
			var dbList []*bolt.DB
			cv,dbList1:=k.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,rd.startTime,rd.endTime)
			dbList=append(dbList,dbList1...)
			bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
			sv1,dbList2:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,rd.startTime)
			dbList=append(dbList,dbList2...)
			sv2,dbList3:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,rd.endTime,endTime)
			dbList=append(dbList,dbList3...)
			tempKv1:=mergeMap(sv1,sv2)
			tempKv2:=mergeMap(tempKv1,cv)
			kv:=mergeMap(bv,tempKv2)
			k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
			return kv,dbList
		}
		bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		sv,dbList:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		kv:=mergeMap(bv,sv)
		k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
		return kv,dbList
	}
	*/
}


func mergeMap (bv map[int64]float64,sv map[int64]float64) map[int64]float64 {
	kv:=make(map[int64]float64,0)
	for k,v:=range sv {
		kv[k]=v
	}
	for k,v:=range bv {
		kv[k]=v
	}
	return kv
}