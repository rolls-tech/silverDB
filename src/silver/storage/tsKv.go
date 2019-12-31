package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
const sep=string(os.PathSeparator)

type tsKv struct {
	Cache *tsCacheData
	Buffer *tsBufferData
	IsIndexed bool
	IsCompressed bool
}

func (k *tsKv) ReadTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) (map[int64][]byte,[]*bolt.DB) {
	kv,dbList:=k.readTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
	return kv,dbList
}


func (k *tsKv) WriteTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64][]byte) error {
		e:=k.Buffer.writeTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,value)
	return e
}

type tss struct {
	mutex     sync.RWMutex
	dataDir   []string
}

func NewTsKv(ttl int,tss *tss) *tsKv{
	tsKv:=&tsKv{
		Cache:  NewtsCacheData(ttl,tss),
		Buffer: NewTsBufferData(ttl,tss),
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

func (t *tss) writeTsData(wp  *WPoint) {
	var tagKv string
	dataBase:=wp.DataBase
	tableName:=wp.TableName
	if wp.Tags !=nil {
		for tagK,tagV:=range wp.Tags {
			tagKv+=tagK+tagV
		}
	}
	if wp.Value != nil {
	    for fieldKey,value:=range wp.Value {
			tableFileKv:=t.setTableFile(dataBase,tableName,value.Kv)
			if tableFileKv != nil {
				for tableFile,Kv:=range tableFileKv {
					go func() {
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
									err = table.Put(intToByte(k),v)
									if err != nil {
										return err
									}
								}
							}
							return nil
						}); err != nil {
							log.Println(err)
						}
					}()
				}
			}
		}
	}
}

func (t *tss) setTableFile(dataBase,tableName string,Kv map[int64][]byte) map[string]map[int64][]byte {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var tableFile string
	var endTime int64
	tableFileKv:=make(map[string]map[int64][]byte,0)
	kv:=make(map[int64][]byte)
	if Kv != nil {
		for k,v:=range Kv {
			endTime=getEndTime(k,24*time.Hour)
			tableFile=t.scanDataDir(dataBase,tableName,k,endTime)
			value,exist := tableFileKv[tableFile]
			if exist {
                 value[k]=v
			} else {
				 kv[k]=v
				 tableFileKv[tableFile]=kv
			}
		}
	}
	return tableFileKv
}

func (t *tss) scanDataDir(dataBase,tableName string,st,et int64) string {
	var tableFile string
	startTime := strconv.FormatInt(st,10)
	endTime := strconv.FormatInt(et,10)
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist == true {
		fileList,err:=ioutil.ReadDir(dataBaseDir)
		if err !=nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+endTime+".db"
			return tableFile
		}
		for _,file:=range fileList {
			tableInfo:=strings.Split(file.Name(),"-")
			tn:=tableInfo[0]
			st:=tableInfo[1]
			et:=strings.Split(tableInfo[2],".")[0]
			if strings.Compare(tn,tableName)==0 && strings.Compare(startTime,st) >=0  && strings.Compare(startTime,et) <=0  {
				tableFile=dataBaseDir+sep+file.Name()
				return tableFile
			}
			tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+endTime+".db"
			return tableFile
		}
	}
	rand.Seed(time.Now().Unix())
	n := rand.Intn(len(t.dataDir))
	err:=os.MkdirAll(t.dataDir[n]+dataBase,os.ModePerm)
	if err !=nil {
		log.Println(err)
	}
	tableFile=t.dataDir[n]+dataBase+sep+tableName+"-"+startTime+"-"+endTime+".db"
	return tableFile
}

func (t *tss) dataBaseDirIsExist(dataBase string) (string,bool) {
	for _,dir:= range t.dataDir {
		if t.fileIsExist(dir+dataBase) {
			return dir+dataBase,true
		}
	}
	return "",false
}

func (t *tss) fileIsExist(file string) bool {
	_, err := os.Stat(file)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func intToByte(num int64) []byte{
	var buffer bytes.Buffer
	err :=binary.Write(&buffer,binary.BigEndian,num)
	if err !=nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func getEndTime (dataTime int64,durationTime time.Duration) int64 {
	endTime:=dataTime+durationTime.Nanoseconds()
	return endTime
}

func (t *tss) openDB(dataFile string) *bolt.DB {
	db, err := bolt.Open(dataFile, 777, nil)
	if err != nil {
		log.Println(err.Error())
	}
	return db
}

func (t *tss) readTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) (map[int64][]byte,[]*bolt.DB) {
	sTime := strconv.FormatInt(startTime, 10)
	eTime := strconv.FormatInt(endTime, 10)
    tableFileList:=t.getTableFile(dataBase,tableName,sTime,eTime)
    tsValue:=make(map[int64][]byte)
	var wg sync.WaitGroup
	var dbList []*bolt.DB
    if len(tableFileList) !=0 {
    	for i,_:=range tableFileList {
    		tableFile:=tableFileList[i]
			wg.Add(1)
    		go func(wg *sync.WaitGroup,dbList []*bolt.DB,tsValue map[int64][]byte) {
				db:=t.openDB(tableFile)
				dbList=append(dbList,db)
				if err:= db.View(func(tx *bolt.Tx) error {
					rootTable := tx.Bucket([]byte(tagKv))
					b:=rootTable.Bucket([]byte(fieldKey))
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						t:=int64(binary.BigEndian.Uint64(k))
						if t >= startTime && t <= endTime {
							tsValue[t]=v
						}
					}
					return nil
				}); err != nil {
					log.Println(err)
				}
			wg.Done()
			}(&wg,dbList,tsValue)
		}
    	wg.Wait()
	}
	return tsValue,dbList
}

func (t *tss) getTableFile(dataBase,tableName,startTime,endTime string) []string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var tableFile string
	var tableFileList []string
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
			tableInfo:=strings.Split(file.Name(),"-")
			tn:=tableInfo[0]
			st:=tableInfo[1]
			et:=strings.Split(tableInfo[2],".")[0]
			if strings.Compare(tn,tableName) == 0 {
				if strings.Compare(startTime,st) >= 0 && strings.Compare(startTime,et) <=0 && strings.Compare(endTime,et) <= 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(startTime,st) <= 0 && strings.Compare(endTime,st) >=0 && strings.Compare(endTime,et) <= 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(startTime,st) >= 0 &&  strings.Compare(startTime,et) <= 0 && strings.Compare(endTime,et) >= 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,et) > 0 {
					tableFileList1:=t.getTableFile(dataBase,tableName,startTime,st)
					tableFileList2:=t.getTableFile(dataBase,tableName,et,endTime)
					tableFileList=append(tableFileList,tableFileList1...)
					tableFileList=append(tableFileList,tableFileList2...)
				}
			}
		}
	}
	return tableFileList
}

func  (k *tsKv) readTsCache(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,startTime,endTime int64) (map[int64][]byte,[]*bolt.DB) {
	uniqueKey:=dataBase+tableName+tagKv+fieldKey
	rd,ok:=k.Cache.cache[tableName]
	if !ok {
		bv:=k.Buffer.readTsBuffer(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		sv,dbList:=k.Cache.tss.readTsData(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime)
		kv:=mergeMap(bv,sv)
		k.Cache.writeTsCache(dataBase,tableName,tagKv,fieldKey,tags,startTime,endTime,kv)
		return kv,dbList
	}
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
