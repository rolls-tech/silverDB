package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"silver/result"
	"strconv"
	"strings"
	"sync"
	"time"
)

const RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
const sep=string(os.PathSeparator)

type tss struct {
	mutex     sync.RWMutex
	dataDir   []string
	dataInfo  []*dataBase
	Stat
}

func (t *tss) SetKv(key string, value []byte) error {
	return nil
}

func (t *tss) GetKv(key string) ([]byte, error) {
	return nil,nil
}

func (t *tss) DelKv(key string) error {
	return nil
}

func (t *tss) SetDBandKV(dataBase,table,key string,value []byte) error {
	return nil
}

func (t *tss) GetDBandKV(dataBase,table,key string) ([]byte, *bolt.DB, error) {
	return nil,nil,nil
}

func (t *tss) DelDBandKV(dataBase,table,key string) (*bolt.DB, error) {
	return nil,nil
}

func (t *tss) SetTSData(dataBase, tableName, rowKey ,key string, value []byte,dataTime int64) error {
	tableFile,startTime:=t.setTableFile(dataBase,tableName,dataTime)
	db := t.OpenDB(tableFile)
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		table, err := tx.CreateBucketIfNotExists([]byte(rowKey+key))
		if err != nil {
			return err
		}
		err = table.Put([]byte(startTime), value)
		if err != nil {
			return err
		}
		t.Addstat(key, value)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (t *tss) GetTimeRangeData(db *bolt.DB, rowKey, key string,startTime,endTime int64) []*result.TsField {
	st := strconv.FormatInt(startTime, 10)
	et := strconv.FormatInt(endTime, 10)
	data:=make([]*result.TsField,0)
	if err:= db.View(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte(rowKey + key))
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						if strings.Compare(string(k), st) >= 0 && strings.Compare(string(k), et) <= 0 {
							tsf := &result.TsField{
								Timestamp:            k,
								Value:                v,
								XXX_NoUnkeyedLiteral: struct{}{},
								XXX_unrecognized:     nil,
								XXX_sizecache:        0,
							}
							data = append(data, tsf)
						}
					}
					return nil
				}); err != nil {
					log.Println(err)
				}
	         return data
}

func (t *tss) DelTSData(dataBase, tableName, rowKey, key string,startTime,endTime int64) (*bolt.DB, error) {
	return nil,nil
}

type dataBase struct {
	dataBaseName string
	dataBaseId string
	tableList []*table
}

type table struct {
	tableName string
	tableId string
	maxKey string
	minKey string
	maxTime int64
	minTime int64
	duration time.Duration
	tableFile string
}

func (t *tss) GetStat() Stat {
	return t.Stat
}

func NewTss(dataDir []string) *tss {
	return &tss{
		mutex:         sync.RWMutex{},
		dataDir:       dataDir,
		dataInfo:      make([]*dataBase,0),
		Stat:          Stat{},
	}
}

func (t *tss) OpenDB(dataFile string) *bolt.DB {
	db, err := bolt.Open(dataFile, 777, nil)
	if err != nil {
		log.Println(err.Error())
	}
	return db
}

func (t *tss) setTableFile(dataBase,tableName string,dataTime int64) (string,string) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	nowTime:=time.Now()
	var tableFile string
	var startTime string
	var endTime string
	if dataTime == 0 {
		startTime,endTime=getSandETime(nowTime,24*time.Hour)
		tableFile=t.scanDataDir(dataBase,tableName,startTime,endTime)
		return tableFile,startTime
	}
	startTime,endTime=getSandETime(time.Unix(0,dataTime),24*time.Hour)
	tableFile=t.scanDataDir(dataBase,tableName,startTime,endTime)
	return tableFile,startTime
}

func (t *tss) GetStorageFile(dataBase,tableName string,startTime,endTime int64) []string {
	sTime := strconv.FormatInt(startTime, 10)
	eTime := strconv.FormatInt(endTime, 10)
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
				if strings.Compare(sTime,st) >= 0 && strings.Compare(eTime,et) <= 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(sTime,st) < 0 && strings.Compare(eTime,st) >0 && strings.Compare(eTime,et) < 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(sTime,st) > 0 && strings.Compare(eTime,et) >0 && strings.Compare(sTime,et) <0{
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
			}
		}
	}
	return tableFileList
}

func (t *tss) scanDataDir(dataBase,tableName,startTime,endTime string) string {
	var tableFile string
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
			if strings.Compare(tn,tableName)==0 && strings.Compare(startTime,st) == 1 && strings.Compare(startTime,et)==-1 {
				tableFile=dataBaseDir+sep+file.Name()
				return tableFile
			}
		 }
			tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+endTime+".db"
			return tableFile
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
	err :=binary.Write(&buffer,binary.LittleEndian,num)
	if err !=nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func transTime (originTime int64) string {
	timePrecision:=len(intToByte(originTime))
	switch timePrecision {
	case 10:
		tt:=time.Unix(0,originTime*1e9).Format(RFC3339Nano)
		return tt
	case 13:
		tt:=time.Unix(0,originTime*1e6).Format(RFC3339Nano)
		return tt
	case 19:
		tt:=time.Unix(0,originTime).Format(RFC3339Nano)
		return tt
	default:
		log.Println("Time format not supported")
		return ""
	}
}

func getSandETime (nowTime time.Time,durationTime time.Duration) (string,string) {
	startTime:=nowTime.UnixNano()
	endTime:=startTime+durationTime.Nanoseconds()
	sstartTime := strconv.FormatInt(startTime,10)
	eendTime:=strconv.FormatInt(endTime,10)
	return sstartTime,eendTime
}

