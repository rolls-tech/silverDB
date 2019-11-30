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
	db := t.openDB(tableFile)
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
		/*err = table.Put([]byte(key+"time"),[]byte(startTime))
		if err != nil {
			return err
		} */
		t.Addstat(key, value)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (t *tss) GetTimeRangeData(dataBase, tableName, rowKey, key string,startTime,endTime int64) ([]byte, *bolt.DB, error) {
	st:= strconv.FormatInt(startTime,10)
	tableFile:= t.getTableFile(dataBase, tableName,st)
	if tableFile != "" {
		db := t.openDB(tableFile)
		var value []byte
		if err := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(rowKey+key))
			c:=b.Cursor()
			for k,v:=c.First();k!=nil;k,v=c.Next() {
				st:=intToByte(startTime)
				et:=intToByte(endTime)
				if bytes.Compare(k,st)>=0 && bytes.Compare(k,et)<=0 {
				      value=v
				}
			}
			return nil
		}); err != nil {
			log.Println(err)
		}
		return value, db, nil
	}
	return nil,nil,nil
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

func (t *tss) openDB(dataFile string) *bolt.DB {
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

func (t *tss) getTableFile(dataBase,tableName string,startTime string) string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var tableFile string
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist == true {
		fileList,err:=ioutil.ReadDir(dataBaseDir)
		if err !=nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			return ""
		}
		for _,file:=range fileList {
			tableInfo:=strings.Split(file.Name(),"-")
			tn:=tableInfo[0]
			st:=tableInfo[1]
			et:=tableInfo[2]
			if strings.Compare(tn,tableName)==0 && strings.Compare(startTime,st) > 0 && strings.Compare(startTime,et) <0 {
				tableFile=dataBaseDir+sep+file.Name()
				return tableFile
			}
		}
	}
	return tableFile
}

func (t *tss) scanDataDir(dataBase,tableName,startTime,endTime string) string {
	var tableFile string
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist==true {
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
			et:=tableInfo[2]
			log.Println(startTime)
			log.Println(endTime)
			if strings.Compare(tn,tableName)==0 && strings.Compare(startTime,st) > 0 && strings.Compare(startTime,et) <0 {
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

