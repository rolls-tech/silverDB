package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"log"
	"silver/compress"
	"silver/node/point"
	"silver/utils"
	"time"
)

func openDB(dataFile string) *bolt.DB {
	db, err := bolt.Open (dataFile, 777, nil)
	if err != nil {
		log.Println(err.Error())
	}
	return db
}

func getEndTime (dataTime int64,durationTime time.Duration) int64 {
	endTime:=dataTime+durationTime.Nanoseconds()
	return endTime
}

func getStartTime(dataTime int64,durationTime time.Duration) int64 {
	startTime:=dataTime- durationTime.Nanoseconds()
	return startTime
}

func getMaxTime(v []byte) []byte {
	maxTime:=v[:15]
	return maxTime
}


func scanKv(tableFile,tagKv,fieldKey string, value *point.Value,startTime,endTime int64) *bolt.DB {
	db:=openDB(tableFile)
	if err:= db.View(func(tx *bolt.Tx) error {
		rootTable := tx.Bucket([]byte(tagKv))
		if rootTable !=nil {
			c:=rootTable.Cursor()
			k,v:=c.Seek([]byte(fieldKey))
			if k != nil && v == nil {
				subB:=rootTable.Bucket(k)
				subC:=subB.Cursor()
				for key,vv:=subC.Seek(utils.IntToByte(startTime)); key != nil && bytes.Compare(key,utils.IntToByte(endTime)) <= 0; key,vv=subC.Next() {
					value.Kv[int64(binary.BigEndian.Uint64(key))]=utils.ByteToFloat64(vv)
				}
			}
		}
		return nil
	}); err != nil {
		log.Println(err)
	}
	return db
}


func scanCompressKv(tableFile,tagKv,fieldKey string, value *point.Value,startTime,endTime int64) *bolt.DB {
	db:=openDB(tableFile)
	if err:= db.View(func(tx *bolt.Tx) error {
		rootTable := tx.Bucket([]byte(tagKv))
		if rootTable !=nil {
			c:=rootTable.Cursor()
			k,v:=c.Seek([]byte(fieldKey))
			if k != nil && v == nil {
				subB:=rootTable.Bucket(k)
				_ = subB.ForEach(func(k, v []byte) error {
					minTime := k
					if bytes.Compare(minTime,utils.IntToByte(startTime)) >= 0 {
						deCompress(v[15:],value,startTime, endTime)
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

func deCompress(v []byte,value *point.Value,startTime int64,endTime int64){
	c:=compress.NewBXORChunk(v)
	it:=c.Iterator(nil)
	for it.Next() {
		tt,vv:=it.At()
		if tt >= startTime && tt <= endTime {
			value.Kv[tt]=vv
		}
	}
}


func setKv(tableFile,tagKv,fieldKey string,Kv map[int64][]byte) error {
	db := openDB(tableFile)
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
				err = table.Put(utils.IntToByte(k),v)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		log.Println(err)
		return err
	}
	return nil
}