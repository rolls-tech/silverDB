package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"silver/node/point"
	"silver/utils"
	"strconv"
	"strings"
	"time"
)

func openDB(dataFile string) *bolt.DB {
	db, err := bolt.Open (dataFile,os.ModePerm,nil)
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
				for key,vv:=subC.Seek(utils.Int64ToByte(startTime)); key != nil && bytes.Compare(key,utils.Int64ToByte(endTime)) <= 0; key,vv=subC.Next() {
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


/*func scanCompressKv(tableFile,tagKv,fieldKey string, value *point.Value,startTime,endTime int64) *bolt.DB {
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
					if bytes.Compare(minTime,utils.Int64ToByte(startTime)) >= 0 {
						deCompressChunk(v[15:],value,startTime, endTime)
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
}*/




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
				err = table.Put(utils.Int64ToByte(k),v)
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


func generateChunkData(chunk *compressPoints) ([]byte,[]byte) {
	minTime:=utils.Int64ToByte(chunk.minTime)
	maxTime:=utils.Int64ToByte(chunk.maxTime)
	timestamp:=utils.Int64ToByte(time.Now().UnixNano())
	count:=utils.IntToByte(chunk.count)
	precision:=utils.Int32ToByte(chunk.precision)
	metricType:=utils.Int32ToByte(chunk.metricType)
	bucketKey:=[]byte(fmt.Sprintf("%s%s%s%s,%s,%s,%s,%s",minTime,maxTime,timestamp,metricType,precision,chunk.minValue,chunk.maxValue,count))
	chunkValue:=[]byte(fmt.Sprintf("%s,%s",
		chunk.timeChunk,chunk.valueChunk))
	return bucketKey,chunkValue
}

func generateTimeChunkData(chunk *compressPoints) []byte {
	minTime:=utils.Int64ToByte(chunk.minTime)
	maxTime:=utils.Int64ToByte(chunk.maxTime)
	timestamp:=utils.Int64ToByte(time.Now().UnixNano())
	count:=utils.IntToByte(chunk.count)
	precision:=utils.Int32ToByte(chunk.precision)
	bucketKey:=[]byte(fmt.Sprintf("%s%s%s,%s,%s,%s",minTime,maxTime,timestamp,chunk.timeChunk,precision,count))
	//log.Println(len(bucketKey) / 1024, " kB" )
	return bucketKey
}

func generateValueChunkData(chunk *compressPoints) []byte {
	metricType:=utils.Int32ToByte(chunk.metricType)
	valueKey:=[]byte(fmt.Sprintf("%s%s,%s,%s",metricType,chunk.minValue,chunk.maxValue,chunk.valueChunk))
	//log.Println(len(valueKey) / 1024, " kB" )
	return valueKey
}



type chunkData struct {
	minTime []byte
    maxTime []byte
 	timestamp []byte
    count []byte
	chunk []byte
	maxValue []byte
	minValue []byte
	metricType int32
	precision int32
}

func newChunkData() *chunkData {
	return &chunkData{}
}

func resolverChunkData(keyByte,chunkByte []byte) *chunkData {
	chunkData:=newChunkData()
	if len(keyByte) >= 24 && len(chunkByte) > 0 {
		minTime:=keyByte[0:8]
		maxTime:=keyByte[8:16]
		timestamp:=keyByte[16:24]
		kb:=bytes.NewReader(keyByte[24:])
		readerKb:=bufio.NewReader(kb)
		suffix := make([]byte, len(keyByte[24:]))
		_,e:= io.ReadFull(readerKb,suffix)
		if e !=nil {
			if e != io.EOF {
				log.Println("resolver chunk key failed !",e)
			}
		}
		suffixList:=bytes.Split(suffix, []byte(","))
		if len(suffixList) == 5 {
			metricTypeByte:=suffixList[0]
			precisionByte:=suffixList[1]
			minValueByte:=suffixList[2]
			maxValueByte:=suffixList[3]
			countByte:=suffixList[4]
			chunkData.minValue=minValueByte
			chunkData.maxValue=maxValueByte
			chunkData.count=countByte
			chunkData.metricType=utils.ByteToInt32(metricTypeByte)
			chunkData.precision=utils.ByteToInt32(precisionByte)
		}
		chunkData.minTime=minTime
		chunkData.maxTime=maxTime
		chunkData.timestamp=timestamp
		rd:= bytes.NewReader(chunkByte)
		reader:=bufio.NewReader(rd)
		chunkLen,_:=reader.ReadString(',')
		chunkLen=strings.ReplaceAll(chunkLen,",","")
		cLen,e:= strconv.Atoi(chunkLen)
		if e != nil {
			log.Println(e)
		}
		chunk := make([]byte,cLen)
		_,e= io.ReadFull(reader,chunk)
		if e !=nil {
			log.Println("resolver chunk data failed !",e)
		}
			chunkData.chunk=chunk
		}
		return chunkData
	}


func writeKv(tableFile,tagKv string,chunkList []*compressPoints) error {
		db:=openDB(tableFile)
		defer db.Close()
		if len(chunkList) > 0 {
		//for _,chunk:=range chunkList {
			if err := db.Batch(func(tx *bolt.Tx) error {
			// 优化数据存储
			tagTable, err := tx.CreateBucketIfNotExists([]byte(tagKv))
			if err != nil {
				return err
			}
			timeData:=generateTimeChunkData(chunkList[0])
			bucket,err:=tagTable.CreateBucketIfNotExists(timeData)
			for i:=0; i < len(chunkList); i++ {
				valueData:=generateValueChunkData(chunkList[i])
				err=bucket.Put([]byte(chunkList[i].fieldKey),valueData)
				if err != nil {
					return err
				}
			}
			/*table, err := tx.CreateBucketIfNotExists([]byte(tagKv+chunk.fieldKey))
			key,value:=generateChunkData(chunk)
			err = table.Put(key,value)
			if err != nil {
			   return err
			}*/
			return nil
			}); err != nil {
				log.Println(err)
				return err
			}
			//}
		}
	return nil
}


func readKv(tableFile,tagKv,fieldKey string,startTime,endTime int64) (map[int]ChunkDataList,*bolt.DB) {
	db:=openDB(tableFile)
	chunkDataGroup:=make(map[int]ChunkDataList,0)
	chunkDataGroup[1]=make(ChunkDataList,0)
	chunkDataGroup[2]=make(ChunkDataList,0)
	chunkDataGroup[3]=make(ChunkDataList,0)
	chunkDataGroup[4]=make(ChunkDataList,0)
	if err:= db.View(func(tx *bolt.Tx) error {
		table := tx.Bucket([]byte(tagKv+fieldKey))
		if table !=nil {
			err:= table.ForEach(func(key, value []byte) error {
				if key !=nil && value !=nil {
					chunkData:=resolverChunkData(key,value)
					if bytes.Compare(utils.Int64ToByte(startTime),chunkData.minTime) >=0 &&
						bytes.Compare(utils.Int64ToByte(endTime),chunkData.maxTime) <= 0 {
						chunkDataGroup[1]=append(chunkDataGroup[1], chunkData)
					}
					if bytes.Compare(utils.Int64ToByte(startTime),chunkData.minTime) >= 0 &&
						bytes.Compare(utils.Int64ToByte(endTime),chunkData.maxTime) > 0 {
						chunkDataGroup[2]=append(chunkDataGroup[2], chunkData)
					}
					if bytes.Compare(utils.Int64ToByte(startTime),chunkData.minTime) <= 0 &&
						bytes.Compare(utils.Int64ToByte(endTime),chunkData.maxTime) < 0 {
						chunkDataGroup[3]=append(chunkDataGroup[3], chunkData)

					}
					if bytes.Compare(utils.Int64ToByte(startTime),chunkData.minTime) < 0 &&
						bytes.Compare(utils.Int64ToByte(endTime),chunkData.maxTime) > 0 {
						chunkDataGroup[4]=append(chunkDataGroup[4], chunkData)
					}
				}
				return nil
			})
			if err != nil {
				log.Println(err)
				return nil
			}
		}
		return nil
	}); err != nil {
		log.Println(err)
	}
	return chunkDataGroup,db
}