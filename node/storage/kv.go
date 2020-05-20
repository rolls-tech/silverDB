package storage

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"silver/compress"
	"silver/node/point"
	"silver/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const sep=string(os.PathSeparator)

type kv struct {
	mutex     sync.RWMutex
	dataDir   []string
	isCompressed bool
	duration time.Duration
	compressCount int
}

func NewKv(dataDir []string,isCompress bool,duration time.Duration,compressCount int) *kv {
	if len(dataDir) > 0 {
		for _,dir:=range dataDir {
			ok:=utils.CheckFileIsExist(dir)
			if !ok {
				e:=os.MkdirAll(dir,os.ModePerm)
				if e !=nil {
					log.Println("failed create data dir",dir,e)
				}
			}
		}
	}
	return &kv{
		mutex:        sync.RWMutex{},
		dataDir:      dataDir,
		isCompressed: isCompress,
		duration: duration,
		compressCount: compressCount,
	}
}



type compressPoints struct {
	maxTime int64
	minTime int64
	chunk compress.Chunk
	fieldKey string
	count int
}


func (s *kv) writeDataLinked(dn *dataNodeLinked) {
	go func() {
		nodeData:=make([]*metricData,0)
		current := dn.head
		for current != nil {
			if len(current.metrics) > 0 {
				nodeData=append(nodeData,current.metrics...)
			}
			current = current.next
		}
		s.writeDataKv(dn.dataBase,dn.tableName,dn.tagKv,nodeData)
	}()
}


type nodeMetricData struct {
	metricData map[string]*metricData
}

func newNodeMetricData() *nodeMetricData{
	return &nodeMetricData{
		metricData: make(map[string]*metricData,0),
	}
}


func (s *kv) writeDataKv(dataBase,table,tagKv string,nodeData []*metricData) {
	nodeMetricData:=newNodeMetricData()
	switch s.isCompressed {
	case true:
		if len(nodeData) > 0 {
			for _,data:=range nodeData {
				//合并fieldKey
				fieldData,ok:=nodeMetricData.metricData[data.metric]
			    if ok {
					fieldData.points=append(fieldData.points,data.points...)
					fieldData.count+=data.count
			    	if fieldData.minTime == 0 {
						fieldData.minTime=data.minTime
					}
			    	if fieldData.minTime >= data.minTime {
						fieldData.minTime=data.minTime
					}
			    	if fieldData.maxTime <= data.maxTime {
						fieldData.maxTime=data.maxTime
					}
				} else {
					nodeMetricData.metricData=make(map[string]*metricData,0)
					nodeMetricData.metricData[data.metric]=newMetricData(data.metric,data.points)
					nodeMetricData.metricData[data.metric].count+=data.count
					nodeMetricData.metricData[data.metric].minTime=data.minTime
					nodeMetricData.metricData[data.metric].maxTime=data.maxTime
				}
			}
			// data split
            fieldKeyDataList:=s.splitData(nodeMetricData)

            //generateDataFile
            s.generateDataFile(dataBase,table,fieldKeyDataList)

            // setDataFile
			tableFileDataList:=s.setDataFile(dataBase,table,fieldKeyDataList)
			// compressData
			tableFileChunkList:=s.compressTask(dataBase,table,tableFileDataList)
			// writeData
			s.writeData(tagKv,tableFileChunkList)
		}
	case false:

	}
}

func (s *kv) splitData(nodeMetricData *nodeMetricData) map[string][]*metricData {
	fieldKeyData:=make(map[string][]*metricData,0)
	if nodeMetricData.metricData !=nil {
		for metric,data:=range nodeMetricData.metricData {
		    m:=data.count / s.compressCount
		    n:=data.count % s.compressCount
		    if m == 0 && n > 0 {
		   	   dataList,ok:=fieldKeyData[metric]
		   	   if ok {
		   	   	  dataList=append(dataList,data)
			   } else {
			   	  dataList=make([]*metricData,0)
			   	  dataList=append(dataList,data)
			   	  fieldKeyData[metric]=dataList
			   }
		    }
		    if m > 0 {
		       for k:=0; k < m; k++ {
				   dataList,ok:=fieldKeyData[metric]
				   if ok {
				   	   tmpPoints:=data.points[k*s.compressCount:(k+1)*s.compressCount]
				   	   tmpMetricData:=newMetricData(metric,tmpPoints)
					   tmpMetricData.minTime=tmpPoints[0].T
					   tmpMetricData.maxTime=tmpPoints[len(tmpPoints) -1].T
					   tmpMetricData.count=len(tmpPoints)
					   fieldKeyData[metric]=append(fieldKeyData[metric],tmpMetricData)
				   } else {
					   dataList=make([]*metricData,0)
					   tmpPoints:=data.points[k*s.compressCount:(k+1)*s.compressCount]
					   tmpMetricData:=newMetricData(metric,tmpPoints)
					   tmpMetricData.minTime=tmpPoints[0].T
					   tmpMetricData.maxTime=tmpPoints[len(tmpPoints) -1].T
					   tmpMetricData.count=len(tmpPoints)
					   dataList=append(dataList,tmpMetricData)
					   fieldKeyData[metric]=dataList
				   }
			   }
		       if n > 0 {
				   tmpPoints:=data.points[m*s.compressCount:]
				   tmpMetricData:=newMetricData(metric,tmpPoints)
				   tmpMetricData.minTime=tmpPoints[0].T
				   tmpMetricData.maxTime=tmpPoints[len(tmpPoints) -1].T
				   tmpMetricData.count=len(tmpPoints)
				   fieldKeyData[metric]=append(fieldKeyData[metric],tmpMetricData)
			   }
			}
		}
	}
	return fieldKeyData
}

func (s *kv) generateDataFile(dataBase,tableName string,fieldKeyDataList map[string][]*metricData) {
	if fieldKeyDataList != nil {
		for _,dataList:=range fieldKeyDataList {
			if len(dataList) > 0 {
				for _,data:=range dataList {
						maxTime:=data.maxTime
						minTime:=data.minTime
						tableFileTimeRange:=s.scanDataDir(dataBase,tableName,minTime,maxTime)
						if tableFileTimeRange != nil {
							for tableFile,_:=range tableFileTimeRange {
								ok:=utils.CheckFileIsExist(tableFile)
								if !ok {
									db:=openDB(tableFile)
									db.Close()
								}
							}
						}
				}
			}
		}
	}
}



func (s *kv) setDataFile(dataBase,tableName string,fieldKeyDataList map[string][]*metricData) map[string][]*metricData {
	tableFileDataList:=make(map[string][]*metricData)
	if fieldKeyDataList != nil {
	   for fieldKey,dataList:=range fieldKeyDataList {
	      if len(dataList) > 0 {
			  var wg sync.WaitGroup
			  for _,data:=range dataList {
				  wg.Add(1)
				  go func(data *metricData,wg *sync.WaitGroup) {
					  maxTime:=data.maxTime
					  minTime:=data.minTime
					  tableFileTimeRange:=s.scanDataDir(dataBase,tableName,minTime,maxTime)
	                  if tableFileTimeRange != nil {
	                  	   for tableFile,timeRange:=range tableFileTimeRange {
							   s.mutex.Lock()
							   startTime:=timeRange.startTime
							   endTime:=timeRange.endTime
							   index1:=search(data.points,startTime)
							   index2:=search(data.points,endTime)
							   tmpPoints:=data.points[index1:index2]
							   tmpPoints=append(tmpPoints,data.points[index2])
							   _,ok:=tableFileDataList[tableFile]
							   if ok {
							   	  tmpMetricData:=newMetricData(fieldKey,tmpPoints)
							   	  tmpMetricData.count=len(tmpPoints)
							   	  tmpMetricData.minTime=startTime
							   	  tmpMetricData.maxTime=endTime
							   	  tableFileDataList[tableFile]=append(tableFileDataList[tableFile],tmpMetricData)
							   } else {
							   	  metricDataList:=make([]*metricData,0)
								  tmpMetricData:=newMetricData(fieldKey,tmpPoints)
								  tmpMetricData.count=len(tmpPoints)
								  tmpMetricData.minTime=startTime
								  tmpMetricData.maxTime=endTime
								  metricDataList=append(metricDataList,tmpMetricData)
								  tableFileDataList[tableFile]=metricDataList
							   }
							   s.mutex.Unlock()
						   }
					  }
					  wg.Done()
				  }(data,&wg)
			  }
			  wg.Wait()
		  }
	   }
	}
	return tableFileDataList
}



func (s *kv) writeData (tagKv string,tableFileChunkList map[string][]*compressPoints) {
	if tableFileChunkList != nil {
		for tableFile,chunkList:=range tableFileChunkList {
		   if len(chunkList) > 0 {
		   	  go func(tableFile string ,chunkList []*compressPoints ) {
				  e:=writeKv(tableFile,tagKv,chunkList)
				  if e !=nil {
					  log.Println("write kv storage failed !",e)
				  }
			  }(tableFile,chunkList)
		   }
		}
	}
}



type ChunkDataList []*chunkData


func (cd ChunkDataList) Len() int {
	return len(cd)
}

func (cd ChunkDataList) Less(i,j int) bool {
	return bytes.Compare(cd[i].timestamp,cd[j].timestamp) < 0
}

func (cd ChunkDataList) Swap(i,j int) {
	cd[i],cd[j]=cd[j],cd[i]
}

type filterData struct {
	version int64
	kv map[int64]float64
}

type filterDataList []*filterData

func (f filterDataList) Len() int {
	return len(f)
}

func (f filterDataList) Less(i,j int) bool {
	return f[i].version < f[j].version
}

func (f filterDataList) Swap(i,j int) {
	f[i],f[j]=f[j],f[i]
}


func newFilterData() *filterData {
	return &filterData{
		version: 0,
		kv:      make(map[int64]float64,0),
	}
}

func (s *kv) filterChunkDataList(chunkDataList ChunkDataList,startTime,endTime int64) *filterData {
	data:=newFilterData()
	if len(chunkDataList) > 0 {
		sort.Sort(chunkDataList)
		filterDataList:=make([]*filterData,0)
		for _,chunkData:=range chunkDataList {
			filterData:=newFilterData()
			filterData.version=utils.ByteToInt64(chunkData.timestamp)
			c:=compress.NewBXORChunk(chunkData.chunk)
			it:=c.Iterator(nil)
			for it.Next() {
				tt,vv:=it.At()
				if tt >= startTime && tt <= endTime {
					 filterData.kv[tt]=vv
				}
			}
			filterDataList=append(filterDataList,filterData)
		}
		if len(filterDataList) > 0 {
			kv:=make(map[int64]float64,0)
			for _,filterData:=range filterDataList {
				 kv=mergeMap(filterData.kv,kv)
			}
			data.kv=kv
			data.version=filterDataList[len(filterDataList)-1].version
			return data
		}
	}
	return data
}


func (s *kv) filterDataList(value *point.Value,filterDataList filterDataList) {
	if len(filterDataList) > 0 {
		sort.Sort(filterDataList)
		for _,filterData:=range filterDataList {
			value.Kv=mergeMap(filterData.kv,value.Kv)
		}
	}
}





func (s *kv) readData(dataBase,tableName,tagKv,fieldKey string,startTime,endTime int64) [][]byte {
	sTime := strconv.FormatInt(startTime, 10)
	eTime := strconv.FormatInt(endTime, 10)
	tableFileList:=s.getTableFile(dataBase,tableName,sTime,eTime)
	dataSet:=make([][]byte,0)
	var wg sync.WaitGroup
	if tableFileList != nil {
		for k,_:=range tableFileList {
			tableFile:=k
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				value:=&point.Value {
					Kv: make(map[int64]float64,0),
				}
				if s.isCompressed == true {
					chunkDataGroup,db:=readKv(tableFile,tagKv,fieldKey,startTime,endTime)
					defer db.Close()
					if chunkDataGroup !=nil {
						var wg sync.WaitGroup
						var dataList filterDataList
						for _,chunkList:=range chunkDataGroup {
							wg.Add(1)
							go func(chunkList ChunkDataList,wg *sync.WaitGroup) {
								data:=s.filterChunkDataList(chunkList,startTime,endTime)
								dataList=append(dataList,data)
								wg.Done()
							}(chunkList,&wg)
							wg.Wait()
						}
						if len(dataList) > 0 {
							s.filterDataList(value,dataList)
						}
					}
				}
				/*if s.isCompressed ==false {
					db:=scanKv(tableFile,tagKv,fieldKey,value,startTime,endTime)
					defer db.Close()
				}*/
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


func (s *kv) compressTask(dataBase,table string,tableFileDataList map[string][]*metricData) map[string][]*compressPoints {
	tableFileChunkList:=make(map[string][]*compressPoints)
	if tableFileDataList != nil {
		for tableFile,dataList:=range tableFileDataList {
			if len(dataList) > 0 {
				var wg sync.WaitGroup
				for _,data:=range dataList {
					wg.Add(1)
					go func(data *metricData,wg *sync.WaitGroup) {
						chunk:=s.compressData(data)
						s.mutex.Lock()
						_,ok:=tableFileChunkList[tableFile]
						if ok {
							tableFileChunkList[tableFile]=append(tableFileChunkList[tableFile],chunk)
						} else {
							chunkList:=make([]*compressPoints,0)
							chunkList=append(chunkList,chunk)
							tableFileChunkList[tableFile]=chunkList
						}
						s.mutex.Unlock()
						wg.Done()
					}(data,&wg)
				}
				wg.Wait()
			}
		}
	}
	return tableFileChunkList
}


func (s *kv) compressData(metricData *metricData) *compressPoints{
	chunk := compress.NewXORChunk()
	app, err := chunk.Appender()
	if err != nil {
		log.Println(err)
	}
	if len(metricData.points) > 0 {
		for _,p:=range metricData.points {
			app.Append(p.T, p.V)
		}
	}
	return &compressPoints {
		maxTime:  metricData.maxTime,
		minTime:  metricData.minTime,
		chunk:    chunk,
		fieldKey: metricData.metric,
		count: metricData.count,
	}
}


func search (points []utils.Point,t int64) int {
	left:=0
	right:= len(points) - 1
	mid:= (left + right) / 2
	for left < right {
		if points[mid].T < t {
			left=mid+1
			mid=(left+right) / 2
		}
		if points[mid].T > t{
			right=mid-1
			mid=(left+right) / 2
		}
		if points[mid].T == t {
			return mid
		}
	}
	return left
}


type timeRange struct {
	startTime int64
	endTime int64
}

func newTimeRange(startTime,endTime int64) timeRange {
	return timeRange {
		startTime: startTime,
		endTime:   endTime,
	}
}


func (s *kv) spiltTimeRange(minTime,maxTime int64) []timeRange {

	timeRangeList:=make([]timeRange,0)

	m:= (maxTime - minTime) / s.duration.Nanoseconds()
	n:= (maxTime - minTime) % s.duration.Nanoseconds()

	if m ==0 {
		timeRange:=timeRange {
			startTime: 0,
			endTime:   0,
		}
		timeRange.endTime=maxTime
		timeRange.startTime=minTime
		timeRangeList=append(timeRangeList,timeRange)
		return timeRangeList
	}

	if m > 0 && n == 0 {
	   k:=int(m)
	   tempMaxTime:=getEndTime(minTime,s.duration)
	   tempMinTime:=minTime
	   for i:=0; i< k ; i++ {
		   timeRange:=timeRange{
			   startTime: 0,
			   endTime:   0,
		   }
		   timeRange.endTime=tempMaxTime
		   timeRange.startTime=tempMinTime
		   tempMinTime=tempMaxTime
		   tempMaxTime=getEndTime(tempMinTime,s.duration)
		   timeRangeList=append(timeRangeList,timeRange)
	   }
	   return timeRangeList
	}

	if m > 0 && n != 0 {
		k:=int(m)
		tempMaxTime:=getEndTime(minTime,s.duration)
		tempMinTime:=minTime

		for i:=0; i< k ; i++ {
			timeRange:=timeRange{
				startTime: 0,
				endTime:   0,
			}
			timeRange.endTime=tempMaxTime
			timeRange.startTime=tempMinTime
			tempMinTime=tempMaxTime
			tempMaxTime=getEndTime(tempMinTime,s.duration)
			timeRangeList=append(timeRangeList,timeRange)
		}
		timeRange:=timeRange{
			startTime: tempMaxTime,
			endTime:   maxTime,
		}
		timeRangeList=append(timeRangeList,timeRange)
		return timeRangeList
	}
	return timeRangeList
}

func (s *kv) scanDataDir(dataBase,tableName string,minTime,maxTime int64) map[string]timeRange {
	timeRangeList:=s.spiltTimeRange(minTime,maxTime)
	tableFileMap:=make(map[string]timeRange,0)
	if timeRangeList != nil && len(timeRangeList) > 0 {
		for _,timeRange:= range timeRangeList {
			var tableFile string
			startTime:= strconv.FormatInt(timeRange.startTime,10)
			endTime:= strconv.FormatInt(timeRange.endTime,10)
			dataBaseDir,exist:=s.dataBaseDirIsExist(dataBase)
			if exist == true {
				fileList,_:=ioutil.ReadDir(dataBaseDir)
				if len(fileList) > 0 {
				   for _,file:=range fileList {
					   if !strings.HasSuffix(file.Name(),"lock") {
						   tableInfo:=strings.Split(file.Name(),"-")
						   tn:=tableInfo[0]
						   st:=tableInfo[1]
						   et:=strings.Split(tableInfo[2],".")[0]
						   if strings.Compare(tn,tableName) == 0 {
							   if strings.Compare(startTime, st) >= 0 && strings.Compare(startTime, et) < 0 && strings.Compare(endTime, et) <= 0 {
								   tableFile = dataBaseDir + sep + file.Name()
								   tableFileMap[tableFile]=timeRange
							   }
							   if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,st) >0 && strings.Compare(endTime,et) < 0 {
								   tableFile = dataBaseDir + sep + file.Name()
								   startTime,_:=strconv.ParseInt(st, 10, 64)
								   tableFileMap[tableFile]= newTimeRange(startTime,timeRange.endTime)
								   tableFile = dataBaseDir+ sep + tableName +"-"+strconv.FormatInt(startTime - s.duration.Nanoseconds(),10)+"-"+st+".db"
								   tableFileMap[tableFile]=newTimeRange(startTime-s.duration.Nanoseconds(),startTime)
							   }
							   if strings.Compare(startTime,st) >= 0 &&  strings.Compare(startTime,et) < 0 && strings.Compare(endTime,et) > 0 {
								   tableFile = dataBaseDir + sep + file.Name()
								   endTime,_:=strconv.ParseInt(et, 10, 64)
								   tableFileMap[tableFile]= newTimeRange(timeRange.startTime,endTime)
								   tableFile = dataBaseDir+ sep + tableName+"-"+et+"-"+strconv.FormatInt(endTime + s.duration.Nanoseconds(),10)+".db"
								   tableFileMap[tableFile]=newTimeRange(endTime,endTime+s.duration.Nanoseconds())
							   }
						   }
					   }
				   }
				} else {
					if (timeRange.endTime - timeRange.startTime) < s.duration.Nanoseconds() {
						tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+ strconv.FormatInt(timeRange.startTime+s.duration.Nanoseconds(),10)+".db"
						tableFileMap[tableFile]=newTimeRange(timeRange.startTime,timeRange.startTime+s.duration.Nanoseconds())
					}
					if (timeRange.endTime - timeRange.startTime) >= s.duration.Nanoseconds() {
						tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+endTime+".db"
						tableFileMap[tableFile]=timeRange
					}
				}
			} else {
				rand.Seed(time.Now().UnixNano())
				n := rand.Intn(len(s.dataDir))
				e:=os.MkdirAll(s.dataDir[n]+dataBase,os.ModePerm)
				if e !=nil {
					log.Println("failed create data dir",s.dataDir[n]+dataBase,e)
				}
				if (timeRange.endTime - timeRange.startTime) < s.duration.Nanoseconds() {
					tableFile=s.dataDir[n]+dataBase+sep+tableName+"-"+startTime+"-"+ strconv.FormatInt(timeRange.startTime+s.duration.Nanoseconds(),10)+".db"
					tableFileMap[tableFile]=newTimeRange(timeRange.startTime,timeRange.startTime+s.duration.Nanoseconds())
				}
				if (timeRange.endTime - timeRange.startTime) >= s.duration.Nanoseconds() {
					tableFile=s.dataDir[n]+dataBase+sep+tableName+"-"+startTime+"-"+endTime+".db"
					tableFileMap[tableFile]=timeRange
				}
			}
		}
	}
    return tableFileMap
}

func (s *kv) dataBaseDirIsExist(dataBase string) (string,bool) {
	for _,dir:= range s.dataDir {
		if utils.CheckFileIsExist(dir+dataBase) {
			return dir+dataBase,true
		}
	}
	return "",false
}

func (s *kv) getTableFile(dataBase,tableName,startTime,endTime string) map[string]bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var tableFile string
	tableFileList:=make(map[string]bool,0)
	dataBaseDir,exist:=s.dataBaseDirIsExist(dataBase)
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
						tableFileList1:=s.getTableFile(dataBase,tableName,startTime,st)
						tableFileList2:=s.getTableFile(dataBase,tableName,st,endTime)
						for tableFileK1,tableFileV1:=range tableFileList1 {
							tableFileList[tableFileK1]=tableFileV1
						}
						for tableFileK2,tableFileV2:=range tableFileList2 {
							tableFileList[tableFileK2]=tableFileV2
						}
					}
					if strings.Compare(startTime,st) >= 0 &&  strings.Compare(startTime,et) < 0 && strings.Compare(endTime,et) > 0 {
						tableFileList1:=s.getTableFile(dataBase,tableName,startTime,et)
						tableFileList2:=s.getTableFile(dataBase,tableName,et,endTime)
						for tableFileK1,tableFileV1:=range tableFileList1 {
							tableFileList[tableFileK1]=tableFileV1
						}
						for tableFileK2,tableFileV2:=range tableFileList2 {
							tableFileList[tableFileK2]=tableFileV2
						}
					}
					if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,et) >= 0 {
						tableFileList1:=s.getTableFile(dataBase,tableName,startTime,st)
						tableFileList2:=s.getTableFile(dataBase,tableName,et,endTime)
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
