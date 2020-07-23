package storage

import (
	"bytes"
	"fmt"
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
	timeChunk []byte
	valueChunk []byte
	fieldKey string
	count int
	maxValue []byte
	minValue []byte
	metricType int32
	precision int32
}

var dnCount int

func (s *kv) writeDataLinked(dn *dataNodeLinked) {

	go func(dn *dataNodeLinked) {
		// 这里针对每个seriesKey的链表开启了协程进行处理，其实这里应该去把链表中的数据进行一次，合并。
		// 这里逻辑需要优化
		// 1、将数据按照metricKey,进行合并
		// 2、将合并的数据按照设置的数据块的的大小进行分块切割；
		nodeMetric:=newNodeMetricData()
		current:= dn.head.next
		for current != nil {
		  if current.fields !=nil {
		     for key,value:=range current.fields {
		     	 fieldData,ok:=nodeMetric.metricData[key]
		     	 if ok {
					 pointKv := utils.NewSortMap(value.Metric)
					 sort.Sort(pointKv)
					 fieldData.points=append(fieldData.points,pointKv...)
				 } else {
					 pointKv := utils.NewSortMap(value.Metric)
					 sort.Sort(pointKv)
				 	 data:=newMetricData(key,pointKv,value.MetricType,1)
				     nodeMetric.metricData[key]=data
				 }
			 }
		  }
		    current=current.next
		/*	s.mutex.Lock()
			dnCount+=1
			log.Println("buffer dn data count: ",dnCount)
			s.mutex.Unlock()*/
		}
		s.writeKv(dn.dataBase,dn.tableName,dn.tagKv,nodeMetric)
		/*nodeData:=make([]*metricData,0)
		current := dn.head
		for current != nil {
			if len(current.metrics) > 0 {
				nodeData=append(nodeData,current.metrics...)
			}
			current = current.next
		}*/
		//s.writeDataKv(dn.dataBase,dn.tableName,dn.tagKv,nodeMetric)
	}(dn)
}


type nodeMetricData struct {
	metricData map[string]*metricData
}

func newNodeMetricData() *nodeMetricData{
	return &nodeMetricData{
		metricData: make(map[string]*metricData,0),
	}
}

var writeCount,writetotal int

func (s *kv) writeKv(database,table,tagKv string,nodeMetricData *nodeMetricData) {
	switch s.isCompressed {
	case true:

	/*	s.mutex.Lock()
		writeCount+=1
		writetotal+= len(nodeMetricData.metricData["status"].points)
		fmt.Printf("receive request count: %d and write total %d \n",writeCount,writetotal)
		fmt.Println("=================================")
		s.mutex.Unlock()*/

		fieldKeyDataList:=s.splitData(nodeMetricData)
		//以下代码需要优化，重复扫描了两遍目录
		//generateDataFile
		tableFileDataList:=s.generateDataFile(database,table,fieldKeyDataList)
		// setDataFile
		//tableFileDataList:=s.setDataFile(database,table,fieldKeyDataList)
		// compressData
		//tableFileChunkList:=s.compressTask(tagKv,tableFileDataList)
		s.compressTask(tagKv,tableFileDataList)
		// writeData
		//s.writeData(tagKv,tableFileChunkList)
	}

}




/*func (s *kv) writeDataKv(dataBase,table,tagKv string,nodeData []*metricData) {
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
					nodeMetricData.metricData[data.metric]=newMetricData(data.metric,data.points,data.metricType,data.precision)
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
			tableFileChunkList:=s.compressTask(tableFileDataList)
			// writeData
			s.writeData(tagKv,tableFileChunkList)
		}
	case false:

	}
}*/

func (s *kv) splitData(nodeMetricData *nodeMetricData) map[string][]*metricData {
	//根据数据压缩块的大小配置，进行数据切片；
	fieldKeyData:=make(map[string][]*metricData,0)
	if nodeMetricData.metricData !=nil {
		for metric,data:=range nodeMetricData.metricData {
			data.count= len(data.points)
            //按照时间范围做一个归并排序
            sortPoints:=utils.CombineSort(data.points)
            data.points=sortPoints
			m:=data.count / s.compressCount
		    n:=data.count % s.compressCount
		    if m == 0 && n > 0 {
		   	   dataList,ok:=fieldKeyData[metric]
		   	   data.minTime=data.points[0].T
		   	   data.maxTime=data.points[data.count-1].T
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
				   	   tmpMetricData:=newMetricData(metric,tmpPoints,data.metricType,data.precision)
					   tmpMetricData.minTime=tmpPoints[0].T
					   tmpMetricData.maxTime=tmpPoints[len(tmpPoints) -1].T
					   tmpMetricData.count=len(tmpPoints)
					   dataList=append(dataList,tmpMetricData)
				   } else {
					   dataList=make([]*metricData,0)
					   tmpPoints:=data.points[k*s.compressCount:(k+1)*s.compressCount]
					   tmpMetricData:=newMetricData(metric,tmpPoints,data.metricType,data.precision)
					   tmpMetricData.minTime=tmpPoints[0].T
					   tmpMetricData.maxTime=tmpPoints[len(tmpPoints) -1].T
					   tmpMetricData.count=len(tmpPoints)
					   dataList=append(dataList,tmpMetricData)
					   fieldKeyData[metric]=dataList
				   }
			   }
		       if n > 0 {
				   tmpPoints:=data.points[m*s.compressCount:]
				   tmpMetricData:=newMetricData(metric,tmpPoints,data.metricType,data.precision)
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

var splitDataCount int

func (s *kv) generateDataFile(dataBase,tableName string,fieldKeyDataList map[string][]*metricData) map[string][]*metricData {
	tableFileDataList:=make(map[string][]*metricData)

	/*s.mutex.Lock()
	for _,data:=range fieldKeyDataList["status"] {
		splitDataCount+=len(data.points)
        fmt.Printf("split data count : %d\n",splitDataCount)
	}
	s.mutex.Unlock()*/

	// to-do
	//这里重复扫描数据目录，同时还丢了数据
	if fieldKeyDataList != nil {
		log.Println(len(fieldKeyDataList))
		for fieldKey,dataList:=range fieldKeyDataList {
			if len(dataList) > 0 {
				for _,data:=range dataList {
						maxTime:=data.maxTime
						minTime:=data.minTime
						tableFileTimeRange:=s.scanDataDir(dataBase,tableName,minTime,maxTime)
						if tableFileTimeRange != nil {
							for tableFile,timeRange:=range tableFileTimeRange {
								s.mutex.Lock()
								if tableFile !="" {
									ok:=utils.CheckFileIsExist(tableFile)
									if !ok {
										db:=openDB(tableFile)
										db.Close()
									}
								}
								startTime:=timeRange.startTime
								endTime:=timeRange.endTime
								index1:=search(data.points,startTime)
								index2:=search(data.points,endTime)
								//fmt.Printf("data endtime: %d and time range endtime: %d\n",data.points[index2].T,endTime)
								var tmpPoints []utils.Point
								if endTime - startTime < s.duration.Nanoseconds() && endTime -startTime != 0 {
									tmpPoints=data.points[index1:index2+1]
								}
								if endTime - startTime == 0 {
									tmpPoints=data.points[index1:index2]
								}
								splitDataCount+=len(tmpPoints)
								fmt.Printf("data is split by time: %d,%d,%d and tmp count %d\n", len(tmpPoints),index1,index2,splitDataCount)
								_,ok:=tableFileDataList[tableFile]
								if ok {
									tmpMetricData:=newMetricData(fieldKey,tmpPoints,data.metricType,data.precision)
									tmpMetricData.count=len(tmpPoints)
									tmpMetricData.minTime=startTime
									tmpMetricData.maxTime=endTime
									tableFileDataList[tableFile]=append(tableFileDataList[tableFile],tmpMetricData)
								} else {
									metricDataList:=make([]*metricData,0)
									tmpMetricData:=newMetricData(fieldKey,tmpPoints,data.metricType,data.precision)
									tmpMetricData.count=len(tmpPoints)
									tmpMetricData.minTime=startTime
									tmpMetricData.maxTime=endTime
									metricDataList=append(metricDataList,tmpMetricData)
									tableFileDataList[tableFile]=metricDataList
								}
								s.mutex.Unlock()
							}
						}
				}
			}
		}
	}
	return tableFileDataList
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
							   	  tmpMetricData:=newMetricData(fieldKey,tmpPoints,data.metricType,data.precision)
							   	  tmpMetricData.count=len(tmpPoints)
							   	  tmpMetricData.minTime=startTime
							   	  tmpMetricData.maxTime=endTime
							   	  tableFileDataList[tableFile]=append(tableFileDataList[tableFile],tmpMetricData)
							   } else {
							   	  metricDataList:=make([]*metricData,0)
								  tmpMetricData:=newMetricData(fieldKey,tmpPoints,data.metricType,data.precision)
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


var total int
var count int

func (s *kv) writeData(tagKv string, tableFileChunkList map[string][]*compressPoints) {
	if tableFileChunkList != nil {
		for tableFile, chunkList := range tableFileChunkList {
			s.mutex.Lock()
			count += 1
			total += chunkList[0].count
			fmt.Printf("request count: %d and chunk count: %d and total count: %d \n", count, chunkList[0].count,total)
			s.mutex.Unlock()
			if len(chunkList) > 0 {
				go func(tableFile string, chunkList []*compressPoints) {
					e := writeKv(tableFile, tagKv, chunkList)
					if e != nil {
						log.Println("write kv storage failed !", e)
					}

				}(tableFile, chunkList)
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
	kv map[int64][]byte
	datatype int32
	precision int32
	maxValue []byte
	minValue []byte
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
		kv:      make(map[int64][]byte,0),
	}
}

func (s *kv) filterChunkDataList(chunkDataList ChunkDataList,startTime,endTime int64) *filterData {
	data:=newFilterData()
	if len(chunkDataList) > 0 {
		sort.Sort(chunkDataList)
		filterDataList:=make([]*filterData,0)
		data.precision=chunkDataList[0].precision
		data.datatype=chunkDataList[0].metricType
		data.maxValue=chunkDataList[0].maxValue
		data.minValue=chunkDataList[0].minValue
		for _,chunkData:=range chunkDataList {
			filterData:=newFilterData()
			filterData.version=utils.ByteToInt64(chunkData.timestamp)
			c:=compress.NewBXORChunk(chunkData.chunk)
			it:=c.Iterator(nil)
			for it.Next() {
				tt,vv:=it.At()
				if tt >= startTime && tt <= endTime {
					 bv:=utils.Float64ToByte(vv)
					 if bytes.Compare(bv,data.minValue) <= 0 {
					 	data.minValue=bv
					 }
					 if bytes.Compare(bv,data.maxValue) >= 0 {
					 	data.maxValue=bv
					 }
					 filterData.kv[tt]=bv
				}
			}
			filterDataList=append(filterDataList,filterData)
		}
		if len(filterDataList) > 0 {
			kv:=make(map[int64][]byte,0)
			for _,filterData:=range filterDataList {
				 if filterData.kv !=nil && filterData.version > 0 {
					 kv=mergeMap(filterData.kv,kv)
				 }
			}
			data.kv=kv
			data.version=filterDataList[len(filterDataList)-1].version
			return data
		}
	}
	return data
}


func (s *kv) filterDataList(value *point.Metric,filterDataList filterDataList) {
	if len(filterDataList) > 0 {
		sort.Sort(filterDataList)
		for _,filterData:=range filterDataList {
			if len(filterData.kv) > 0 && filterData.version > 0 {
				value.Metric=mergeMap(filterData.kv,value.Metric)
				value.MetricType=filterData.datatype
			}
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
				metric:=&point.Metric {
					Metric:               make(map[int64][]byte,0),
					MetricType:           0,
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
							s.filterDataList(metric,dataList)
						}
					}
				}
				buf,e:=proto.Marshal(metric)
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


var uncompressTotal int

func (s *kv) compressTask(tagKv string,tableFileDataList map[string][]*metricData) map[string][]*compressPoints {
	tableFileChunkList:=make(map[string][]*compressPoints)
	if tableFileDataList != nil {
		for tableFile,dataList:=range tableFileDataList {
		/*	s.mutex.Lock()
			uncompressTotal+= dataList[0].count
			fmt.Printf("uncompress data count: %d\n",uncompressTotal)
			s.mutex.Unlock()*/
			chunkList:=make([]*compressPoints,0)
			if len(dataList) > 0 {
				var wg sync.WaitGroup
				for _,data:=range dataList {
					wg.Add(1)
					go func(data *metricData,wg *sync.WaitGroup) {
						s.mutex.Lock()
						chunk:=s.compressData(data)
						chunkList=append(chunkList,chunk)
						/*_,ok:=tableFileChunkList[tableFile]
						if ok {
							tableFileChunkList[tableFile]=append(tableFileChunkList[tableFile],chunk)
						} else {
							chunkList:=make([]*compressPoints,0)
							chunkList=append(chunkList,chunk)
							tableFileChunkList[tableFile]=chunkList
						}*/
						s.mutex.Unlock()
						wg.Done()
					}(data,&wg)
				}
				wg.Wait()
			}
			/*s.mutex.Lock()
			count += 1
			total += chunkList[0].count
			fmt.Printf("request count: %d and chunk count: %d and total count: %d \n", count, chunkList[0].count,total)
			s.mutex.Unlock()*/
			e := writeKv(tableFile, tagKv, chunkList)
			if e != nil {
				log.Println("write kv storage failed !", e)
			}
		}
	}
	return tableFileChunkList
}


func (s *kv) compressData(metricData *metricData) *compressPoints {

	var timeChunk []byte
	var valueChunk []byte
	var minValue []byte
	var maxValue []byte
	/*
	to do code improve
	*/
	switch metricData.metricType {
	case utils.Bool:
		enc1:=compress.NewBooleanEncoder(metricData.count)
		enc2:=compress.NewTimeEncoder(metricData.count)
		if len(metricData.points) > 0 {
			minValue=metricData.points[0].V
			maxValue=metricData.points[0].V
			for _,p:=range metricData.points {
				if bytes.Compare(p.V,[]byte{0}) == 0 {
					enc1.Write(false)
				}else {
					enc1.Write(true)
				}
				enc2.Write(p.T)
				if bytes.Compare(p.V,minValue) < 0 {
					   minValue=p.V
				}
				if bytes.Compare(p.V,maxValue) >= 0 {
					   maxValue=p.V
				}
			}
		}
		timeChunk,_= enc2.Bytes()
		valueChunk,_= enc1.Bytes()
		break
	case utils.Long:
		enc1:=compress.NewIntegerEncoder(metricData.count)
		enc2:=compress.NewTimeEncoder(metricData.count)
		if len(metricData.points) > 0 {
			minValue=metricData.points[0].V
			maxValue=metricData.points[0].V
			for _,p:=range metricData.points {
				enc1.Write(utils.TransByteToData(utils.Long,p.V).(int64))
				enc2.Write(p.T)
				if bytes.Compare(p.V,minValue) < 0 {
					minValue=p.V
				}
				if bytes.Compare(p.V,maxValue) >= 0 {
					maxValue=p.V
				}
			}
		}
		timeChunk,_= enc2.Bytes()
		valueChunk,_= enc1.Bytes()
		break
	case utils.Double:
		enc1:=compress.NewFloatEncoder()
		enc2:=compress.NewTimeEncoder(metricData.count)
		if len(metricData.points) > 0 {
			minValue=metricData.points[0].V
			maxValue=metricData.points[0].V
			for _,p:=range metricData.points {
				enc1.Write(utils.TransByteToData(utils.Double,p.V).(float64))
				enc2.Write(p.T)
				if bytes.Compare(p.V,minValue) < 0 {
					minValue=p.V
				}
				if bytes.Compare(p.V,maxValue) >= 0 {
					maxValue=p.V
				}
			}
		}
		timeChunk,_= enc2.Bytes()
		valueChunk,_= enc1.Bytes()
		break
	case utils.Int:
		enc1:=compress.NewIntegerEncoder(metricData.count)
		enc2:=compress.NewTimeEncoder(metricData.count)
		if len(metricData.points) > 0 {
			minValue=metricData.points[0].V
			maxValue=metricData.points[0].V
			for _,p:=range metricData.points {
				b:=make([]byte,4)
				b=append(b,p.V...)
				enc1.Write(utils.TransByteToData(utils.Long,b).(int64))
				enc2.Write(p.T)
				if bytes.Compare(p.V,minValue) < 0 {
					minValue=p.V
				}
				if bytes.Compare(p.V,maxValue) >= 0 {
					maxValue=p.V
				}
			}
		}
		timeChunk,_= enc2.Bytes()
		valueChunk,_= enc1.Bytes()
		break
	case utils.Float:
		enc1:=compress.NewFloatEncoder()
		enc2:=compress.NewTimeEncoder(metricData.count)
		if len(metricData.points) > 0 {
			minValue=metricData.points[0].V
			maxValue=metricData.points[0].V
			for _,p:=range metricData.points {
				enc1.Write(utils.TransByteToData(utils.Double,p.V).(float64))
				enc2.Write(p.T)
				if bytes.Compare(p.V,minValue) < 0 {
					minValue=p.V
				}
				if bytes.Compare(p.V,maxValue) >= 0 {
					maxValue=p.V
				}
			}
		}
		timeChunk,_= enc2.Bytes()
		valueChunk,_= enc1.Bytes()
		break
	}
	return &compressPoints {
		maxTime:  metricData.maxTime,
		minTime:  metricData.minTime,
		timeChunk: timeChunk,
		valueChunk: valueChunk,
		fieldKey: metricData.metric,
		count: metricData.count,
		metricType: metricData.metricType,
		precision: metricData.precision,
		minValue:minValue,
		maxValue:maxValue,
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
		if points[mid].T > t {
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



// 这部分可以进行性能优化，负责将本地的数据文件的时间范围信息，注册到元数据，而不必每次都进行数据文件的扫描

func (s *kv) scanDataDir(dataBase,tableName string,minTime,maxTime int64) map[string]timeRange {
	//按照配置的时间范围，对数据进行时间切片，将数据分割
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
				   	   tableFileMap=s.setTableFile(dataBase,tableName,timeRange,file.Name())
					  /* if !strings.HasSuffix(file.Name(),"lock") {
						   tableInfo:=strings.Split(file.Name(),"-")
						   tn:=tableInfo[0]
						   st:=tableInfo[1]
						   et:=strings.Split(tableInfo[2],".")[0]
						   if strings.Compare(tn,tableName) == 0 {
						   	   //根据数据按照时间的切片范围，获取对应的数据文件，数据正好落在st-et数据文件范围内
							   if strings.Compare(startTime, st) >= 0 && strings.Compare(startTime, et) < 0 && strings.Compare(endTime, et) < 0 {
								   tableFile = dataBaseDir + sep + file.Name()
								   tableFileMap[tableFile]=timeRange
							   }
							   //数据落在 st-et 的左-半中部分，这个时候数据将会再次根据时间范围进行切片starttime - st 和 st- endime
							   if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,st) >= 0 && strings.Compare(endTime,et) < 0 {
								   tableFile = dataBaseDir + sep + file.Name()
								   startTime,_:=strconv.ParseInt(st, 10, 64)
								   tableFileMap[tableFile]= newTimeRange(startTime,timeRange.endTime)
								   tableFile = dataBaseDir+ sep + tableName +"-"+strconv.FormatInt(startTime - s.duration.Nanoseconds(),10)+"-"+st+".db"
								   tableFileMap[tableFile]=newTimeRange(timeRange.startTime,startTime)
							   }
							   //数据落在 st-et 的右-半中部分，数据将会被分成starttime-et 和 et-endtime
							   if strings.Compare(startTime,st) >= 0 &&  strings.Compare(startTime,et) < 0 && strings.Compare(endTime,et) > 0 {
								   tableFile = dataBaseDir + sep + file.Name()
								   endTime,_:=strconv.ParseInt(et, 10, 64)
								   tableFileMap[tableFile]= newTimeRange(timeRange.startTime,endTime)
								   tableFile = dataBaseDir+ sep + tableName+"-"+et+"-"+strconv.FormatInt(endTime + s.duration.Nanoseconds(),10)+".db"
								   tableFileMap[tableFile]=newTimeRange(endTime,timeRange.endTime)
							   }
							   //数据落在st-et的左半部分
                               if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,st) <0 {
								   startTime,_:=strconv.ParseInt(st, 10, 64)
								   tableFile = dataBaseDir+ sep + tableName +"-"+strconv.FormatInt(startTime - s.duration.Nanoseconds(),10)+"-"+st+".db"
								   ok:=utils.CheckFileIsExist(tableFile)
								   if !ok {
									   db:=openDB(tableFile)
									   db.Close()
								   }
                               }
							   //数据落在st-et的右半部分
							   if strings.Compare(startTime,et) > 0 && strings.Compare(endTime,et) > 0 {
								   endTime,_:=strconv.ParseInt(et, 10, 64)
								   tableFile = dataBaseDir+ sep + tableName+"-"+et+"-"+strconv.FormatInt(endTime + s.duration.Nanoseconds(),10)+".db"
								   //tableFileMap[tableFile]=newTimeRange(endTime,endTime+s.duration.Nanoseconds())
								   ok:=utils.CheckFileIsExist(tableFile)
								   if !ok {
									   db:=openDB(tableFile)
									   db.Close()
								   }
							   }
						   }
					   }*/
				   }
				} else {
					if (timeRange.endTime - timeRange.startTime) < s.duration.Nanoseconds() {
						tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+ strconv.FormatInt(timeRange.startTime+s.duration.Nanoseconds(),10)+".db"
						tableFileMap[tableFile]=newTimeRange(timeRange.startTime,timeRange.startTime+s.duration.Nanoseconds())
					}
					if (timeRange.endTime - timeRange.startTime) == s.duration.Nanoseconds() {
						tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+ strconv.FormatInt(timeRange.startTime+s.duration.Nanoseconds(),10)+".db"
						tableFileMap[tableFile]=newTimeRange(timeRange.startTime,timeRange.startTime+s.duration.Nanoseconds())
						tableFile=dataBaseDir+sep+tableName+"-"+endTime+"-"+ strconv.FormatInt(timeRange.endTime+s.duration.Nanoseconds(),10)+".db"
						tableFileMap[tableFile]=newTimeRange(timeRange.endTime,timeRange.endTime)
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
					tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+ strconv.FormatInt(timeRange.startTime+s.duration.Nanoseconds(),10)+".db"
					tableFileMap[tableFile]=newTimeRange(timeRange.startTime,timeRange.startTime+s.duration.Nanoseconds())
				}
				if (timeRange.endTime - timeRange.startTime) == s.duration.Nanoseconds() {
					tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+ strconv.FormatInt(timeRange.startTime+s.duration.Nanoseconds(),10)+".db"
					tableFileMap[tableFile]=newTimeRange(timeRange.startTime,timeRange.startTime+s.duration.Nanoseconds())
					tableFile=dataBaseDir+sep+tableName+"-"+endTime+"-"+ strconv.FormatInt(timeRange.endTime+s.duration.Nanoseconds(),10)+".db"
					tableFileMap[tableFile]=newTimeRange(timeRange.endTime,timeRange.endTime)
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

func (s *kv) setTableFile(dataBase,tableName string,tr timeRange,fileName string) map[string]timeRange {
	var tableFile string
	tableFileMap:=make(map[string]timeRange,0)
	startTime:= strconv.FormatInt(tr.startTime,10)
	endTime:= strconv.FormatInt(tr.endTime,10)
	dataBaseDir,_:=s.dataBaseDirIsExist(dataBase)
	if !strings.HasSuffix(fileName, "lock") {
		tableInfo := strings.Split(fileName, "-")
		tn := tableInfo[0]
		st := tableInfo[1]
		et := strings.Split(tableInfo[2], ".")[0]
		if strings.Compare(tn, tableName) == 0 {
			ok,f:=s.isLeftPart(startTime, st, endTime, dataBaseDir, tableName)
			if ok {
				s.setTableFile(dataBase, tableName,tr,f)
			}
			ok,f=s.isRightPart(startTime, endTime, et, dataBaseDir, tableName)
			if ok {
				s.setTableFile(dataBase, tableName, tr,f)
			}
			//根据数据按照时间的切片范围，获取对应的数据文件，数据正好落在st-et数据文件范围内
			if strings.Compare(startTime, st) >= 0 && strings.Compare(startTime, et) < 0 && strings.Compare(endTime, et) < 0 {
				tableFile = dataBaseDir + sep + fileName
				tableFileMap[tableFile] = tr
			}
			//数据落在 st-et 的左-半中部分，这个时候数据将会再次根据时间范围进行切片starttime - st 和 st- endime
			if strings.Compare(startTime, st) < 0 && strings.Compare(endTime, st) >= 0 && strings.Compare(endTime, et) < 0 {
				tableFile = dataBaseDir + sep + fileName
				startTime, _ := strconv.ParseInt(st, 10, 64)
				tableFileMap[tableFile] = newTimeRange(startTime, tr.endTime)
				tableFile = dataBaseDir + sep + tableName + "-" + strconv.FormatInt(startTime-s.duration.Nanoseconds(), 10) + "-" + st + ".db"
				tableFileMap[tableFile] = newTimeRange(tr.startTime, startTime)
			}
			//数据落在 st-et 的右-半中部分，数据将会被分成starttime-et 和 et-endtime
			if strings.Compare(startTime, st) >= 0 && strings.Compare(startTime, et) < 0 && strings.Compare(endTime, et) > 0 {
				tableFile = dataBaseDir + sep + fileName
				endTime, _ := strconv.ParseInt(et, 10, 64)
				tableFileMap[tableFile] = newTimeRange(tr.startTime, endTime)
				tableFile = dataBaseDir + sep + tableName + "-" + et + "-" + strconv.FormatInt(endTime+s.duration.Nanoseconds(), 10) + ".db"
				tableFileMap[tableFile] = newTimeRange(endTime, tr.endTime)
			}
		}
	}
	return tableFileMap

}


func (s *kv) isLeftPart(startTime,st,endTime,dataBaseDir,tableName string ) (bool,string) {
	//数据落在st-et的左半部分
	var fileName string
	if strings.Compare(startTime,st) < 0 && strings.Compare(endTime,st) <0 {
		startTime,_:=strconv.ParseInt(st, 10, 64)
		fileName=tableName +"-"+strconv.FormatInt(startTime - s.duration.Nanoseconds(),10)+"-"+st+".db"
		tableFile:=dataBaseDir+ sep + fileName
		ok:=utils.CheckFileIsExist(tableFile)
		if !ok {
			db:=openDB(tableFile)
			db.Close()
		}
		return true,fileName
	}
	return false,fileName
}




func (s *kv) isRightPart(startTime,endTime,et,dataBaseDir,tableName string ) (bool,string) {
	//数据落在st-et的右半部分
	var fileName string
	if strings.Compare(startTime,et) > 0 && strings.Compare(endTime,et) > 0 {
		endTime,_:=strconv.ParseInt(et, 10, 64)
		fileName=tableName+"-"+et+"-"+strconv.FormatInt(endTime + s.duration.Nanoseconds(),10)+".db"
		tableFile:=dataBaseDir+ sep + fileName
		ok:=utils.CheckFileIsExist(tableFile)
		if !ok {
			db:=openDB(tableFile)
			db.Close()
		}
		return true,fileName
	}
	return false,fileName
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
