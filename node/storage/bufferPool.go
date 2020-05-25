package storage

import (
	"github.com/golang/protobuf/proto"
	"log"
	"silver/config"
	"silver/metastore"
	"silver/node/point"
	"silver/utils"
	"sort"
	"sync"
	"time"
)

type dataNode struct {
	mutex   sync.RWMutex
	metrics []*metricData
	currentListNums int
	created time.Time
	count   int
	maxTime int64
	minTime int64
	next    *dataNode
	size    int
}

type metricData struct {
	metric  string
	points  []utils.Point
	maxTime int64
	minTime int64
	count   int
}

type dataNodeLinked struct {
	head      *dataNode
	maxTime   int64
	minTime   int64
	dataBase  string
	tableName string
	tagKv     string
	tags      map[string]string
	count     int
	size      int
	currentNodeNums int
}

type DataBuffer struct {
	mutex        sync.RWMutex
	buffer       map[string]*dataNodeLinked
	size         int
	maxSize      int64
	count        int
	snapshot     *DataBuffer
	snapshotSize uint64
	snapshotting bool
	lastSnapshot time.Time
	ttl          time.Duration
	flushCount   int
	nodeNums int
	listNums int
	*kv
	listener *metastore.Listener
	register *metastore.Register
}

func newDataNode() *dataNode {
	return &dataNode{
		mutex:   sync.RWMutex{},
		metrics: nil,
		created: time.Now(),
		count:   0,
		maxTime: 0,
		minTime: 0,
		next:    nil,
	}
}

func newMetricData(metric string, points []utils.Point) *metricData {
	return &metricData{
		metric: metric,
		points: points,
	}
}

func initDataNodeLinked(dataBase, tableName, tagKv string, tags map[string]string) *dataNodeLinked {
	return &dataNodeLinked{
		head:     newDataNode(),
		currentNodeNums:   0,
		maxTime:   0,
		minTime:   0,
		dataBase:  dataBase,
		tableName: tableName,
		tags:      tags,
		tagKv:     tagKv,
		count:     0,
	}
}

func (b *DataBuffer) sequenceTraversal(dn *dataNodeLinked) *dataNode {
	current := dn.head
	for current.next != nil {
		current = current.next
	}
	return current
}

func (b *DataBuffer) WriteData(wp *point.WritePoint, tagKv string) error {
	e := b.writeBuffer(wp, tagKv)
	if e != nil {
		log.Println("write data buffer failed !", e)
		return e
	}
	_, ok := b.listener.LocalMeta[wp.DataBase+wp.TableName]
		if !ok {
		  e= b.register.PutMata(wp.DataBase, wp.TableName)
			if e != nil {
				log.Println("update meta data failed !")
			}
		}
	return e
}


func (b *DataBuffer) ReadData(dataBase,tableName,tagKv,fieldKey string,startTime,endTime int64) map[int64]float64 {
	var value point.Value
	kv:=make(map[int64]float64)
    dataSet:=b.readData(dataBase,tableName,tagKv,fieldKey,startTime,endTime)
	if len(dataSet) != 0 {
		for _,data:=range dataSet {
			if len(data) !=0 {
				_=proto.Unmarshal(data,&value)
				kv=mergeMap(kv,value.Kv)
			}
		}
	}
    return kv
}

// To-do  snapshot read buffer data
func (b *DataBuffer) readBuffer() map[int64]float64 {
	kv:=make(map[int64]float64)
	return kv
}

//To-do encoded SeriesKey
func (b *DataBuffer) encodedSeriesKey() {


}






func (b *DataBuffer) writeBuffer(wp *point.WritePoint, tagKv string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	var e error
	if wp != nil {
		seriesKey := wp.DataBase + wp.TableName + tagKv
		dn, ok := b.buffer[seriesKey]
		var currentNode *dataNode
		if ok {
			node := b.sequenceTraversal(dn)
			if node.currentListNums >= b.listNums {
				node.next = newDataNode()
				currentNode=node.next
			} else {
				currentNode=node
			}
		} else {
			dn = initDataNodeLinked(wp.DataBase, wp.TableName, tagKv, wp.Tags)
			b.buffer[seriesKey] = dn
			currentNode=dn.head
		}
		if wp.Value != nil {
			for key, value := range wp.Value {
				if value.Kv != nil {
					pointKv := utils.NewSortMap(value.Kv)
					sort.Sort(pointKv)
					metric := newMetricData(key,pointKv)
					metric.count = pointKv.Len()
					metric.maxTime = pointKv[pointKv.Len()-1].T
					metric.minTime = pointKv[0].T
					currentNode.count += metric.count
					if currentNode.maxTime < metric.maxTime {
						currentNode.maxTime = metric.maxTime
					}
					if len(currentNode.metrics) == 0 {
						currentNode.minTime=metric.minTime
					} else {
						if currentNode.minTime > metric.minTime {
							currentNode.minTime = metric.minTime
						}
					}
					currentNode.metrics = append(currentNode.metrics, metric)
					currentNode.currentListNums += 1
				}
			}
			if dn.maxTime < currentNode.maxTime {
				dn.maxTime = currentNode.maxTime
			}
			if dn.minTime > currentNode.minTime {
				dn.minTime = currentNode.minTime
			}
			dn.currentNodeNums += 1
			dn.count += currentNode.count
		}
	}
	return e
}

func NewDataBuffer(config config.NodeConfig, listener1 *metastore.Listener, register1 *metastore.Register) *DataBuffer {
	buffer := &DataBuffer{
		mutex:        sync.RWMutex{},
		buffer:       make(map[string]*dataNodeLinked, 0),
		size:         0,
		maxSize:      0,
		count:        0,
		snapshot:     nil,
		snapshotSize: 0,
		snapshotting: false,
		lastSnapshot: time.Time{},
		ttl:          time.Duration(config.Flush.TTL) * time.Second,
		kv:           NewKv(config.DataDir, config.Compressed, 24*time.Hour,config.CompressCount),
		listener:     listener1,
		register:     register1,
	}
	if config.Flush.Count > 0 {
		go buffer.flush(config.Flush.Count)
	}
	return buffer
}

func (b *DataBuffer) flush(flushCount int) {
	for {
		time.Sleep(b.ttl)
		if len(b.buffer) != 0 && b.buffer != nil {
			for seriesKey, dn := range b.buffer {
				if dn.count >= flushCount {
					b.mutex.Lock()
					//current := dn.head
					b.kv.writeDataLinked(dn)

					/*for current != nil {
						if len(current.metrics) > 0 {
							b.kv.writeData(dn.dataBase, dn.tableName, dn.tagKv, dn.tags,current.metrics)
						}
						current = current.next
					}*/
					delete(b.buffer,seriesKey)
					b.mutex.Unlock()
				}
			}
		}
	}
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
