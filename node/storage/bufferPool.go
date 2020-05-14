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
	nodeNum   int
	maxTime   int64
	minTime   int64
	dataBase  string
	tableName string
	tagKv     string
	tags      map[string]string
	count     int
	size      int
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
	*kv
	//*index
	listener *metastore.Listener
	register *metastore.Register
}

func newDataNode(created time.Time) *dataNode {
	return &dataNode{
		mutex:   sync.RWMutex{},
		metrics: nil,
		created: created,
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
		head:      nil,
		nodeNum:   0,
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
	if current.next == nil {
		created := time.Now()
		current.next = newDataNode(created)
		return current.next
	}
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
		  e= b.register.PutNode(wp.DataBase, wp.TableName)
			if e != nil {
				log.Println("update meta data failed !")
			}
		}
	return e
}


func (b *DataBuffer) ReadData(dataBase,tableName,tagKv,fieldKey string,startTime,endTime int64) map[int64]float64 {
	var value point.Value
	kv:=make(map[int64]float64)
    dataSet:=b.readTsData(dataBase,tableName,tagKv,fieldKey,startTime,endTime)
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


/*func (b *DataBuffer) readBuffer() error {

}*/


/*func (b *DataBuffer) writeData(wp *point.WritePoint, tagKv string, walCh chan bool) {
	ok := <-walCh
	if ok {
		go func(wp *point.WritePoint, tagKv string) {
			e := b.writeBuffer(wp, tagKv)
			if e != nil {
				log.Println("write data buffer failed !", e)
			}
		}(wp, tagKv)
		go func(wp *point.WritePoint, tagKv string) {
			e := b.index.writeIndex(wp, tagKv)
			if e != nil {
				log.Println("write index data failed !", e)
			}
		}(wp, tagKv)
		go func(wp *point.WritePoint) {
			_, ok := b.listener.LocalMeta[wp.DataBase+wp.TableName]
			if !ok {
				e := b.register.PutNode(wp.DataBase, wp.TableName)
				if e != nil {
					log.Println("update meta data failed !")
				}
			}
		}(wp)
	}
}*/



func (b *DataBuffer) writeBuffer(wp *point.WritePoint, tagKv string) error {
	b.mutex.Lock()
	b.mutex.Unlock()
	var e error
	if wp != nil {
		seriesKey := wp.DataBase + wp.TableName + tagKv
		dn, ok := b.buffer[seriesKey]
		var node *dataNode
		if ok {
			node = b.sequenceTraversal(dn)
			node.created=time.Now()
		} else {
			created := time.Now()
			dn = initDataNodeLinked(wp.DataBase, wp.TableName, tagKv, wp.Tags)
			dn.head = newDataNode(created)
			dn.head.next = newDataNode(created)
			node = dn.head.next
			b.buffer[seriesKey] = dn
		}
		if wp.Value != nil {
			metrics := make([]*metricData, 0)
			for key, value := range wp.Value {
				if value.Kv != nil {
					sortKv := utils.NewSortMap(value.Kv)
					sort.Sort(sortKv)
					metric := newMetricData(key, sortKv)
					metric.count = sortKv.Len()
					metric.maxTime = sortKv[sortKv.Len()-1].T
					metric.minTime = sortKv[0].T
					node.count += metric.count
					if node.maxTime < metric.maxTime {
						node.maxTime = metric.maxTime
					}
					if node.minTime > metric.minTime {
						node.minTime = metric.minTime
					}
					metrics = append(metrics, metric)
				}
			}
			node.metrics = metrics
			if dn.maxTime < node.maxTime {
				dn.maxTime = node.maxTime
			}
			if dn.minTime > node.minTime {
				dn.minTime = node.minTime
			}
			dn.nodeNum += 1
			dn.count += node.count
			//dn.size+=len(data) / 1024
			b.count += dn.count
			//b.size+= len(data) /1024
		}
		return nil
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
		kv:           NewKv(config.DataDir, config.Compressed, 24*time.Hour),
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
		if len(b.buffer) != 0 && b.snapshotting == false {
		/*	b.snapshot = b
			b.snapshotting=true
			b.lastSnapshot = time.Now()
			b.snapshot.mutex.Lock()
			temp:=b.snapshot.buffer*/
			for seriesKey, dn := range b.buffer {
				if dn.count >= flushCount {
					current := dn.head
					for current.next != nil {
						for _, metric := range current.next.metrics {
							b.kv.writeData(dn.dataBase, dn.tableName, dn.tagKv, dn.tags, metric)
						}
						current = current.next
					}
					b.mutex.Lock()
					delete(b.buffer, seriesKey)
					b.count -= dn.count
					b.size -= dn.size
					b.mutex.Unlock()
				}
				/*if dn.head != nil {
					current:=dn.head
					for current.next != nil {
					   if current.next.created.Add(b.ttl).Before(time.Now()) {
						   for _, metric := range current.next.metrics {
							   b.snapshot.kv.writeData(dn.dataBase, dn.tableName, dn.tagKv, dn.tags, metric)
						   }
						   b.count -= current.next.count
						   b.size -= current.size
					    }
					    current = current.next
						b.count -= dn.count
						b.size -= dn.size
					}
				}*/
			}
			/*b.snapshotting = false
			b.snapshot.mutex.Unlock()*/

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
