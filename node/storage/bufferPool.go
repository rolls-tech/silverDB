package storage

import (
	"crypto/md5"
	"github.com/golang/protobuf/proto"
	"log"
	"silver/config"
	"silver/metastore"
	"silver/node/point"
	"silver/utils"
	"sync"
	"time"
)

type dataNode struct {
	mutex           sync.RWMutex
	metrics         []*metricData
	fields          map[string]*point.Metric
	currentListNums int
	created         time.Time
	count           int
	maxTime         int64
	minTime         int64
	next            *dataNode
	size            int
}

type metricData struct {
	metric     string
	points     []utils.Point
	metricType int32
	maxTime    int64
	minTime    int64
	count      int
	minValue   []byte
	maxValue   []byte
	precision  int32
}

type dataNodeLinked struct {
	head            *dataNode
	maxTime         int64
	minTime         int64
	dataBase        string
	tableName       string
	tagKv           string
	tags            map[string]string
	count           int
	size            int
	currentNodeNums int
	created         time.Time
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
	nodeNums     int
	listNums     int
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

func newMetricData(metric string, points []utils.Point, metricType, precision int32) *metricData {
	return &metricData{
		metric:     metric,
		points:     points,
		metricType: metricType,
		precision:  precision,
	}
}

func initDataNodeLinked(dataBase, tableName, tagKv string, tags map[string]string) *dataNodeLinked {
	return &dataNodeLinked{
		head:            newDataNode(),
		currentNodeNums: 0,
		maxTime:         0,
		minTime:         0,
		dataBase:        dataBase,
		tableName:       tableName,
		tags:            tags,
		tagKv:           tagKv,
		count:           0,
		created:         time.Now(),
	}
}

func (b *DataBuffer) sequenceTraversal(dn *dataNodeLinked) *dataNode {
	current := dn.head
	for current.next != nil {
		current = current.next
	}
	node := newDataNode()
	current.next = node
	node.next = nil
	dn.currentNodeNums = dn.currentNodeNums + 1
	return node
}

func (b *DataBuffer) WriteData(wp *point.WritePoint, tagKv string) error {
	e := b.writeBuffer(wp, tagKv)
	if e != nil {
		log.Println("write data buffer failed !", e)
		return e
	}
	_, ok := b.listener.LocalMeta[wp.DataBase+wp.TableName]
	if !ok {
		e = b.register.PutMata(wp.DataBase, wp.TableName)
		if e != nil {
			log.Println("update meta data failed !")
		}
	}
	return e
}

func (b *DataBuffer) ReadData(dataBase, tableName, tagKv, fieldKey string, startTime, endTime int64) (map[int64][]byte, int32) {
	var metric point.Metric
	kv := make(map[int64][]byte)
	dataSet := b.readData(dataBase, tableName, tagKv, fieldKey, startTime, endTime)
	if len(dataSet) > 0 {
		for _, data := range dataSet {
			if len(data) != 0 {
				_ = proto.Unmarshal(data, &metric)
				kv = mergeMap(kv, metric.Metric)
			}
		}
	}
	return kv, metric.MetricType
}

// To-do  snapshot read buffer data
func (b *DataBuffer) readBuffer() map[int64]float64 {
	kv := make(map[int64]float64)
	return kv
}

// To-do encoded SeriesKey
func (b *DataBuffer) encodedSeriesKey(seriesKey string) string {
	if len(seriesKey) > 0 {
		md := md5.New()
		md.Write([]byte(seriesKey))
		result := md.Sum([]byte(""))
		return string(result)
	}
	return ""
}

func (b *DataBuffer) writeBuffer(wp *point.WritePoint, tagKv string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	var e error
	if wp != nil {
		seriesKey := wp.DataBase + wp.TableName + tagKv
		//这里计算seriesKey的md5值
		mdSeriesKey := b.encodedSeriesKey(seriesKey)
		if len(mdSeriesKey) <= 0 {
			return e
		}
		//在buffer中的数据结构基本和wal中的类似
		dn, ok := b.buffer[mdSeriesKey]
		var currentNode *dataNode
		if ok {
			//这里只需要找一个空的节点就可以
			node := b.sequenceTraversal(dn)
			currentNode = node
		} else {
			dn = initDataNodeLinked(wp.DataBase, wp.TableName, tagKv, wp.Tags)
			b.buffer[mdSeriesKey] = dn
			currentNode = newDataNode()
			dn.head.next = currentNode
			dn.currentNodeNums += 1
		}
		if wp.Metric != nil {
			for _, value := range wp.Metric {
				if value.Metric != nil {
					currentNode.count += len(value.Metric)
					//其实在这里做排序这样的操作，似乎是没有任何意义的
					/*
						pointKv := utils.NewSortMap(value.Metric)
						sort.Sort(pointKv)
						metric := newMetricData(key,pointKv,value.MetricType,wp.TimePrecision)
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
					*/
				}
			}
			/*if dn.maxTime < currentNode.maxTime {
				dn.maxTime = currentNode.maxTime
			}
			if dn.minTime > currentNode.minTime {
				dn.minTime = currentNode.minTime
			}*/
			currentNode.fields = wp.Metric

			//这里主要是为了说明，数据条数的增加
			dn.count += currentNode.count / len(currentNode.fields)
			b.count += dn.count
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
		kv:           NewKv(config.DataDir, config.Compressed, 24*time.Hour, config.CompressCount),
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
			if b.count >= flushCount {
				for seriesKey, dn := range b.buffer {
					//如果对于某个sk，缓存的数据已经大于涮写的数据大小.
					b.mutex.Lock()
					/*if dn.count >= flushCount {
						b.kv.writeDataLinked(dn)
						delete(b.buffer,seriesKey)
						b.count-=dn.count
					} else */
					if dn.created.Add(b.ttl).Before(time.Now()) {
						b.kv.writeDataLinked(dn)
						delete(b.buffer, seriesKey)
						b.count -= dn.count
						//}

					}
					b.mutex.Unlock()

				}

			}
		}
	}
}

func mergeMap(bv map[int64][]byte, sv map[int64][]byte) map[int64][]byte {
	kv := make(map[int64][]byte, 0)
	for k, v := range sv {
		kv[k] = v
	}
	for k, v := range bv {
		kv[k] = v
	}
	return kv
}
