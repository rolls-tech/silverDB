package storage

import (
	"log"
	"silverDB/config"
	"silverDB/node/point"
	"sync"
	"time"
)

type walNode struct {
	mutex     sync.RWMutex
	next *walNode
	created time.Time
	wd *walDataList
}

type walDataList struct {
	dataList []*walData
	listNum int
	currentNum int

}

type walData struct {
	wp *point.WritePoint
	tagKv string
	sequenceId int64
	timestamp int64
	operate string
	data []byte
	size int
}

type walNodeLinked struct {
	head *walNode
	num int
	ttl time.Duration
	flushCount int
	listNum int
}

type WalBuffer struct {
	mutex sync.Mutex
	buffer map[string]*walNodeLinked
	ttl time.Duration
	flushCount int
	listNum int
	*Wal
}


func newWalDataList(listNum int)  *walDataList {
	var num int
	if listNum !=0 {
		num = listNum
	}else {
		num=20
	}
	return &walDataList{
		dataList:   make([]*walData,num),
		listNum:    num,
		currentNum: 0,
	}
}


func newWalNode(listNum int,created time.Time) *walNode {
	return  &walNode {
		mutex:      sync.RWMutex{},
		next:       nil,
		created:    created,
		wd:   newWalDataList(listNum),
	}

}

func initWalNodeLinked() *walNodeLinked {
	created:=time.Now()
	wl:=&walNodeLinked {
		head: newWalNode(0,created),
		num:  0,
	}
	wl.head.next=nil
	return wl
}



func newWalData(wp *point.WritePoint,tagKv string,data []byte,dataLen int,timestamp int64,id int64) *walData {
	return &walData{
		sequenceId: id,
		timestamp:  timestamp,
		operate:    "s",
		data:       data,
		size:       dataLen,
		wp: wp,
		tagKv: tagKv,
	}
}


func NewWalBuffer(config config.NodeConfig)  *WalBuffer {
	wb:=&WalBuffer{
		mutex:  sync.Mutex{},
		buffer: make(map[string]*walNodeLinked,0),
		ttl: time.Duration(config.Wal.TTL) * time.Second,
		flushCount: 0,
		listNum: config.Wal.Nums,
		Wal:NewWal(config.Wal.WalData,
			config.NodeData,
			config.IndexData,
			config.Compressed,
			config.Flush.Timeout,
			config.Wal.Size,
			config.Flush.Count),
    }
	go wb.flush()
	return wb
}



func (wb *WalBuffer) flush() {
	for {
		time.Sleep(wb.ttl)
		if wb.buffer != nil {
			for _,wn:=range wb.buffer {
				current:=wn.head
				if current.next == nil {
					return
				}
				for current.next != nil {
				   current=current.next
				   if current.created.Add(wb.ttl).Before(time.Now()) {
				   	   if current.wd.currentNum > 0 {
				   	   	   for _,data:=range current.wd.dataList {
				   	   	   	   if data !=nil {
								   e:=wb.writeData(data)
								   if  e != nil {
									   log.Println("write wal data failed",e)
									   return
								   }
								   current.wd.dataList=current.wd.dataList[1:]
								   current.wd.currentNum=current.wd.currentNum-1
							   }
						   }
					   }
				   }
				}
			}
		}
	}
}



func(wb *WalBuffer) WriteData(wp *point.WritePoint,tagKv string,data []byte,dataLen int,timestamp,id int64) error {
    node:=wb.getWalNode(wp.DataBase,wp.TableName)
	node.wd.dataList=append(node.wd.dataList,newWalData(wp,tagKv,data,dataLen,timestamp,id))
	node.wd.currentNum=node.wd.currentNum+1
	return nil
}


func (wb *WalBuffer) getWalNode(dataBase,tableName string) *walNode {
	var node *walNode
	var newNode *walNode
	wb.mu.Lock()
	wn,ok:=wb.buffer[dataBase+tableName]
	wb.mu.Unlock()
	created:=time.Now()
    if ok {
        node=wb.sequenceTraversal(wn)
        if node.wd.currentNum < wb.listNum {
			return node
		}
        newNode=newWalNode(wb.listNum,created)
        wn.appendLinkedNode(newNode)
		node=wb.sequenceTraversal(wn)
        return node
	}
    wn=initWalNodeLinked()
    newNode=newWalNode(wb.listNum,created)
    wn.appendLinkedNode(newNode)
	node=wb.sequenceTraversal(wn)
	wb.mutex.Lock()
	wb.buffer[dataBase+tableName]=wn
	wb.mutex.Unlock()
	return node
}


func (wb *WalBuffer) sequenceTraversal(wn *walNodeLinked) *walNode {
	current:=wn.head
	if current.next == nil {
		created:=time.Now()
		current.next=newWalNode(wb.listNum,created)
		return current.next
	}
	for current.next != nil {
		current=current.next
	}
	return current
}


func (wd *walNodeLinked) appendLinkedNode(node *walNode) {
	if wd.head.next == nil {
		wd.head.next=node
		wd.num=wd.num+1
	}
	current:=wd.head
	for current.next != nil {
		current=current.next
	}
	current.next=node
	node.next=nil
	wd.num=wd.num+1
}

