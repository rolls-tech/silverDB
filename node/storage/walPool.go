package storage

import (
	"github.com/bwmarrin/snowflake"
	"log"
	"silver/config"
	"silver/node/point"
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
	dataList        []*walData
	currentListNum  int
}

type walData struct {
	databaseName string
	tableName string
	tagKv string
	sequenceId int64
	timestamp int64
	operate string
	data []byte
	size int
}

type walNodeLinked struct {
	head *walNode
	currentNodeNums int
}

type WalBuffer struct {
	mu sync.Mutex
	buffer map[string]*walNodeLinked
	ttl time.Duration
	flushCount int
	nodeNums int
	listNums int
	nodeId int64
	*Wal
}


func newWalDataList()  *walDataList {
	return &walDataList{
		dataList:   make([]*walData,0),
		currentListNum:   0,
	}
}


func newWalNode() *walNode {
	return  &walNode {
		mutex:      sync.RWMutex{},
		next:       nil,
		created:    time.Now(),
		wd:   newWalDataList(),
	}

}

func initWalNodeLinked() *walNodeLinked {
	wl:=&walNodeLinked {
		head: newWalNode(),
		currentNodeNums:  0,
	}
	wl.head.next=nil
	return wl
}



func newWalData(databaseName,tableName,tagKv string,data []byte,dataLen int,timestamp int64,nodeId int64) *walData {
	n,e:=snowflake.NewNode(nodeId)
    if e !=nil {
    	log.Println("generate wal id failed !",e)
    	return nil
	}
    id:=n.Generate().Int64()
	return &walData{
		sequenceId: id,
		timestamp:  timestamp,
		operate:    "s",
		data:       data,
		size:       dataLen,
		tagKv: tagKv,
		databaseName: databaseName,
		tableName:tableName,
	}
}


func NewWalBuffer(config config.NodeConfig)  *WalBuffer {
	wb:=&WalBuffer{
		mu:  sync.Mutex{},
		buffer: make(map[string]*walNodeLinked,0),
		ttl: time.Duration(config.Wal.TTL) * time.Second,
		flushCount: 0,
		nodeNums:config.Wal.NodeNums,
		listNums:config.Wal.ListNums,
		Wal:NewWal(config),
		nodeId:config.NodeId,
    }
	go wb.flush()
	return wb
}



func (wb *WalBuffer) flush() {
	for {
		time.Sleep(wb.ttl)
		if wb.buffer != nil && len(wb.buffer) != 0 {
			for _,wn:=range wb.buffer {
				prev:=wn.head
				for prev.next != nil {
				   current:= prev.next
				   if current.created.Add(wb.ttl).Before(time.Now()) {
				   	     for _,data:=range current.wd.dataList {
				   	   	   	if data != nil {
							   e:=wb.writeData(data)
							   if  e != nil {
								 log.Println("write wal data failed",e)
							   }
							   current.wd.dataList=current.wd.dataList[1:]
							   current.wd.currentListNum=current.wd.currentListNum-1
							   }
						   }
				   }
				   //node回收
				  /* if current.wd.currentListNum == 0 && len(current.wd.dataList) == 0 {
				   	      prev.next=current.next
					      return
				   }*/
				   prev=prev.next
				}
			}
		}
	}
}



func(wb *WalBuffer) WriteData(wp *point.WritePoint,tagKv string,data []byte,dataLen int,timestamp,id int64) {
    node:=wb.getWalNode(wp.DataBase,wp.TableName)
	node.wd.dataList=append(node.wd.dataList,newWalData(wp.DataBase,wp.TableName,tagKv,data,dataLen,timestamp,wb.nodeId))
	node.wd.currentListNum=node.wd.currentListNum+1
}


func (wb *WalBuffer) getWalNode(dataBase,tableName string) *walNode {
	var node *walNode
	var newNode *walNode
	wb.mutex.RLock()
	wn,ok:=wb.buffer[dataBase+tableName]
	wb.mutex.RUnlock()
    if ok {
        node=wb.sequenceTraversal(wn)
        if node.wd.currentListNum < wb.listNums {
			return node
		}
        newNode=newWalNode()
        wn.appendLinkedNode(newNode)
		node=wb.sequenceTraversal(wn)
        return node
	}
    wn=initWalNodeLinked()
    newNode=newWalNode()
    wn.appendLinkedNode(newNode)
	node=wb.sequenceTraversal(wn)
	wb.mutex.Lock()
	wb.buffer[dataBase+tableName]=wn
	wb.mutex.Unlock()
	return node
}


func (wb *WalBuffer) sequenceTraversal(wn *walNodeLinked) *walNode {
	current:=wn.head
	for current.next != nil {
		current=current.next
	}
	return current
}


func (wd *walNodeLinked) appendLinkedNode(node *walNode) {
	if wd.head.next == nil {
		wd.head.next=node
		wd.currentNodeNums=wd.currentNodeNums+1
		return
	}
	current:=wd.head
	for current.next != nil {
		current=current.next
	}
	current.next=node
	node.next=nil
	wd.currentNodeNums=wd.currentNodeNums+1
}

