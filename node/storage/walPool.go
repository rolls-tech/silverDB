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
	created time.Time
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



func newWalData(data []byte,nodeId int64) *walData {
	n,e:=snowflake.NewNode(nodeId)
    if e !=nil {
    	log.Println("generate wal id failed !",e)
    	return nil
	}
    id:=n.Generate().Int64()
	return &walData {
		sequenceId: id,
		operate:    "s",
		data:       data,
		created: time.Now(),
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
				         //遍历节点之中的list
				   	     for _,data:=range current.wd.dataList {
				   	   	   	if data != nil {
				   	   	   		//如果，该list中的data数据创建时间超过配置的过期时间，则进行刷写
				   	   	   	   if data.created.Add(wb.ttl).Before(time.Now()) {
				   	   	   	   	   //持久化到磁盘
								   e:=wb.writeData(data)
								   if  e != nil {
									   log.Println("write wal data failed",e)
								   }
								   //更新list
								   current.wd.dataList=current.wd.dataList[1:]
								   //更新list大小
								   current.wd.currentListNum=current.wd.currentListNum-1
							   }
				   	   	   	}
				   	     }
				   // 如果当前node的list大小为空，则需要回收当前current node
				   if current.wd.currentListNum == 0 && len(current.wd.dataList) == 0 {
				   	      prev.next=current.next
					      return
				   }
				}
			}
		}
	}
}



func(wb *WalBuffer) WriteData(wp *point.WritePoint,tagKv string,data []byte) {
    //获取一个符合条件的节点，以便将数据条件到该节点的list中。
	node:=wb.getWalNode(wp.DataBase,wp.TableName)
	node.wd.dataList=append(node.wd.dataList,newWalData(data,wb.nodeId))
	node.wd.currentListNum=node.wd.currentListNum+1
}


func (wb *WalBuffer) getWalNode(dataBase,tableName string) *walNode {
	var node *walNode
	var newNode *walNode
	wb.mutex.RLock()
	wn,ok:=wb.buffer[dataBase+tableName]
	wb.mutex.RUnlock()
    if ok {
    	//找到一个node节点，这里默认从头节点开始遍历.
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
		//如果当前节点不为空，且当前节点中walData list大小未超过已经配置的list大小，则返回该节点。
		//否则，继续寻找下一个节点，知道下一个节点为空，则退出循环。
		if current.wd.currentListNum < wb.listNums {
			return current
		}
	}
	// 新建一个节点，添加到莲表尾部
	current=newWalNode()
	wn.appendLinkedNode(current)
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

