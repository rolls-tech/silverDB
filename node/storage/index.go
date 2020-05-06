package storage

import (
	"github.com/boltdb/bolt"
	"log"
	"math/rand"
	"os"
	"silverDB/node/point"
	"silverDB/utils"
	"sort"
	"sync"
	"time"
)

type indexNode struct {
	database string
	table string
	indexTag map[string]string
	metrics map[string]bool
	tagsCount int
	metricCount int
	created time.Time
	isChanged bool
	next *indexNode
}

type indexNodeLinked struct {
	head *indexNode
}

type indexKv struct {
	indexDir []string
	mu sync.Mutex
}


type index struct {
	mu sync.Mutex
	writeData map[string]*indexNodeLinked
	readData map[string]*indexNode
	ttl time.Duration
	size int
	count int
	*indexKv
}


func NewIndex(ttl int64,indexDir []string) *index {
	index:=&index{
		mu:    sync.Mutex{},
		readData:  make(map[string]*indexNode,0),
		writeData: make(map[string]*indexNodeLinked,0),
		ttl:   time.Duration(ttl) * time.Second,
		size:  0,
		count: 0,
		indexKv: newIndexKv(indexDir),
	}
	go index.flush()
	return index
}

func newIndexKv(indexDir []string) *indexKv {
	if len(indexDir) > 0 {
		for _,dir:=range indexDir {
			ok:=utils.CheckFileIsExist(dir)
			if !ok {
				e:=os.MkdirAll(dir,os.ModePerm)
				if e !=nil {
					log.Println("failed create index dir",dir,e)
				}
			}
		}
	}
	return &indexKv{
		indexDir: indexDir,
		mu:       sync.Mutex{},
	}
}

func newIndexNode(wp *point.WritePoint) *indexNode{
	node:=&indexNode{
		database:    wp.DataBase,
		table:       wp.TableName,
		indexTag:    make(map[string]string,0),
		metrics:     make(map[string]bool,0),
		tagsCount:   0,
		metricCount: 0,
		created:     time.Now(),
		isChanged:   false,
		next:        nil,
	}
	return node
}


func (n *index) generateIndexNode(wp *point.WritePoint) *indexNode {
	node:=newIndexNode(wp)
	var tagKv string
	if wp.Tags != nil {
		st := utils.NewSortTags(wp.Tags)
		sort.Sort(st)
		for _, tags := range st {
			tagKv += tags.TagK + tags.TagV
		}
		tagSet := generateTagPrefix(wp.Tags)
		if len(tagSet) > 0 {
			for _, tag := range tagSet {
				node.indexTag[tag] = tagKv
			}
		}
	}
    if wp.Value !=nil {
    	for metric,_:=range wp.Value {
    		node.metrics[metric]=true
		}
	}
	return node
}



func (n *index) updateIndexData(wp *point.WritePoint,sortTagKv string) error {
	node,ok:=n.writeData[wp.DataBase+wp.TableName]
	if !ok {
		node=&indexNodeLinked{head:newIndexNode(wp)}
		indexNode:=n.generateIndexNode(wp)
		n.mu.Lock()
		indexNode.created=time.Now()
		node.head.next=indexNode
		n.writeData[wp.DataBase+wp.TableName]=node
		n.mu.Unlock()
		return nil
	}
	current:=node.head
	for current.next !=nil {
	   current=current.next
	}
	current=n.generateIndexNode(wp)
	current.created=time.Now()
	return nil
}


func (n *index) updateMemData(wp *point.WritePoint,sortTagKv string) error {
	node,ok:=n.readData[wp.DataBase+wp.TableName]
	if !ok {
	   	indexNode:=n.generateIndexNode(wp)
	   	n.mu.Lock()
	   	n.readData[wp.DataBase+wp.TableName]=indexNode
	   	n.mu.Unlock()
	   	return nil
	}
	if node.indexTag !=nil {
		_,ok:=node.indexTag[sortTagKv]
		if !ok {
		  if wp.Tags !=nil {
			  tagSet := generateTagPrefix(wp.Tags)
			  if len(tagSet) > 0 {
				  for _, tag := range tagSet {
					  n.mu.Lock()
					  node.indexTag[tag] = sortTagKv
					  n.mu.Unlock()
				  }
			  }
		  }
		}
	}
	if node.metrics != nil {
		if wp.Value != nil {
			for metric,_:=range wp.Value {
				n.mu.Lock()
				node.metrics[metric]=true
				n.mu.Unlock()
			}
		}
	}
	return nil
}


func (n *index) writeIndex(wp *point.WritePoint,sortTagKv string) error {
	e:=n.updateIndexData(wp,sortTagKv)
	e=n.updateMemData(wp,sortTagKv)
	if e !=nil {
		log.Println("write index data failed !",e)
	}
	return e
}

func (n *index) writeIndexKv(node *indexNode) error {
	rand.Seed(time.Now().UnixNano())
	i:= rand.Intn(len(n.indexKv.indexDir))
	indexFile:=n.indexKv.indexDir[i]+node.database+"_index.db"
	ok:=utils.CheckFileIsExist(indexFile)
	if !ok {
		db:=openDB(indexFile)
		e:=db.Close()
		if e!=nil {
			log.Println("create index File failed !",indexFile,e)
		}
	}
	db:=openDB(indexFile)
	defer db.Close()
	if err := db.Batch(func(tx *bolt.Tx) error {
		rootTable, err := tx.CreateBucketIfNotExists([]byte(node.table))
		if err != nil {
			log.Println("create index table failed !", err)
			return err
		}
		if node.indexTag != nil {
			for tag, tagKv := range node.indexTag {
				tagTable, err := rootTable.CreateBucketIfNotExists([]byte(tag))
				if err != nil {
					log.Println("create tag index failed !", err)
					return err
				}
				if node.metrics != nil {
					for metric, _ := range node.metrics {
						err = tagTable.Put([]byte(tagKv), []byte(metric))
						if err != nil {
							return err
						}
					}
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


func (n *index) flush() {
	for {
		time.Sleep(n.ttl)
		if n.writeData !=nil {
		for _,nodeLink:=range n.writeData {
			go func(nodeLink *indexNodeLinked) {
				current:=nodeLink.head
				head:=nodeLink.head
				current=head.next
				for current !=nil {
				  if current.created.Add(n.ttl).Before(time.Now()) && !current.isChanged {
					  e:=n.writeIndexKv(current)
					  if e !=nil {
						  log.Println("write index kv failed! ",e)
					  }
					  current.isChanged=true
					  current=current.next
					  head.next=current
				  }
				}
			}(nodeLink)
		  }
		}
	}
}


func generateTagPrefix(tags map[string]string) []string {
	var ts []string
	var tagSet []string
	if tags != nil {
		for tagK,tagV:=range tags {
			ts=append(ts,tagK+tagV)
		}
	}
	sort.Strings(ts)
	tsLen:= len(ts)
	for i := 1; i <= tsLen; i++ {
		tagSet=combine(0,i,ts,"",tagSet)
	}
	return tagSet
}


func combine (index,n int,ts []string,temp string,tagSet []string) []string {
	if n==1 {
		for i:=index; i< len(ts); i++ {
			a:=temp+ts[i]
			tagSet=append(tagSet,a)
		}
	} else {
		for i:=index; i< len(ts); i++ {
			b:=temp+ts[i]
			tagSet=combine(i+1,n-1,ts,b,tagSet)
		}
	}
	return tagSet
}
























































