package storage

import (
	"errors"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"os"
	"silver/node/point"
	"silver/utils"
	"sort"
	"strings"
	"sync"
	"time"
)

type indexTags struct {
    tags  map[string]*indexMetric
	count int
}

type indexMetric struct {
	metric map[string]bool
	count int
}

type indexNode struct {
	database string
	table string
	indexTag map[string]*indexTags
	created time.Time
	isChanged bool
	next *indexNode
}

type indexNodeLinked struct {
	head *indexNode
}

type indexKv struct {
	indexDir []string
	mu sync.RWMutex
}


type Index struct {
	mu sync.RWMutex
	writeData map[string]*indexNodeLinked
	readData map[string]*indexNode
	ttl time.Duration
	size int
	count int
	*indexKv
}


func NewIndex(ttl int64,indexDir []string) *Index {
	index:=&Index{
		mu:    sync.RWMutex{},
		readData:  loadIndex(indexDir),
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
		mu:       sync.RWMutex{},
	}
}

func newIndexNode(databaseName,tableName string) *indexNode {
	node:=&indexNode{
		database:    databaseName,
		table:       tableName,
		indexTag:    make(map[string]*indexTags,0),
		created:     time.Now(),
		isChanged:   false,
		next:        nil,
	}
	return node
}

func newIndexTags() *indexTags {
	return &indexTags{
		tags:  make(map[string]*indexMetric),
		count: 0,
	}
}


func newIndexMetric() *indexMetric {
	return &indexMetric{
		metric: make(map[string]bool),
		count:  0,
	}
}


func (n *Index) generateIndexNode(wp *point.WritePoint,sortTagKv string) *indexNode {
	node:=newIndexNode(wp.DataBase,wp.TableName)
	if wp.Tags != nil {
		tagSet := generateTagPrefix(wp.Tags)
		if len(tagSet) > 0 {
			for _, tag := range tagSet {
				indexTags,ok:=node.indexTag[tag]
				if !ok {
					node.indexTag[tag]=newIndexTags()
					indexTags=node.indexTag[tag]
				}
				metrics,ok:=indexTags.tags[sortTagKv]
				if !ok {
					indexTags.tags[sortTagKv]=newIndexMetric()
					metrics=indexTags.tags[sortTagKv]
				}
				if wp.Metric !=nil {
					for key,_:=range wp.Metric {
						metrics.metric[key]=true
					}
				}
			}
		}
	}
	return node
}


func (n *Index) updateIndexData(wp *point.WritePoint,sortTagKv string) error {
	n.mu.RLock()
	node,ok:=n.writeData[wp.DataBase+wp.TableName]
	n.mu.RUnlock()
	if !ok {
		node=&indexNodeLinked{head:newIndexNode(wp.DataBase,wp.TableName)}
		indexNode:=n.generateIndexNode(wp,sortTagKv)
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
	current=n.generateIndexNode(wp,sortTagKv)
	current.created=time.Now()
	return nil
}


func (n *Index) updateMemData(wp *point.WritePoint,sortTagKv string) error {
	node,ok:=n.readData[wp.DataBase+wp.TableName]
	if !ok {
	   	indexNode:=n.generateIndexNode(wp,sortTagKv)
	   	n.mu.Lock()
	   	n.readData[wp.DataBase+wp.TableName]=indexNode
	   	n.mu.Unlock()
	   	return nil
	}
	if wp.Tags != nil {
		tagSet := generateTagPrefix(wp.Tags)
		if len(tagSet) > 0 {
			for _, tag := range tagSet {
				indexTags,ok:=node.indexTag[tag]
				if ok {
					metric,ok:=indexTags.tags[sortTagKv]
					if ok {
						if wp.Metric !=nil {
							for key,_:=range wp.Metric {
								_,ok:=metric.metric[key]
								if !ok {
									metric.metric[key]=true
								}
							}
						}
					} else {
						indexTags.tags[sortTagKv]=newIndexMetric()
						metric,_:=indexTags.tags[sortTagKv]
						if wp.Metric !=nil {
							for key,_:=range wp.Metric {
								metric.metric[key]=true
							}
						}

					}
				} else {
					node.indexTag[tag]=newIndexTags()
					node.indexTag[tag].tags[sortTagKv]=newIndexMetric()
					metric:=node.indexTag[tag].tags[sortTagKv]
					if wp.Metric != nil {
						for key,_:=range wp.Metric {
							metric.metric[key]=true
						}
					}
				}
			}
		}
	}
	return nil
}

func (n *Index) WriteData(wp *point.WritePoint,sortTagKv string) error {
	return n.writeIndex(wp,sortTagKv)
}

func (n *Index) ReadData(rp *point.ReadPoint,tagKv string) (map[string][]string,error) {
	return n.readIndex(rp,tagKv)
}

func (n *Index) readIndex(rp *point.ReadPoint,tagKv string) (map[string][]string,error) {
	tagMetrics:=make(map[string][]string,0)
	node,ok:=n.readData[rp.DataBase+rp.TableName]
	if !ok {
		return tagMetrics,errors.New("non-existed "+rp.DataBase+" and "+rp.TableName)
	}
	if tagKv=="" {
		for _,indexTags:=range node.indexTag {
			for tag,fields:=range indexTags.tags {
				var metrics []string
				if rp.Metrics == nil && fields.metric != nil {
					for metric,_:=range fields.metric {
						metrics=append(metrics,metric)
					}
				}
				if fields.metric != nil && rp.Metrics != nil {
					for metric,_:=range rp.Metrics {
						_,ok:=fields.metric[metric]
						if ok {
							metrics=append(metrics,metric)
						}
					}
				}
				tagMetrics[tag]=metrics
			}
		}
		return tagMetrics,nil
	} else {
	   indexTags,ok:=node.indexTag[tagKv]
	   if !ok {
			return tagMetrics,errors.New("non-existed "+tagKv+ " tagSet ! ")
		}
	   for tag,fields:=range indexTags.tags {
			var metrics []string
			if rp.Metrics == nil && fields.metric != nil {
				for metric,_:=range fields.metric {
					metrics=append(metrics,metric)
				}
			}
			if fields.metric != nil && rp.Metrics != nil {
				for metric,_:=range rp.Metrics {
					_,ok:=fields.metric[metric]
					if ok {
						metrics=append(metrics,metric)
					}
				}
			}
			tagMetrics[tag]=metrics
		}
		return tagMetrics,nil
	}
}


func (n *Index) writeIndex(wp *point.WritePoint,sortTagKv string) error {
	e:=n.updateIndexData(wp,sortTagKv)
	e=n.updateMemData(wp,sortTagKv)
	if e !=nil {
		log.Println("write index data failed !",e)
	}
	return e
}

func (n *Index) writeIndexKv(node *indexNode) error {
	indexFile:=n.indexKv.indexDir[0]+node.database+"_index.db"
	db:=openDB(indexFile)
	defer db.Close()
	if err := db.Batch(func(tx *bolt.Tx) error {
		rootTable, err := tx.CreateBucketIfNotExists([]byte(node.table))
		if err != nil {
			log.Println("create index table failed !", err)
			return err
		}
		if node.indexTag != nil {
			for tag, indexMetrics := range node.indexTag {
				tagTable, err := rootTable.CreateBucketIfNotExists([]byte(tag))
				if err != nil {
					log.Println("create tag index failed !", err)
					return err
				}
				if indexMetrics.tags != nil {
					for tagKv,metric:= range indexMetrics.tags {
					   	if metric.metric != nil {
					   		for k,_:=range metric.metric {
								err = tagTable.Put([]byte(tagKv+k), []byte(k))
								if err != nil {
									log.Println("write index and tag failed ! "+tagKv+" : "+k)
									return err
								}
							}
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


func scanIndexKv(indexFile string,databaseName string,readData map[string]*indexNode) {
	db:=openDB(indexFile)
	defer db.Close()
	if err:= db.View(func(tx *bolt.Tx) error {
        scan:=tx.Cursor()
		for tableName,_:=scan.First(); tableName != nil; tableName,_=scan.Next() {
			   indexNode,ok:=readData[databaseName+string(tableName)]
			   if !ok {
			   	  readData[databaseName+string(tableName)]=newIndexNode(databaseName,string(tableName))
			   	  indexNode=readData[databaseName+string(tableName)]
			   }
			   scanTag:=tx.Bucket(tableName).Cursor()
			   for tag,_:=scanTag.First(); tag !=nil; tag,_=scanTag.Next() {
			   	    tags,ok:=indexNode.indexTag[string(tag)]
			   	    if !ok {
						indexNode.indexTag[string(tag)]=newIndexTags()
						tags=indexNode.indexTag[string(tag)]
			   	    }
			   	    t:=tx.Bucket(tableName).Bucket(tag)
			   	    t.ForEach(func(tagKv,metric []byte) error {
			   	    	key:=string(tagKv)
						n:=strings.LastIndex(key,";")
						v:=key[n+1:]
						k:=key[:n+1]
						metrics,ok:=tags.tags[k]
						if !ok {
							tags.tags[k]=newIndexMetric()
							metrics=tags.tags[k]
						}
						_,ok=metrics.metric[v]
						if !ok {
							metrics.metric[v]=true
						}
                        return nil
					})
			   }
		}
		return nil
	}); err != nil {
		log.Println(err)
	}
}


func (n *Index) flush() {
	for {
		time.Sleep(n.ttl)
		if n.writeData !=nil {
		var wg sync.WaitGroup
		for _,nodeLink:=range n.writeData {
			wg.Add(1)
			go func(nodeLink *indexNodeLinked,wg *sync.WaitGroup) {
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
				wg.Done()
			}(nodeLink,&wg)
		  }
		wg.Wait()
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


func loadIndex (indexDir []string) map[string]*indexNode {
    readData:=make(map[string]*indexNode,0)
	if len(indexDir) > 0 {
		for _,dir:=range indexDir {
		  	ok:=utils.CheckFileIsExist(dir)
		  	if ok {
				fileList,err:=ioutil.ReadDir(dir)
				if err !=nil {
					log.Println(err)
				}
				if len(fileList) > 0 {
					for _,file:=range fileList {
						if strings.HasSuffix(file.Name(),"_index.db") {
							databaseName:=strings.Split(file.Name(),"_")[0]
							scanIndexKv(dir+file.Name(),databaseName,readData)
						}
					}
				}
			}
		}
	}
	return readData
}


// To-do index key
func encodedIndex() {


}



















































