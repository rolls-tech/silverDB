package metastore

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"log"
	"strings"
	"sync"
	"time"
)

type Discovery struct {
	client *clientv3.Client
	ServerList map[string]map[string]bool
	metaPath string
	mu sync.Mutex
}

func NewDiscovery(metaAddr []string,timeout int64,metaPath string) (*Discovery,error) {
	conf :=clientv3.Config{
		Endpoints:   metaAddr,
		DialTimeout: time.Duration(timeout) * time.Second,
	}

	if c,err := clientv3.New(conf);err==nil {
		return &Discovery {
			client: c,
			ServerList:make(map[string]map[string]bool,0),
			metaPath:metaPath,
		}, nil
	}else {
		return nil,err
	}
}

func (dc *Discovery) MetaDataService() error {
	response, err := dc.client.Get(context.Background(), dc.metaPath, clientv3.WithPrefix())
	if err != nil{
		return err
	}

	dc.extractAddr(response)

	go dc.watcher(dc.metaPath)

	return nil

}

func (dc *Discovery) watcher(prefix string){
	watch := dc.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wasp := range watch {
		for _ ,ev:= range wasp.Events{
			switch ev.Type{
			case mvccpb.PUT:
				dc.parseMetaData(string(ev.Kv.Key))
			case mvccpb.DELETE:
				dc.deleteServiceList(string(ev.Kv.Key))
			}
		}
	}

}

func (dc *Discovery) extractAddr(response *clientv3.GetResponse) {
	if response != nil && response.Kvs != nil {
		for i := range response.Kvs {
			if k :=response.Kvs[i].Key; k != nil {
				dc.parseMetaData(string(response.Kvs[i].Key))
			}
		}
	}
}

func (dc *Discovery) deleteServiceList(key string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	delete(dc.ServerList,key)
	log.Println("delete data key :",key)
}

func (dc *Discovery) parseMetaData(key string) {
	dataList:=strings.Split(key,"/")
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.ServerList[dataList[3]+dataList[4]] != nil {
		ipMap:=dc.ServerList[dataList[3]+dataList[4]]
		ipMap[dataList[5]]=true
	}else {
		ipMap:=make(map[string]bool)
		ipMap[dataList[5]]=true
		dc.ServerList[dataList[3]+dataList[4]]=ipMap
	}
	log.Println("set metaData: ",dc.ServerList)
}