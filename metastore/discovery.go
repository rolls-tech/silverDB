package metastore

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

type Listener struct {
	client *clientv3.Client
	LocalMeta map[string]map[string]bool
	metaPath string
	mu sync.Mutex
}

func NewListener(metaAddr []string,timeout int64,metaPath string) (*Listener,error) {
	conf :=clientv3.Config{
		Endpoints:   metaAddr,
		DialTimeout: time.Duration(timeout) * time.Second,
	}
	if c,err := clientv3.New(conf);err==nil {
		return &Listener {
			client: c,
			LocalMeta:make(map[string]map[string]bool,0),
			metaPath:metaPath,
		}, nil
	}else {
		return nil,err
	}
}

func (dc *Listener) MetaDataService() error {
	response, err := dc.client.Get(context.Background(), dc.metaPath, clientv3.WithPrefix())
	if err != nil{
		return err
	}

	dc.extractAddr(response)

	go dc.watcher(dc.metaPath)

	return nil

}

func (dc *Listener) watcher(prefix string){
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

func (dc *Listener) extractAddr(response *clientv3.GetResponse) {
	if response != nil && response.Kvs != nil {
		for i := range response.Kvs {
			if k :=response.Kvs[i].Key; k != nil {
				dc.parseMetaData(string(response.Kvs[i].Key))
			}
		}
	}
}

func (dc *Listener) deleteServiceList(key string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	delete(dc.LocalMeta,key)
}

func (dc *Listener) parseMetaData(key string) {
	dataList:=strings.Split(key,"/")
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.LocalMeta[dataList[3]+dataList[4]] != nil {
		ipMap:=dc.LocalMeta[dataList[3]+dataList[4]]
		ipMap[dataList[5]]=true
	}else {
		ipMap:=make(map[string]bool)
		ipMap[dataList[5]]=true
		dc.LocalMeta[dataList[3]+dataList[4]]=ipMap
	}
}

