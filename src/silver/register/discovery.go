package register

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"log"
	"strings"
	"sync"
	"time"
)

type DiscoverClient struct{
	client  *clientv3.Client
	ServerList map[string]map[string]bool
	mu sync.Mutex
}

func NewDiscoverClient (addr []string,timeout int64)(*DiscoverClient,error) {
	conf :=clientv3.Config{
		Endpoints:   addr,
		DialTimeout: time.Duration(timeout) * time.Second,
	}

	if c,err := clientv3.New(conf);err==nil {
		return &DiscoverClient {
			client: c,
			ServerList:make(map[string]map[string]bool,0),
		}, nil
	}else {
		return nil,err
	}
}

func (dc *DiscoverClient) GetService(prefix string) error {
	response, err := dc.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil{
		return err
	}

	dc.extractAddr(response)

	go dc.watcher(prefix)

	return nil

}

func (dc *DiscoverClient) watcher(prefix string){
	watch := dc.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wasp := range watch{
		for _ ,ev:= range wasp.Events{
			switch ev.Type{
			case mvccpb.PUT:
				dc.ParseMetaData(string(ev.Kv.Key))
			case mvccpb.DELETE:
				dc.DeleteServiceList(string(ev.Kv.Key))
			}
		}
	}

}

func (dc *DiscoverClient) extractAddr(response *clientv3.GetResponse) {
	if response != nil && response.Kvs != nil {
		for i := range response.Kvs {
			if k :=response.Kvs[i].Key; k != nil {
				dc.ParseMetaData(string(response.Kvs[i].Key))
			}
		}
	}
}

func (dc *DiscoverClient) DeleteServiceList(key string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	delete(dc.ServerList,key)
	log.Println("delete data key :",key)
}

func (dc *DiscoverClient) ParseMetaData(key string) {
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
