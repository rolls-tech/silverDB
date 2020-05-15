package metastore

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"log"
	"silver/config"
	"time"
)

type Register struct {
	client     *clientv3.Client
	tick       int64
	resp       *clientv3.LeaseGrantResponse
	metaPath   string
	workerAddr string
	nodePath string
}

func NewRegister(meta config.MetaStore,workerAddr string) *Register {
	conf:=clientv3.Config {
		Endpoints: meta.MetaAddr  ,
		DialTimeout: time.Duration(meta.Timeout) * time.Second,
	}

	client,e:=clientv3.New(conf)
	if e != nil {
		log.Fatal("create register client failed !",e)
	}

	resp,e:=client.Grant(context.TODO(),meta.HeartBeat)
	if e !=nil {
		log.Fatal(e)
	}

	ng:=&Register{
		client: client,
		tick: meta.HeartBeat,
		resp: resp,
		metaPath:meta.MetaPrefix,
		nodePath:meta.NodePrefix,
		workerAddr:workerAddr,
	}

	go ng.listenerNode()

	return ng
}


func (ng *Register) PutMata(databaseName, tableName string)  error {
    key:= ng.metaPath +databaseName+"/"+tableName+"/"+ng.workerAddr
    value:=""
	_,e:= ng.client.Put(context.TODO(), key, value, clientv3.WithLease(ng.resp.ID))
	if e != nil {
		log.Fatal(e)
	}
	return e
}

func (ng *Register) PutNode(clusterAddr string)  error {
	key:= ng.nodePath+clusterAddr
	value:=ng.workerAddr
	_,e:= ng.client.Put(context.TODO(), key, value, clientv3.WithLease(ng.resp.ID))
	if e != nil {
		log.Fatal(e)
	}
	return e
}


func (ng *Register) listenerNode() {
  /* for {
	  time.Sleep(3*time.Second)*/
		  ch,e:=ng.client.KeepAlive(context.TODO(),ng.resp.ID)
		  if e !=nil {
			  log.Println("register lease failed !",e)
		  }
		  if <-ch == nil {
		  	 log.Println("metaStore server exception")
		  }
   //}
}
