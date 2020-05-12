package metastore

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"log"
	"time"
)

type Register struct {
	client     *clientv3.Client
	tick       int64
	resp       *clientv3.LeaseGrantResponse
	metaPath   string
	workerAddr string
}

func NewRegister(metaPath,workerAddr string,metaAddr []string, timeout int64, tick int64) *Register {
	conf:=clientv3.Config {
		Endpoints:   metaAddr,
		DialTimeout: time.Duration(timeout) * time.Second,
	}

	client,e:=clientv3.New(conf)
	if e != nil {
		log.Fatal("create register client failed !",e)
	}

	resp,e:=client.Grant(context.TODO(),tick)
	if e !=nil {
		log.Fatal(e)
	}

	ng:=&Register{
		client: client,
		tick: tick,
		resp: resp,
		metaPath:metaPath,
		workerAddr:workerAddr,
	}

	go ng.listenerNode()

	return ng
}


func (ng *Register) PutNode(databaseName, tableName string)  error {
    key:= ng.metaPath +databaseName+"/"+tableName+"/"+ng.workerAddr
    value:=""
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

func (ng *Register) listenerNodeData() {








}