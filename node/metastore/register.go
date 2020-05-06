package metastore

import (
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"log"
	"silverDB/node"
	"time"
)

type nodeRegister struct {
	client    *clientv3.Client
	lease     clientv3.Lease
	leasers   *clientv3.LeaseGrantResponse
	canceling func()
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key string
}

func NewNodeRegister(metaAddr []string, timeout int64, tick int64, node node.Node) (*nodeRegister,error) {
	conf:=clientv3.Config {
		Endpoints:   metaAddr,
		DialTimeout: time.Duration(timeout) * time.Second,
	}

	var (
		client *clientv3.Client
	)

	if  c,err := clientv3.New(conf); err !=nil {
		return nil, err
	} else {
		client = c
	}

	ng:= &nodeRegister {
		client:        client,
	}

	if ng,err:= ng.setLease(tick,node); err !=nil{
		return ng,err
	}

	go ng.listenLeaseRespChan()

	return ng,nil
}

func (ng *nodeRegister) listenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-ng.keepAliveChan:
			if leaseKeepResp == nil {
				log.Println("heatBeat chan is closed !")
				return
			}
		}
	}
}

func (ng *nodeRegister) setLease(time int64,node node.Node) (*nodeRegister, error) {
	status:=node.Status()
	if status == true {
		lease:=clientv3.NewLease(ng.client)
		leaseResp,_ := lease.Grant(context.TODO(),time)
		ctx, canceling := context.WithCancel(context.TODO())
		leaseRespChan,_ :=lease.KeepAlive(ctx,leaseResp.ID)
		ng.lease=lease
		ng.leasers=leaseResp
		ng.canceling=canceling
		ng.keepAliveChan=leaseRespChan
	}
	return  ng,nil
}

func (ng *nodeRegister) PutNode(key, value string)  error {
	kv :=clientv3.NewKV(ng.client)
	_, err := kv.Put(context.TODO(), key, value, clientv3.WithLease(ng.leasers.ID))
	return err
}

func (ng *nodeRegister) revokeLease() error {
	ng.canceling()
	_,err := ng.lease.Revoke(context.TODO(),ng.leasers.ID)
	return  err
}
