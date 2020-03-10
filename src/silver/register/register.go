package register

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"silver/worker"
	"time"
)

type NodeRegister struct {
	client    *clientv3.Client
	lease     clientv3.Lease
	leasers   *clientv3.LeaseGrantResponse
	canceling func()
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key string
}

func NewNodeRegister(addr []string, timeout int64, tick int64,workerNode *worker.Server) (*NodeRegister,error) {
       conf:=clientv3.Config {
		   Endpoints:   addr,
		   DialTimeout: time.Duration(timeout) * time.Second,
	   }

	var (
		client *clientv3.Client
	)

	if  c,err := clientv3.New(conf); err !=nil {
		return nil, err
	   }else{
	   	   client = c
	   }

	ng:= &NodeRegister{
		client:        client,
	}

	if ng,err:= ng.setLease(tick,workerNode); err !=nil{
		return ng,err
	}

	go ng.ListenLeaseRespChan()

	return ng,nil
}

func (register NodeRegister) ListenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-register.keepAliveChan:
			if leaseKeepResp == nil {
				fmt.Printf("close!")
				return
			}
		}
	}
}

func (register *NodeRegister) setLease(time int64,workerNode *worker.Server) (*NodeRegister, error) {
	status:=workerNode.GetHealthStatus()
	if status == true {
		lease:=clientv3.NewLease(register.client)
		leaseResp,_ := lease.Grant(context.TODO(),time)
		ctx, canceling := context.WithCancel(context.TODO())
		leaseRespChan,_ :=lease.KeepAlive(ctx,leaseResp.ID)
		register.lease=lease
		register.leasers=leaseResp
		register.canceling=canceling
		register.keepAliveChan=leaseRespChan
	}
	return  register,nil
}

func (register *NodeRegister) PutNode(key, value string)  error {
	kv :=clientv3.NewKV(register.client)
	_, err := kv.Put(context.TODO(), key, value, clientv3.WithLease(register.leasers.ID))
	return err
}

func (register *NodeRegister) RevokeLease() error {
	register.canceling()
	time.Sleep(2*time.Second)
	_,err := register.lease.Revoke(context.TODO(),register.leasers.ID)
	return  err
}


