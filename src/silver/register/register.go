package main

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/gpmgo/gopm/modules/log"
	"golang.org/x/net/context"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"quickSilver/worker"
	"silver/metadata"
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


func NewNodeRegister(addr []string, timeout int64, tick int64,workerNode worker.Worker) (*NodeRegister,error) {
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

func (register *NodeRegister) setLease(time int64,workerNode worker.Worker) (*NodeRegister, error) {
	status:=workerNode.GetHealthstatus()
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


func init() {





	workerNode1:=worker.NewWorker(ip1,port1,token1)
	reg1, _ := NewNodeRegister([]string{"127.0.0.1:2379"}, 10, 10,workerNode1)

	orgList1:=workerNode1.Getorg()

	bucketList1:=workerNode1.Getbucket(orgList1)

	_= reg1.PutNode("/quickSilver/workers/"+hostname1, ip1+":"+port1)

	for _,bucket:=range bucketList1 {
		_=reg1.PutNode("/quickSilver/orgs/"+bucket+"/"+ip1+":"+port1,workerNode1.Token)
	}
	_=reg1.PutNode("quickSilver/master","")

	ip2:="115.28.187.62"
	port2:="9997"
	hostname2:="node2"
	token2:="Token 4HCsA83zZmPySWgglAsBcSgnjkZjzwdhDVBU0cs5MarZ3X0VRARc0NLEsSjta274orcJb2DE6b29TnqWRtpySw=="
	workerNode2:=worker.NewWorker(ip2,port2,token2)

	orgList2:=workerNode2.Getorg()
	bucketList2:=workerNode2.Getbucket(orgList2)

	reg2, _ := NewNodeRegister([]string{"127.0.0.1:2379"}, 10, 10,workerNode2)
	_= reg2.PutNode("/quickSilver/workers/"+hostname2, ip2+":"+port2)

	for _,bucket:=range bucketList2 {
		_=reg2.PutNode("/quickSilver/orgs/"+bucket+"/"+ip2+":"+port2,workerNode2.Token)
	}
	_=reg2.PutNode("quickSilver/master","")

}

func main() {

	err:=rpc.Register(new(worker.Service))

     if err != nil {
     	log.Fatal("workerServer failed",err.Error())
	 }

	listener,err:=net.Listen("tcp","127.0.0.1:1234")
     if err != nil {
     	log.Fatal("workerServer ListenTcp error:",err)
	 }

     for {
		 conn,err :=listener.Accept()
		 if err != nil {
			 log.Fatal("Accept error:",err)
			 continue
		 }
		 go func(conn net.Conn) {
		 	jsonrpc.ServeConn(conn)
		 }(conn)
	 }


}
