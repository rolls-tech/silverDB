package cluster

import (
	"flag"
	"github.com/hashicorp/memberlist"
	"io/ioutil"
	"log"
	"os"
	"stathat.com/c/consistent"
	"time"
)

type Node interface {
	ShouldProcess(key string) (string,bool)
	Members() []string
    Addr() string
}

type node struct {
	*consistent.Consistent
	addr string
}

func (n *node) Addr() string {
	return n.addr
}

func (n *node) ShouldProcess(key string) (string,bool) {
    addr,_:=n.Get(key)
    return addr,addr==n.addr
}

var bindPort = flag.Int("port", 8001, "gossip port")

func New(addr string,cluster string) (Node,error) {
	conf:=memberlist.DefaultLANConfig()
	hostName,_:=os.Hostname()
	conf.Name=hostName+"-"+addr
	conf.BindAddr = addr
	//conf.BindPort=*bindPort
	conf.LogOutput=ioutil.Discard
	l,err:=memberlist.Create(conf)
	if err !=nil {
		return nil,err
	}
	if cluster=="" {
		cluster=addr
	}
	clu:=[]string{cluster}
	log.Println(clu)
	_,err=l.Join(clu)
	if err !=nil {
		return nil,err
	}
	log.Println(l.Members())
	circle:=consistent.New()
	circle.NumberOfReplicas=256
	go func(){
       for {
       	m:=l.Members()
       	nodes:=make([]string,len(m))
       	for i,node:=range m {
       		nodes[i]=node.Name
       		log.Println(node.Port,node.Name,node.Addr)
		}
       	circle.Set(nodes)
       	time.Sleep(time.Second)
	   }
	}()
	return &node{circle,addr},nil
}



















































