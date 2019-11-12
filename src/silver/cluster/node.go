package cluster

import (
	"github.com/hashicorp/memberlist"
	"io/ioutil"
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

func New(addr,cluster string) (Node,error){
	conf:=memberlist.DefaultLANConfig()
	conf.Name=addr
	conf.BindAddr=addr
	conf.LogOutput=ioutil.Discard
	l,err:=memberlist.Create(conf)
	if err !=nil {
		return nil,err
	}
	if cluster==" " {
		cluster =addr
	}
	clu:=[]string{cluster}
	_,err=l.Join(clu)
	if err !=nil {
		return nil,err
	}
	circle:=consistent.New()
	circle.NumberOfReplicas=256
	go func(){
       for {
       	m:=l.Members()
       	nodes:=make([]string,len(m))
       	for i,node:=range m{
       		nodes[i]=node.Name
		}
       	circle.Set(nodes)
       	time.Sleep(time.Second)
	   }
	}()
	return &node{circle,addr},nil
}



















































