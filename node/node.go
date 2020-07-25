package node

import (
	"github.com/hashicorp/memberlist"
	"io/ioutil"
	"log"
	"stathat.com/c/consistent"
	"strconv"
	"strings"
	"time"
)

type Node interface {
	ShouldProcess(key string) (string, bool)
	Members() []string
	Addr() string
	Status() bool
	StorageAddr() string
}

type node struct {
	*consistent.Consistent
	addr        string
	l           *memberlist.Memberlist
	storageAddr string
}

func (n *node) StorageAddr() string {
	return n.storageAddr
}

func (n *node) ShouldProcess(key string) (string, bool) {
	addr, _ := n.Get(key)
	return addr, addr == n.addr
}

func (n *node) Addr() string {
	return n.addr
}

func (n *node) Status() bool {
	p := &pingNode{
		proto: "udp",
		ip:    n.addr,
	}
	_, err := n.l.Ping(n.addr, p)
	if err != nil {
		return false
	}
	return true
}

type pingNode struct {
	proto string
	ip    string
}

func (p *pingNode) Network() string {
	return p.proto
}

func (p *pingNode) String() string {
	return p.ip
}

func NewNode(addr string, cluster string, storageAddr string) (Node, error) {
	p1 := strings.Split(addr, ":")[1]
	p, err := strconv.Atoi(p1)
	LocalConf := memberlist.DefaultLocalConfig()
	LocalConf.Name = addr
	LocalConf.BindAddr = addr
	LocalConf.BindPort = p
	LocalConf.AdvertisePort = p
	LocalConf.LogOutput = ioutil.Discard
	l, err := memberlist.Create(LocalConf)
	if err != nil {
		return nil, err
	}
	if cluster == "" {
		cluster = addr
	}
	clu := []string{cluster}
	_, err = l.Join(clu)
	log.Println("running node are ", l.Members())
	if err != nil {
		return nil, err
	}
	circle := consistent.New()
	circle.NumberOfReplicas = 256
	go func() {
		for {
			time.Sleep(time.Second)
			m := l.Members()
			for _, node := range m {
				/*p:=&pingNode {
					proto: "udp",
					ip:    node.Name,
				}*/
				//_,e:=l.Ping(node.Name,p)
				//if e == nil {
				circle.Add(node.Name)
				//}
			}
		}
	}()
	return &node{circle, addr, l, storageAddr}, nil
}
