package main

import (
	"log"
	"silver/config"
	"silver/node"
	"silver/node/metastore"
)


var c1 config.NodeConfig

func init() {
	c1=config.LoadConfigInfo("config/config1.yaml")
}

func main() {

	node1, e :=node.NewNode(c1.NodeAddr.CluAddr,c1.NodeAddr.CluAddr)
	if e !=nil {
		log.Fatal(c1.NodeName+" init failed ! ",e)
	}

	//初始化注册元数据
	metaStore1:=metastore.NewMetaStore(c1.NodeData,c1.NodeAddr.TcpAddr)
    register1,e:=metastore.NewNodeRegister(c1.MetaStore.NodeAddr,c1.MetaStore.Timeout,c1.MetaStore.HeartBeat,node1)
	if e != nil {
		log.Fatal(c1.NodeAddr.TcpAddr+"init register failed !",e)
	}

	for db,tb:=range metaStore1.MetaData {
		e:=register1.PutNode(c1.MetaStore.NodePath+db+"/"+tb+"/"+metaStore1.NodeAddr,"")
		if e !=nil {
			log.Fatal("register metaStore data failed !",e)
		}
	}

	//元数据监听
    discovery1,e:=metastore.NewDiscovery(c1.MetaStore.NodeAddr,c1.MetaStore.Timeout,c1.MetaStore.NodePath)
    if e != nil {
        log.Fatal(c1.NodeAddr.TcpAddr+" init discovery failed !",e)
    }

	storage1:=node.NewStorage(c1)
	worker1:=node.NewWorker(storage1,node1,discovery1,5000)
	go worker1.Listen(c1.NodeAddr.TcpAddr)
	select {

	}
}
