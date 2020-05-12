package main

import (
	"log"
	"silver/config"
	"silver/metastore"
	"silver/node"
)


var c1 config.NodeConfig

func init() {

	c1=config.LoadConfigInfo("config/config1.yaml")

}

func main() {
	node1,e:=node.NewNode(c1.NodeAddr.CluAddr,c1.NodeAddr.CluAddr,c1.NodeAddr.TcpAddr)
	if e !=nil {
		log.Fatal(c1.NodeName+" init failed ! ",e)
	}
	//元数据服务
	listener1,e:= metastore.NewListener(c1.MetaStore.NodeAddr,c1.MetaStore.Timeout,c1.MetaStore.NodePath)
	if e != nil {
		log.Fatal(c1.NodeAddr.TcpAddr+" init discovery failed !",e)
	}
	//初始化注册元数据
	metaStore1:= metastore.NewMetaStore(c1.NodeData,c1.NodeAddr.TcpAddr)
	register1:= metastore.NewRegister(c1.MetaStore.NodePath,metaStore1.NodeAddr,
		c1.MetaStore.NodeAddr,c1.MetaStore.Timeout,c1.MetaStore.HeartBeat)
	if metaStore1.MetaData !=nil {
		for db,tbMap:=range metaStore1.MetaData {
			if tbMap !=nil {
				for tb,_:=range tbMap {
					e:=register1.PutNode(db,tb)
					if e !=nil {
						log.Fatal("register metaStore data failed ! ",e)
					}
				}
			}
		}
	}
	storage1:= node.NewStorage(c1,listener1,register1)
	worker1:=node.NewWorker(storage1,node1,listener1,5000)
	go worker1.Listen(c1.NodeAddr.TcpAddr)
	select {

	}
}
