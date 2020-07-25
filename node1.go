package main

import (
	"log"
	"silver/config"
	"silver/metastore"
	"silver/node"
)

var c1 config.NodeConfig

func init() {

	c1 = config.LoadConfigInfo("config/config1.yaml")

}

func main() {

	//元数据服务
	listener1, e := metastore.NewListener(c1.MetaStore.MetaAddr, c1.MetaStore.Timeout, c1.MetaStore.MetaPrefix, c1.MetaStore.NodePrefix)
	if e != nil {
		log.Fatal(c1.NodeAddr.TcpAddr+" init discovery failed !", e)
	}
	//初始化注册元数据
	metaStore1 := metastore.NewMetaStore(c1.DataDir, c1.NodeAddr.TcpAddr)
	register1 := metastore.NewRegister(c1.MetaStore, metaStore1.NodeAddr)
	if metaStore1.MetaData != nil {
		for db, tbMap := range metaStore1.MetaData {
			if tbMap != nil {
				for tb, _ := range tbMap {
					e := register1.PutMata(db, tb)
					if e != nil {
						log.Fatal("register metaStore data failed ! ", e)
					}
				}
			}
		}
	}
	e = register1.PutNode(c1.NodeAddr.CluAddr)
	if e != nil {
		log.Fatal("register node info failed ! ", e)
	}
	node1, e := node.NewNode(c1.NodeAddr.CluAddr, c1.NodeAddr.CluAddr, c1.NodeAddr.TcpAddr)
	if e != nil {
		log.Fatal(c1.NodeName+" init failed ! ", e)
	}
	storage1 := node.NewStorage(c1, listener1, register1)
	worker1 := node.NewWorker(storage1, node1, listener1, 5000)
	worker1.Listen(c1.NodeAddr.TcpAddr)
}
