package main

import (
	"log"
	"silver/config"
	"silver/metastore"
	"silver/node"
)

var c2 config.NodeConfig

func init() {

	c2=config.LoadConfigInfo("config/config2.yaml")

}

func main() {
	node2,e:=node.NewNode(c2.NodeAddr.CluAddr,"127.0.0.1:7947",c2.NodeAddr.TcpAddr)
	if e !=nil {
		log.Fatal(c2.NodeName+" init failed ! ",e)
	}
	//元数据服务
	listener2,e:= metastore.NewListener(c2.MetaStore.MetaAddr,c2.MetaStore.Timeout,c2.MetaStore.MetaPrefix,c2.MetaStore.NodePrefix)
	if e != nil {
		log.Fatal(c2.NodeAddr.TcpAddr+" init discovery failed !",e)
	}
	//初始化注册元数据
	metaStore2:= metastore.NewMetaStore(c2.DataDir,c2.NodeAddr.TcpAddr)
	register2:= metastore.NewRegister(c2.MetaStore,metaStore2.NodeAddr)
	if metaStore2.MetaData !=nil {
		for db,tbMap:=range metaStore2.MetaData {
			if tbMap !=nil {
				for tb,_:=range tbMap {
					e:=register2.PutMata(db,tb)
					if e !=nil {
						log.Fatal("register metaStore data failed ! ",e)
					}
				}
			}
		}
	}
	e=register2.PutNode(c2.NodeAddr.CluAddr)
	if e !=nil {
		log.Fatal("register node info failed ! ",e)
	}
	storage2:= node.NewStorage(c2,listener2,register2)
	worker2:=node.NewWorker(storage2,node2,listener2,5000)
	worker2.Listen(c2.NodeAddr.TcpAddr)
}