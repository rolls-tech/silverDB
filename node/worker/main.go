package main

import (
	"log"
	"silverDB/config"
	"silverDB/node"
)

var c2 config.NodeConfig

func init() {
	c2=config.LoadConfigInfo("config/config2.yaml")
}


func main()  {

	node2, e :=node.NewNode(c2.NodeAddr.CluAddr,"127.0.0.1:7947")
	if e !=nil {
		log.Fatal(c2.NodeName+" init failed ! ",e)
	}
	storage2:=node.NewStorage(c2)
	worker2:=node.NewWorker(storage2,node2,5000)
	go worker2.Listen(c2.NodeAddr.TcpAddr)
	select {

	}
}
