package main

import (
	"flag"
	"log"
	"silver/cluster"
	"silver/http"
	"silver/storage"
	"silver/tcp"
)

func main() {
	typ:=flag.String("type","boltdb","storage type")
	dataPath:=flag.String("dataPath","","data path")
	db:=flag.String("dbName","test","dbName")
	table:=flag.String("table","test-table","tableName")
	node:=flag.String("node","192.168.124.156","node address")
	clus:=flag.String("cluster","","cluster address")
	flag.Parse()
	log.Println("type is",*typ)
	log.Println("dataPath is",*dataPath)
	log.Println("dbName is",*db)
	log.Println("table is",*table)
	log.Println("node is",*node)
	log.Println("cluster is",*clus)
	c:= storage.New(*typ,*dataPath,*db,*table)
	n,err:=cluster.New(*node,*clus)
	if err!=nil {
		panic(err)
	}
	go tcp.New(c,n).Listen()
	http.New(c,n).Listen()
}