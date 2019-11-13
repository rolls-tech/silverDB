package main

import (
	"flag"
	"log"
	"silver/cache"
	"silver/cluster"
	"silver/http"
	"silver/tcp"
)

func main() {
	typ:=flag.String("type","boltdb","storage type")
	dataPath:=flag.String("dataPath","","data path")
	db:=flag.String("dbName","","dbName")
	table:=flag.String("table","","tableName")
	node:=flag.String("node","localhost","node address")
	clus:=flag.String("cluster","","cluster address")
	flag.Parse()
	log.Println("type is",*typ)
	log.Println("dataPath is",*dataPath)
	log.Println("dbName is",*db)
	log.Println("table is",*table)
	log.Println("node is",*node)
	log.Println("cluster is",*clus)
	c:=cache.New(*typ,*dataPath,*db,*table)
	n,err:=cluster.New(*node,*clus)
	if err!=nil {
		panic(err)
	}
	go tcp.New(c,n).Listen()
	http.New(c,n).Listen()
}

