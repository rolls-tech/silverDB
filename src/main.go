package main

import (
	"flag"
	"log"
	"silver/cache"
	"silver/http"
	"silver/tcp"
)

func main() {
	typ:=flag.String("type","inmemory","cache")
	dataPath:=flag.String("dataPath","","data path")
	db:=flag.String("dbName","","dbName")
	table:=flag.String("table","","tableName")
	flag.Parse()
	log.Println("type is",*typ)
	log.Println("dataPath is",*dataPath)
	log.Println("dbName is",*db)
	log.Println("table is",*table)
	c:=cache.New(*typ,*dataPath,*db,*table)
	go tcp.New(c).Listen()
	http.New(c).Listen()
}

