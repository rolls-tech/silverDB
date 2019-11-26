package main

import (
	"flag"
	"log"
	"silver/cluster"
	"silver/http"
	"silver/storage"
	"silver/tcp"
)

var typ, node, clus string

//var dataPath,db,table string

func init() {
	flag.StringVar(&typ, "type", "bolt", "storage type")
	//flag.StringVar(&dataPath,"dataPath","D:\\dev\\silver\\testdata1","data path")
	//flag.StringVar(&db,"dbName","test","dbName")
	//flag.StringVar(&table,"table","test-table","tableName")
	flag.StringVar(&node, "node", "localhost", "node address")
	flag.StringVar(&clus, "cluster", "", "cluster address")
	flag.Parse()
	log.Println("type is", typ)
	//log.Println("dataPath is",dataPath)
	//log.Println("dbName is",db)
	//log.Println("table is",table)
	log.Println("node is", node)
	log.Println("cluster is", clus)

}

func main() {
	var dataDir = make([]string, 0)
	p1 := "D:\\dev\\silver\\testdata1\\"
	p2 := "D:\\dev\\silver\\testdata2\\"
	dataDir = append(dataDir, p1)
	dataDir = append(dataDir, p2)
	c := storage.New(typ, dataDir)
	n, err := cluster.New(node, clus)

	if err != nil {
		panic(err)
	}
	go tcp.New(c, n).Listen()
	http.New(c, n).Listen()
}
