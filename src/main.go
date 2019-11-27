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
	//flag.StringVar(&node, "node", "localhost", "node address")
	//flag.StringVar(&clus, "cluster", "", "cluster address")
	flag.Parse()
	log.Println("type is", typ)
	//log.Println("dataPath is",dataPath)
	//log.Println("dbName is",db)
	//log.Println("table is",table)
	//log.Println("node is", node)
	//log.Println("cluster is", clus)

}

type nodeConfig struct {
	dataDir  []string
	httpAddr string
    tcpAddr string
	cluAddr string
}

type nodeList struct {
	nodes []nodeConfig
}

func initNodes() (nodeList,[]string){
	var dataDir1 = make([]string, 0)
	var dataDir2 = make([]string, 0)
	var dataDir3 = make([]string, 0)
	p1 := "D:\\dev\\silver\\testdata1\\"
	p2 := "D:\\dev\\silver\\testdata2\\"
	p3 := "D:\\dev\\silver\\testdata3\\"
	p4 := "D:\\dev\\silver\\testdata4\\"
	p5 := "D:\\dev\\silver\\testdata5\\"
	p6 := "D:\\dev\\silver\\testdata6\\"
	dataDir1 = append(dataDir1, p1)
	dataDir1 = append(dataDir1, p2)
	dataDir2 = append(dataDir2, p3)
	dataDir2 = append(dataDir2, p4)
	dataDir3 = append(dataDir3, p5)
	dataDir3 = append(dataDir3, p6)

	node1:=nodeConfig{
		dataDir:  dataDir1,
		httpAddr: "127.0.0.1:12345",
		tcpAddr:  "127.0.0.1:12346",
		cluAddr:  "127.0.0.1:7946",
	}
	node2:=nodeConfig{
		dataDir:  dataDir2,
		httpAddr: "127.0.0.1:12347",
		tcpAddr:  "127.0.0.1:12348",
		cluAddr:  "127.0.0.1:7947",
	}
	node3:=nodeConfig{
		dataDir:  dataDir3,
		httpAddr: "127.0.0.1:12349",
		tcpAddr:  "127.0.0.1:12340",
		cluAddr:  "127.0.0.1:7948",
	}
	nodes:=make([]nodeConfig,0)
	cluster:=make([]string,0)
	nodes=append(nodes,node1)
	nodes=append(nodes,node2)
	nodes=append(nodes,node3)
	cluster=append(cluster,node1.cluAddr)
	cluster=append(cluster,node2.cluAddr)
	cluster=append(cluster,node3.cluAddr)
    allNodes:=nodeList{nodes:nodes}
	return allNodes,cluster
}



func main() {
	allNodes,_:=initNodes()
	node1:=allNodes.nodes[0]

	//node1
	c1 := storage.New(typ,node1.dataDir)
	n1, err := cluster.New(node1.cluAddr,node1.cluAddr)
	if err != nil {
		panic(err)
	}
	go tcp.New(c1, n1).Listen(node1.tcpAddr)
	http.New(c1, n1).Listen(node1.httpAddr)
}
