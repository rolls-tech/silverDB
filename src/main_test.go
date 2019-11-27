package main

import (
	"log"
	"silver/cluster"
	"silver/http"
	"silver/storage"
	"silver/tcp"
	"testing"
)

type testConfig struct {
	dataDir  []string
	httpAddr string
	tcpAddr string
	cluAddr string
}

func TestNode2(t *testing.T) {
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

	node1:=testConfig{
		dataDir:  dataDir1,
		httpAddr: "127.0.0.1:12345",
		tcpAddr:  "127.0.0.1:12346",
		cluAddr:  "127.0.0.1:7946",
	}
	node2:=testConfig{
		dataDir:  dataDir2,
		httpAddr: "127.0.0.1:12347",
		tcpAddr:  "127.0.0.1:12348",
		cluAddr:  "10.0.8.14:7946",
	}

	c2 := storage.New("bolt",node2.dataDir)
	n2, err := cluster.New(node2.cluAddr,node1.cluAddr)
	log.Println(n2)
	if err != nil {
		panic(err)
	}
	go tcp.New(c2, n2).Listen(node2.tcpAddr)
	http.New(c2, n2).Listen(node2.httpAddr)

}


func TestNode3(t *testing.T) {
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

	node1:=testConfig{
		dataDir:  dataDir1,
		httpAddr: "127.0.0.1:12345",
		tcpAddr:  "127.0.0.1:12346",
		cluAddr:  "127.0.0.1:7946",
	}
	node3:=testConfig{
		dataDir:  dataDir3,
		httpAddr: "127.0.0.1:12349",
		tcpAddr:  "127.0.0.1:12340",
		cluAddr:  "127.0.0.1:7946",
	}
	//node3:=allNodes.nodes[2]
	//node2
	c2 := storage.New("bolt",node3.dataDir)
	n2, err := cluster.New(node3.cluAddr,node1.cluAddr)
	if err != nil {
		panic(err)
	}
	go tcp.New(c2, n2).Listen(node3.tcpAddr)
	http.New(c2, n2).Listen(node3.httpAddr)

}