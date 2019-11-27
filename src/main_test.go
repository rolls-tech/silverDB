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

func TestNode2(t2 *testing.T) {
	var dataDir1 = make([]string, 0)
	var dataDir2 = make([]string, 0)
	p1 := "D:\\dev\\silver\\testdata1\\"
	p2 := "D:\\dev\\silver\\testdata2\\"
	p3 := "D:\\dev\\silver\\testdata3\\"
	p4 := "D:\\dev\\silver\\testdata4\\"
	dataDir1 = append(dataDir1, p1)
	dataDir1 = append(dataDir1, p2)
	dataDir2 = append(dataDir2, p3)
	dataDir2 = append(dataDir2, p4)

	node1:=testConfig{
		dataDir:  dataDir1,
		httpAddr: "127.0.0.1:12345",
		tcpAddr:  "127.0.0.1:12346",
		cluAddr:  "127.0.0.1",
	}
	node2:=testConfig{
		dataDir:  dataDir2,
		httpAddr: "127.0.0.1:12347",
		tcpAddr:  "127.0.0.1:12348",
		cluAddr:  "127.0.0.1",
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