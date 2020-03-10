package main

import (
	"silver/cluster"
	"silver/http"
	"silver/metadata"
	"silver/register"
	"silver/storage"
	"silver/worker"
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
		cluAddr:  "127.0.0.1:7947",
	}
	node2:=testConfig{
		dataDir:  dataDir2,
		httpAddr: "127.0.0.1:12347",
		tcpAddr:  "127.0.0.1:12348",
		cluAddr:  "127.0.0.1:7946",
	}
	ts:=storage.NewTss(node2.dataDir)
	wal:="D:\\dev\\silver\\wal2\\"

	flushCount:=5000
	indexDir:=make([]string,0)

	m2:=metadata.NewMeta(node2.dataDir,node2.tcpAddr)
	c2 := storage.New("tsStorage",wal,indexDir,ts,m2,int64(flushCount))
	n2, err := cluster.New(node2.cluAddr,node1.cluAddr)
	if err != nil {
		panic(err)
	}
	d2,err:= register.NewDiscoverClient([]string{"127.0.0.1:2379"},5)
	if err != nil {
		panic(err)
	}
	s2:=worker.New(c2, n2,d2)
	if m2.MetaData != nil {
		r1, _ := register.NewNodeRegister([]string{"127.0.0.1:2379"}, 10, 10,s2)
		for db,tb:=range m2.MetaData {
			e:=r1.PutNode("/silver/metaData/"+db+"/"+tb+"/"+node2.tcpAddr,"")
			if e !=nil {
				panic(e)
			}
		}
	}
	go s2.Listen("tsStorage",node2.tcpAddr)
	http.New(c2, n2).Listen(node2.httpAddr)

}