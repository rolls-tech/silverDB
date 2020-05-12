package client

import (
	"silver/config"
	"silver/node/point"
	"testing"
	"time"
)

func Test(t *testing.T) {
	tagKv:=make(map[string]string,0)
	tagKv["k1"]="v1"
	tagKv["k2"]="v2"
	kv:=make(map[int64]float64,0)

		for n:=0; n < 10000 ;n++ {
			kv[time.Now().UnixNano()+int64(n)] = float64(n)
		}

	kv[time.Now().UnixNano()] = float64(123)
	value:=&point.Value {
		Kv:                   kv,
	}
	filedKv:=make(map[string]*point.Value)
	filedKv["key"]=value
	wp:=&point.WritePoint{
		DataBase:             "db1",
		TableName:            "table1",
		Tags:                 tagKv,
		Value:                filedKv,
	}
	config:=config.LoadConfigInfo("../../config/config1.yaml")
    tc:=NewClient(config.NodeAddr.TcpAddr)
    var writeList []*point.WritePoint
	writeList=append(writeList, wp)
    tc.ExecuteWrite(writeList)
}


func TestRead(t *testing.T) {

	tags:=make(map[string]string,0)
	tags["k1"]="v1"
	rp:=&point.ReadPoint{
		DataBase:              "db1",
		TableName:             "table1",
		Tags:                 tags,
		Metrics:              nil,
		StartTime:            1589245880610794800,
		EndTime:              1589245880611771999,
	}
	config:=config.LoadConfigInfo("../../config/config1.yaml")
	tc:=NewClient(config.NodeAddr.TcpAddr)
	tc.ExecuteRead(rp)
}
