package client

import (
	"silverDB/config"
	"silverDB/node/point"
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
