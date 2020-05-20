package main

import (
	"silver/node/client"
	"silver/node/point"
	"time"
)

func main() {

	tagKv:=make(map[string]string,0)
	tagKv["a1"]="vv3"
	tagKv["b2"]="vv4"
	tagKv["c3"]="kk3"
	kv:=make(map[int64]float64,0)

	for n:=0; n < 15000 ; n++ {
		kv[time.Now().UnixNano()+int64(n)] = float64(n)
	}

	kv[time.Now().UnixNano()] = float64(123)
	value:=&point.Value {
		Kv:                   kv,
	}
	filedKv:=make(map[string]*point.Value)
	filedKv["k1"]=value
	wp:=&point.WritePoint{
		DataBase:             "db1",
		TableName:            "table1",
		Tags:                 tagKv,
		Value:                filedKv,
	}
	tc:=client.NewClient("127.0.0.1:12346")
	var writeList []*point.WritePoint
	writeList=append(writeList, wp)
	tc.ExecuteWrite(writeList)


}
