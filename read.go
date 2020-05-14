package main

import (
	"silver/config"
	"silver/node/client"
	"silver/node/point"
)

func main() {
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
	tc:=client.NewClient(config.NodeAddr.TcpAddr)
	tc.ExecuteRead(rp)
}
