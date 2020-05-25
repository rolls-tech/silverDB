package main

import (
	"silver/config"
	"silver/node/client"
	"silver/node/point"
)

func main() {
	tags:=make(map[string]string,0)
	tags["a1"]="vv3"
	rp:=&point.ReadPoint{
		DataBase:              "db1",
		TableName:             "table1",
		Tags:                 tags,
		Metrics:              nil,
		StartTime:            1589960091113831699,
		EndTime:              1590046491113831699,
	}
	config:=config.LoadConfigInfo("config/config1.yaml")
	tc:=client.NewClient(config.NodeAddr.TcpAddr)
	tc.ExecuteRead(rp)



}
