package main

import (
	"log"
	"silver/node/client"
	"silver/node/point"
	"silver/utils"
	"time"
)

func main() {

	//tagSet
	tagKv:=make(map[string]string,0)
	tagKv["a1"]="vv3"
	tagKv["b2"]="vv4"
	tagKv["c3"]="kk3"

	//point
	wp:=&point.WritePoint {
		DataBase:             "db1",
		TableName:            "table1",
		Tags:                 tagKv,
		TimePrecision:        utils.NS,
	}

	dataType:=utils.Int

	//metric value
	value:=make(map[int64][]byte,0)
	switch wp.TimePrecision {
	case utils.S:
		for n:=0; n < 10000 ; n++ {
			t:=(time.Now().UnixNano()+int64(n)) / 1000000000

			value[t] = utils.Float64ToByte(float64(n))
		}
		break
	case utils.NS:
		for n:=0; n < 10000 ; n++ {
			value[time.Now().UnixNano()+int64(n)] = utils.Float64ToByte(float64(n))
		}
		break
	case utils.MS:
		for n:=0; n < 10000 ; n++ {
			t:=(time.Now().UnixNano()+int64(n)) / 1000000
			value[t] = utils.Float64ToByte(float64(n))
		}
		break
	case utils.US:
		for n:=0; n < 10000 ; n++ {
			t:=(time.Now().UnixNano()+int64(n))/ 1000
			value[t] = utils.Float64ToByte(float64(n))
		}
		break
	default:
		log.Println("not support time precision ! ",wp.TimePrecision)
	}

	//metric type
	metric:= &point.Metric {
		Metric:               value,
		MetricType:           utils.Double,
	}

	//metric kv
	metricKv:=make(map[string]*point.Metric)
	metricKv["m1"]=metric

	wp.Metric=metricKv

	tc:=client.NewClient("127.0.0.1:12346")
	var writeList []*point.WritePoint
	writeList=append(writeList, wp)
	tc.ExecuteWrite(writeList)
}
