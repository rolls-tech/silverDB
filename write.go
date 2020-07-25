package main

import (
	"silver/node/client"
	"silver/node/point"
	"silver/utils"
	"time"
)

func main() {

	//tagSet
	tagKv := make(map[string]string, 0)
	tagKv["a1"] = "vv3"
	tagKv["b2"] = "vv4"
	tagKv["c3"] = "kk3"

	//point
	wp := &point.WritePoint{
		DataBase:      "db1",
		TableName:     "table1",
		Tags:          tagKv,
		TimePrecision: utils.NS,
	}

	//metric value
	value1 := make(map[int64][]byte, 0)
	value2 := make(map[int64][]byte, 0)

	var metricType1 int32
	var metricType2 int32

	for n := 0; n < 10000; n++ {
		tt := time.Now().UnixNano() + int64(n)
		data, dataType := utils.TransDataType(n)
		value1[tt] = data
		metricType1 = dataType
	}

	for n := 0; n < 10000; n++ {
		tt := time.Now().UnixNano() + int64(n)
		data, dataType := utils.TransDataType(12323.21212121)
		value2[tt] = data
		metricType2 = dataType
	}

	//metric type
	metric1 := &point.Metric{
		Metric:     value1,
		MetricType: metricType1,
	}

	//metric type
	metric2 := &point.Metric{
		Metric:     value2,
		MetricType: metricType2,
	}

	//metric kv
	metricKv := make(map[string]*point.Metric)
	metricKv["m1"] = metric1
	metricKv["m2"] = metric2
	wp.Metric = metricKv

	tc := client.NewClient("127.0.0.1:12346")
	var writeList []*point.WritePoint
	writeList = append(writeList, wp)
	tc.ExecuteWrite(writeList)
}
