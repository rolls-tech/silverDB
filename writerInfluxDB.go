package main

import (
	"bufio"
	"flag"
	"fmt"
	client "github.com/influxdata/influxdb1-client/v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type params struct {
	host string
	dataDir string
	dataBase string
	table string
	batchSize int
	threads int
	pipeline int
	rows int
}

var param params

func init() {
	flag.StringVar(&(param.dataDir), "dir", "/Volumes/info/data/", "device data dir")
	flag.IntVar(&(param.batchSize),"batch",5000,"write data batch size")
	flag.StringVar(&(param.host), "host", "127.0.0.1:8086", "silverDB server address and port")
	flag.StringVar(&(param.dataBase), "db", "db", "silverDB databaseName")
	flag.StringVar(&(param.table), "table", "table", "silverDB tableName")
	flag.IntVar(&(param.threads),"threads",10,"write threads num")
	flag.IntVar(&(param.pipeline),"pipeline",10,"pipeline size per request ")
	flag.IntVar(&(param.rows),"rows",1000000,"row nums per datafile")
	flag.Parse()
	fmt.Println("test data dir: ", param.dataDir)
	fmt.Println("write data batch size: ", param.batchSize)
	fmt.Println("influxDB server address and port: ", param.host)
	fmt.Println("influxDB database: ", param.dataBase)
	fmt.Println("influxDB table: ",param.table)
	fmt.Println("write threads nums: ",param.threads)
	fmt.Println("pipeline size per request: ",param.pipeline)
	fmt.Println("row nums per datafile: ",param.rows)
}


func readDataFile(fileIndex int,startSize,endSize int) client.BatchPoints {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  param.dataBase,
		Precision: "ns",
	})
	fs,err:=os.Open(param.dataDir+"data_"+strconv.Itoa(fileIndex))
	if err !=nil {
		log.Fatal("open data file is failed !",err)
	}
	defer fs.Close()
	reader:=bufio.NewScanner(fs)
	count:=0
	for reader.Scan() {
		line:=strings.ReplaceAll(reader.Text(),"\n","")
		if count >=startSize && count < endSize {
			preLine:=strings.Split(line," ")
			tstring:=strings.Split(preLine[0],",")
			mstring:=strings.Split(preLine[1],",")
			timestamp,_:=strconv.ParseInt(mstring[3],10,64)

			tr,_:=strconv.ParseInt(mstring[0],10,32)
			hd,_:=strconv.ParseFloat(mstring[1],64)
			st,_:=strconv.ParseBool(mstring[2])


			tags:=map[string]string {
				"deviceId":tstring[0],
				"deviceGroup":tstring[1],
				"deviceName":tstring[2],
			}
			fields := map[string]interface{}{
				"temperature": tr,
				"humidity": hd,
				"status":   st,
			}

			pt, err := client.NewPoint(param.table, tags, fields, time.Unix(0,timestamp))

			if err != nil {
				fmt.Println("Error: ", err.Error())
			}
			bp.AddPoint(pt)
		}
		count++
	}
	return bp
}



func writeInfluxDBPoint(fileIndex int) []client.BatchPoints {
	wpList:=make([]client.BatchPoints,0)
	wpSize:= param.rows / param.batchSize
	for s:=0; s < wpSize; s++ {
		startSize:=s*param.batchSize
		endSize:=startSize + param.batchSize
		batchRecord:=readDataFile(fileIndex,startSize,endSize)
		wpList=append(wpList,batchRecord)
		}
	return wpList
}


func writeInfluxDBData(wg *sync.WaitGroup,i,ftNums int) {
	n:=0
	wpTotal:=make([]client.BatchPoints,0)
	for n < ftNums {
		fileIndex:=i*ftNums+n
		wpList:=writeInfluxDBPoint(fileIndex)
		wpTotal=append(wpTotal,wpList...)
		n++
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://"+param.host,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
	for _,bp:=range wpTotal {
		c.Write(bp)
	}
	wg.Done()
}



func main() {

	var wg sync.WaitGroup
	startTime:=time.Now()
	fileList,_:=ioutil.ReadDir(param.dataDir)
	ftNums:= len(fileList) / param.threads
	for i:=0; i < param.threads; i++ {
		wg.Add(1)
		go writeInfluxDBData(&wg,i,ftNums)
	}
	wg.Wait()
	d:=time.Now().Sub(startTime)
	fmt.Printf("total time: %d s\n",d.Microseconds()/1000000)
	fmt.Printf("sec average for each reacord: %d /s \n",int64(param.rows * 10) / (d.Microseconds()/1000000))

}
