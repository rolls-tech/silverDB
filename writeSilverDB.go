package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os"
	"silver/node/client"
	"silver/node/point"
	"silver/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)


type args struct {
	host string
	dataDir string
	dataBase string
	table string
	batchSize int
	threads int
	pipeline int
	rows int
}

var arg args

func init() {
	flag.StringVar(&(arg.dataDir), "dir", "/Volumes/info/data/", "device data dir")
	flag.IntVar(&(arg.batchSize),"batch",10000,"write data batch size")
	flag.StringVar(&(arg.host), "host", "127.0.0.1:12346", "silverDB server address and port")
	flag.StringVar(&(arg.dataBase), "db", "db", "silverDB databaseName")
	flag.StringVar(&(arg.table), "table", "table", "silverDB tableName")
	flag.IntVar(&(arg.threads),"threads",10,"write threads num")
	flag.IntVar(&(arg.pipeline),"pipeline",10,"pipeline size per request ")
	flag.IntVar(&(arg.rows),"rows",1000000,"row nums per datafile")
	flag.Parse()
	fmt.Println("test data dir: ", arg.dataDir)
	fmt.Println("write data batch size: ", arg.batchSize)
	fmt.Println("silverDB server address and port: ", arg.host)
	fmt.Println("silverDB database: ", arg.dataBase)
	fmt.Println("silverDB table: ",arg.table)
	fmt.Println("write threads nums: ",arg.threads)
	fmt.Println("pipeline size per request: ",arg.pipeline)
	fmt.Println("row nums per datafile: ",arg.rows)
}

type record struct {
	points map[string]*point.WritePoint
	count int
}

type totalRecord struct {
	totalCount int
	*record
}

func newTotalRecord() *totalRecord {
	return &totalRecord{
		totalCount: 0,
		record : &record {
			points: make(map[string]*point.WritePoint,0),
			count:0,
		},
	}
}




func writePoint(fileIndex int) []*point.WritePoint {
	wpList:=make([]*point.WritePoint,0)
	wpSize:= arg.rows / arg.batchSize
	tmp:=make(map[string]string,0)
	for s:=0; s < wpSize; s++ {
		startSize:=s*arg.batchSize
		endSize:=startSize + arg.batchSize
	    record:=readFile(fileIndex,startSize,endSize,tmp)
	    for _,wp:=range record.points {
			wpList=append(wpList,wp)
		}
	}
	wp1:=wpList[0]
	p1:= utils.NewSortMap(wp1.Metric["temperature"].Metric)
	sort.Sort(p1)

	for i:=1; i < len(wpList); i++ {
		wp2:=wpList[i]
		p2:= utils.NewSortMap(wp2.Metric["temperature"].Metric)
		sort.Sort(p2)
		if p1[0].T == p2[0].T {
			log.Info(wp1.Tags,wp2.Tags,p1[0].T,p2[0].T)
		}
	}

	return wpList
}


func readFile(fileIndex int,startSize,endSize int,tmp map[string]string) *totalRecord {
	fs,err:=os.Open(arg.dataDir+"data_"+strconv.Itoa(fileIndex))
	if err !=nil {
		log.Fatal("open data file is failed !",err)
	}
	defer fs.Close()
	record:=newTotalRecord()
	reader:=bufio.NewScanner(fs)
	count:=0
	for reader.Scan() {
		row:=reader.Text()
		if count >=startSize && count < endSize {
			line:=strings.ReplaceAll(row,"\n","")

			/*
			//判断重复数据
			fsName,ok:=tmp[line]
            if ok {
            	log.Info(strconv.Itoa(count)+"-----"+line+"-----"+fsName)
			} else {
				tmp[line]=strconv.Itoa(count)+fs.Name()
			}*/

			preLine:=strings.Split(line," ")
			tstring:=strings.Split(preLine[0],",")
			mstring:=strings.Split(preLine[1],",")

			timestamp,_:=strconv.ParseInt(mstring[3],10,64)

			tr,_:=strconv.ParseInt(mstring[0],10,32)
			hd,_:=strconv.ParseFloat(mstring[1],64)
			st,_:=strconv.ParseBool(mstring[2])

			temperature,t1:=utils.TransDataType(tr)
			humidity,t2:=utils.TransDataType(hd)
			status,t3:=utils.TransDataType(st)

			p,ok:=record.points[preLine[0]]

			if !ok {
				wp:=&point.WritePoint {
					DataBase:             arg.dataBase,
					TableName:            arg.table,
					TimePrecision:        utils.NS,
				}
				v1:=make(map[int64][]byte,0)
				v2:=make(map[int64][]byte,0)
				v3:=make(map[int64][]byte,0)
				m1:= &point.Metric {
					Metric: v1,
				}
				m2:=&point.Metric {
					Metric: v2,
				}
				m3:=&point.Metric {
					Metric: v3,
				}
				metricKv:=make(map[string]*point.Metric)
				metricKv["temperature"]=m1
				metricKv["humidity"]=m2
				metricKv["status"]=m3
				wp.Metric=metricKv

				wp.Metric["temperature"].Metric[timestamp]=temperature
				wp.Metric["temperature"].MetricType=t1

				wp.Metric["humidity"].Metric[timestamp]=humidity
				wp.Metric["humidity"].MetricType=t2

				wp.Metric["status"].Metric[timestamp]=status
				wp.Metric["status"].MetricType=t3

				tags:=map[string]string {
					"deviceId":tstring[0],
					"deviceGroup":tstring[1],
					"deviceName":tstring[2],
				}
				wp.Tags=tags
				record.points[preLine[0]]=wp
				record.totalCount+=1
				record.count+=1

			} else {
				p.Metric["temperature"].Metric[timestamp]=temperature
				p.Metric["humidity"].Metric[timestamp]=humidity
				p.Metric["status"].Metric[timestamp]=status
				record.count+=1
				record.totalCount+=1
			}
		}
		count++
	}
    return record
}



func writeData(wg *sync.WaitGroup,i,ftNums int) {
		n:=0
		wpTotal:=make([]*point.WritePoint,0)
		for n < ftNums {
			fileIndex:=i*ftNums+n
            wpList:=writePoint(fileIndex)
            wpTotal=append(wpTotal,wpList...)
	 	    n++
	    }
	   tc:=client.NewClient(arg.host)
	   requestNums:= len(wpTotal) / arg.pipeline
	   for i:=0;i<requestNums;i++ {
		   tc.ExecuteWrite(wpTotal[i*arg.pipeline:(i+1)*arg.pipeline])
	   }
	   wg.Done()
}


func main() {
	var wg sync.WaitGroup
	startTime:=time.Now()
	fileList,_:=ioutil.ReadDir(arg.dataDir)
	ftNums:= len(fileList) / arg.threads
	for i:=0; i < arg.threads; i++ {
    	wg.Add(1)
    	go writeData(&wg,i,ftNums)
	}
	wg.Wait()
	d:=time.Now().Sub(startTime)
	fmt.Printf("total time: %d s\n",d.Microseconds()/1000000)
	fmt.Printf("sec average for each reacord: %d /s \n",int64(arg.rows * 10) / (d.Microseconds()/1000000))
}
