package main

import (
	"flag"
	"fmt"
	"math/rand"
	"silver/node/client"
	"silver/node/point"
	"strings"
	"sync"
	"time"
)

var typ, server, operation, key, value string
var batchSize, valueSize, threads, keyspaceLen, pipeLen int

func init() {
	flag.StringVar(&typ, "t", "silverDB", "storage type")
	flag.StringVar(&server, "h", "127.0.0.1:12346", "storage server address")
	flag.StringVar(&key, "k", "k", "key")
	flag.StringVar(&value, "v", "v", "value")
	flag.IntVar(&batchSize, "n", 5000, "number of point every pipeline")
	flag.IntVar(&valueSize, "d", 1, "data size of SET/GET value in bytes")
	flag.IntVar(&threads, "c", 1, "number of parallel connections")
	flag.IntVar(&keyspaceLen, "r", 20, "keyspaceLen,use random keys from 0 to keyspaceLen-1")
	flag.IntVar(&pipeLen, "P", 10, "pipeline length")
	flag.Parse()
	fmt.Println("type is", typ)
	fmt.Println("server is", server)
	fmt.Println("key is", key)
	fmt.Println("value is", value)
	fmt.Println("total", batchSize*pipeLen*threads, "point")
	fmt.Println("data size is", valueSize)
	fmt.Println("we have", threads, "connections")
	fmt.Println("keyspaceLen is", keyspaceLen)
	fmt.Println("pipeline length is", pipeLen)
}

type statistic struct {
	id int
	count int
	time  time.Duration
}

type result struct {
	statBuckets []statistic
}

func (r *result) addStatistic(stat statistic) {
	r.statBuckets=append(r.statBuckets,stat)
}

func (r *result) addDuration(id int ,d time.Duration) {
	r.addStatistic(statistic{id,batchSize*pipeLen,d})
}

func pipelineRun(id,batchSize,pipeLen int,result *result,wg *sync.WaitGroup) {
	valuePrefix := strings.Repeat("a", valueSize)
	var writeList []*point.WritePoint
	for i := 1; i <= pipeLen; i++ {
		var tmp int
		if keyspaceLen > 0 {
			tmp = rand.Intn(keyspaceLen)
		} else {
			tmp = id*pipeLen + i
		}
		key := fmt.Sprintf("%s%d", "b",tmp)
		value := fmt.Sprintf("%s%d", valuePrefix, tmp)
		tagKv:=make(map[string]string,0)
		tagKv[key]=value
		kv:=make(map[int64]float64,0)
		for n:=0; n < batchSize ; n++ {
			kv[time.Now().UnixNano()+int64(n)] =float64(n)
		}
		v:=&point.Value {
			Kv:kv,
		}
		filedKv:=make(map[string]*point.Value)
		filedKv[key]=v
		wp:=&point.WritePoint {
			DataBase:             key,
			TableName:            value,
			Tags:                 tagKv,
			Value:                filedKv,
		}
		writeList=append(writeList,wp)
	}
	start := time.Now()
	tc:=client.NewClient("127.0.0.1:12346")
	tc.ExecuteWrite(writeList)
	d := time.Now().Sub(start)
	result.addDuration(id,d)
	wg.Done()
}

func main() {
	res := &result{make([]statistic, 0)}
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go pipelineRun(i,batchSize,pipeLen,res,&wg)
	}
	wg.Wait()
	fmt.Printf("start " +"%d threads to run , every thread process %d ponits\n",threads,batchSize*pipeLen)
	d := time.Now().Sub(start)
	statCountSum := 0
	statTimeSum := time.Duration(0)
	for i := 0; i < threads; i++ {
		statCountSum  +=res.statBuckets[i].count
		statTimeSum += res.statBuckets[i].time
	}
	fmt.Printf("%d usec average for each request\n", int64(statTimeSum/time.Microsecond)/int64(statCountSum))
	fmt.Printf("throughput is %f MB/s\n", float64((statCountSum)*(valueSize+12))/1e6/d.Seconds())
	fmt.Printf("rps is %f\n",float64(statCountSum) / d.Seconds())
}
