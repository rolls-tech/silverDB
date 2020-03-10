package main

import (
	"flag"
	"fmt"
	"time"
)

var typ, server, operation, key, value string
var total, valueSize, threads, keyspaceLen, pipeLen int

func init() {
	flag.StringVar(&typ, "t", "bolt", "storage type")
	flag.StringVar(&server, "h", "127.0.0.1:12346", "storage server address")
	flag.StringVar(&key, "k", "k", "key")
	flag.StringVar(&value, "v", "v", "value")
	flag.IntVar(&total, "n", 50000, "total number of point")
	flag.IntVar(&valueSize, "d", 1, "data size of SET/GET value in bytes")
	flag.IntVar(&threads, "c", 10, "number of parallel connections")
	flag.StringVar(&operation, "op", "set", "Test set,support get/set/mixed")
	flag.IntVar(&keyspaceLen, "r", 10, "keyspaceLen,use random keys from 0 to keyspaceLen-1")
	flag.IntVar(&pipeLen, "P", 10, "pipeline length")
	flag.Parse()
	fmt.Println("type is", typ)
	fmt.Println("server is", server)
	fmt.Println("key is", key)
	fmt.Println("value is", value)
	fmt.Println("total", total*pipeLen, "point")
	fmt.Println("data size is", valueSize)
	fmt.Println("we have", threads, "connections")
	fmt.Println("operation is", operation)
	fmt.Println("keyspaceLen is", keyspaceLen)
	fmt.Println("pipeline length is", pipeLen)
}

func main() {
	ch := make(chan *tsResult, threads)
	res := &tsResult{0, 0, 0, make([]tsStatistic, 0)}
	start := time.Now()
	for i := 0; i < threads; i++ {
		go tsOperate(i, total/threads, ch)
	}
	for i := 0; i < threads; i++ {
		res.addResult(<-ch)
	}
	d := time.Now().Sub(start)
	totalCount := res.getCount + res.missCount + res.setCount
	fmt.Printf("%d records get\n", res.getCount)
	fmt.Printf("%d records miss\n", res.missCount)
	fmt.Printf("%d records set\n", res.setCount)
	fmt.Printf("%f seconds total\n", d.Seconds())
	statCountSum := 0
	statTimeSum := time.Duration(0)
	for _, s := range res.statBuckets {
		if s.count == 0 {
			continue
		}
		statCountSum += s.count
		statTimeSum += s.time
		//fmt.Printf("%d%% reqests < %d ms\n", statCountSum*100/totalCount, b+1)
	}
	fmt.Printf("%d usec average for each request\n", int64(statTimeSum/time.Microsecond)/int64(statCountSum))
	fmt.Printf("throughput is %f MB/s\n", float64((res.getCount+res.setCount)*(valueSize+12))/1e6/d.Seconds())
	fmt.Printf("rps is %f\n",float64(totalCount) / d.Seconds())
}
