package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

var typ, server, operation, db, bucket, key, value string
var total, valueSize, threads, keyspacelen, pipelen int

func init() {
	flag.StringVar(&typ, "t", "bolt", "storage type")
	flag.StringVar(&server, "h", "127.0.0.1:12346", "storage server address")
	flag.StringVar(&db, "db", "test223", "database")
	flag.StringVar(&bucket, "b", "test-table", "table")
	flag.StringVar(&key, "k", "", "key")
	flag.StringVar(&value, "v", "", "value")
	flag.IntVar(&total, "n", 10000, "total number of requests")
	flag.IntVar(&valueSize, "d", 10, "data size of SET/GET value in bytes")
	flag.IntVar(&threads, "c", 5, "number of parallel connections")
	flag.StringVar(&operation, "op", "set", "Test set,support get/set/mixed")
	flag.IntVar(&keyspacelen, "r", 10, "keyspacelen,use random keys from 0 to keyspacelen-1")
	flag.IntVar(&pipelen, "P", 10, "pipeline length")
	flag.Parse()
	fmt.Println("type is", typ)
	fmt.Println("server is", server)
	fmt.Println("database is", db)
	fmt.Println("table is", bucket)
	fmt.Println("key is", key)
	fmt.Println("value is", value)
	fmt.Println("total", total, "requests")
	fmt.Println("data size is", valueSize)
	fmt.Println("we have", threads, "connections")
	fmt.Println("operation is", operation)
	fmt.Println("keyspacelen is", keyspacelen)
	fmt.Println("pipeline length is", pipelen)
	rand.Seed(time.Now().UnixNano())
}

func main() {
	ch := make(chan *result, threads)
	res := &result{0, 0, 0, make([]statistic, 0)}
	start := time.Now()
	for i := 0; i < threads; i++ {
		go operate(i, total/threads, ch)
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
	for b, s := range res.statBuckets {
		if s.count == 0 {
			continue
		}
		statCountSum += s.count
		statTimeSum += s.time
		fmt.Printf("%d%% reqests < %d ms\n", statCountSum*100/totalCount, b+1)
	}
	fmt.Printf("%d usec average for each request\n", int64(statTimeSum/time.Microsecond)/int64(statCountSum))
	fmt.Printf("throughput is %f MB/s\n", float64((res.getCount+res.setCount)*valueSize)/1e6/d.Seconds())

}
