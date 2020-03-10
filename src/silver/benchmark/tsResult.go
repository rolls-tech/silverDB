package main

import (
	"fmt"
	"math/rand"
	"silver/client"
	"silver/cmd"
	"silver/storage"
	"strings"
	"time"
)

type tsStatistic struct {
	count int
	time  time.Duration
}

type tsResult struct {
	getCount    int
	missCount   int
	setCount    int
	statBuckets []tsStatistic
}

func (r *tsResult) addStatistic(bucket int, stat tsStatistic) {
	if bucket > len(r.statBuckets)-1 {
		newStatBuckets := make([]tsStatistic, bucket+1)
		copy(newStatBuckets, r.statBuckets)
		r.statBuckets = newStatBuckets
	}
	s := r.statBuckets[bucket]
	s.count += stat.count
	s.time += stat.time
	r.statBuckets[bucket] = s
}

func (r *tsResult) addDuration(d time.Duration, typ string) {
	bucket := int(d / time.Millisecond)
	r.addStatistic(bucket, tsStatistic{1, d})
	if typ == "get" {
		r.getCount++
	} else if typ == "set" {
		points:= total / threads
		r.setCount+=points
	} else {
		r.missCount++
	}
}

func (r *tsResult) addResult(src *tsResult) {
	for b, s := range src.statBuckets {
		r.addStatistic(b, s)
	}
	r.getCount += src.getCount
	r.missCount += src.missCount
	r.setCount += src.setCount
}

func tsOperate(id,count int, ch chan *tsResult) {
	tc:= client.NewTsClient("127.0.0.1:12346", "tsStorage")
	valuePrefix := strings.Repeat("a", valueSize)
	var cmdList []*cmd.Cmd
	r := &tsResult {
		getCount:    0,
		missCount:   0,
		setCount:    0,
		statBuckets: make([]tsStatistic, 0),
	}
	for i := 0; i < pipeLen; i++ {
		var tmp int
		if keyspaceLen > 0 {
			tmp = rand.Intn(keyspaceLen)
		} else {
			tmp = id*pipeLen + i
		}
		key := fmt.Sprintf("%d", tmp)
		value := fmt.Sprintf("%s%d", valuePrefix, tmp)
		name := operation
		if operation == "mixed" {
			if rand.Intn(2) == 1 {
				name = "set"
			} else {
				name = "get"
			}
		}
		tagKv:=make(map[string]string,0)
		tagKv[key]=value
		if name == "set" {
			kv:=make(map[int64]float64,0)
			for n:=0; n < count ; n++ {
				kv[time.Now().UnixNano()+int64(n)] =float64(n)
			}
			v:=&storage.Value {
				Kv:                   kv,
			}
			filedKv:=make(map[string]*storage.Value)
			filedKv[key]=v
			wp:=&storage.WPoint {
				DataBase:             key,
				TableName:            value,
				Tags:                 tagKv,
				Value:                filedKv,
			}
			setCmd:=&cmd.SetCmd{Wp:wp}
			cmd1:=&cmd.Cmd {
				CmdType: "set",
				Gd:      cmd.GetCmd{},
				Sd:      *setCmd,
			}
			cmdList=append(cmdList,cmd1)
		}
		if name == "get" {
			getCmd:=&cmd.GetCmd {
				DataBase:             key,
				TableName:            value,
				Tags:                 tagKv,
				FieldKey:             key,
				StartTime:            time.Now().UnixNano(),
				EndTime:              time.Now().UnixNano()+24*time.Hour.Nanoseconds(),
			}
			cmd2:=&cmd.Cmd {
				CmdType: "get",
				Gd:      *getCmd,
				Sd:      cmd.SetCmd{},
			}
			cmdList=append(cmdList,cmd2)
		}
	}
	start := time.Now()
	for _,cmd:=range cmdList {
		tc.ExecuteCmd(cmd)
		d := time.Now().Sub(start)
		r.addDuration(d, cmd.CmdType)
	}
	ch <- r
}
