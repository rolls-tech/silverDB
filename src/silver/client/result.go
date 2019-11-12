package main

import (
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"math/rand"
	"strings"
	"time"
)

type statistic struct {
	count int
	time time.Duration
}

type result struct {
	getCount int
	missCount int
	setCount int
	statBuckets []statistic
}

func (r *result) addStatistic(bucket int,stat statistic){
	if bucket > len(r.statBuckets) -1 {
		newStatBuckets:=make([]statistic,bucket+1)
		copy(newStatBuckets,r.statBuckets)
		r.statBuckets=newStatBuckets
	}
	s:=r.statBuckets[bucket]
	s.count+=stat.count
	s.time+=stat.time
	r.statBuckets[bucket]=s
}

func (r *result) addDuration(d time.Duration,typ string) {
	bucket:=int(d/time.Millisecond)
	r.addStatistic(bucket,statistic{1, d})
	if typ == "get" {
		r.getCount++
	}else if typ =="set" {
			r.setCount++
	}else{
			r.missCount++
	}
}

func (r *result) addResult(src *result){
	for b,s:=range src.statBuckets{
		r.addStatistic(b,s)
	}
	r.getCount+=src.getCount
	r.missCount+=src.missCount
	r.setCount+=src.setCount
}

func operate(id,count int,ch chan * result) {
	client:=New(typ,server)
	cmds:=make([]*Cmd,0)
	valuePrefix:=strings.Repeat("a",valueSize)
	r:=&result{
		getCount:    0,
		missCount:   0,
		setCount:    0,
		statBuckets: make([]statistic,0),
	}
	for i:=0;i<count;i++{
		var tmp int
		if keyspacelen > 0 {
			tmp=rand.Intn(keyspacelen)
		}else{
			tmp=id*count+i
		}
		key:=fmt.Sprintf("%d",tmp)
		value:=fmt.Sprintf("%s%d",valuePrefix,tmp)
		name:=operation
		if operation =="mixed" {
			if rand.Intn(2)==1 {
				name ="set"
			}else {
				name ="get"
			}
		}
		c:=&Cmd{name,key,value,nil}
		if pipelen >1 {
			cmds=append(cmds,c)
			if len(cmds)==pipelen {
				pipeline(client,cmds,r)
				cmds=make([]*Cmd,0)
			}
		}else{
			run(client,c,r)
		}
	}
	if len(cmds) != 0 {
		pipeline(client,cmds,r)
	}
	ch <-r
}

func run(client Client,c *Cmd,r *result){
	expect:=c.Value
	start:=time.Now()
	client.Run(c)
	d:=time.Now().Sub(start)
	resultType:=c.Name
	if resultType == "get" {
		if c.Value == "" {
			resultType = "miss"
		}else if c.Value != expect {
			panic(c)
		}
	}
	r.addDuration(d,resultType)
}

func pipeline(client Client,cmds []*Cmd,r *result) {
	expect:=make([]string,len(cmds))
	for i,c:=range cmds {
		if c.Name == "get" {
			expect[i]=c.Value
		}
	}
	start:=time.Now()
	client.PipelineRun(cmds)
	d:=time.Now().Sub(start)
	for i,c:=range cmds {
		resultType :=c.Name
		if resultType == "get" {
			if c.Value == "" {
				resultType="miss"
			}else if c.Value !=expect[i] {
				fmt.Println(expect[i])
				panic(c.Value)
			}
		}
		r.addDuration(d,resultType)
	}
}





































































