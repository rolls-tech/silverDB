package storage

import (
	"log"
	"testing"
)

func Test(t *testing.T) {
	/*ts:=make([]string,0)
	var tagSet []string
	ts=append(ts, "a")
	ts=append(ts, "b")
	ts=append(ts, "c")
	ts=append(ts, "d")
	ts=append(ts, "e")
	kv:=combine(0,2,ts,"",tagSet)
	log.Println(kv)*/
    tags:=make(map[string]string,0)
    tags["k1"]="v1"
	tags["k2"]="v2"
	tags["a1"]="b1"
	tags["c1"]="d1"

	tagSet:=generateTagPrefix(tags)
	log.Println(tagSet)

}

