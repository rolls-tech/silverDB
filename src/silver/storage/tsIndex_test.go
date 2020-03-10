package storage

import (
	"log"
	"testing"
)

func Test_combine(t *testing.T) {
	ts:=make([]string,0)
	var tagSet []string
	ts=append(ts, "a")
	ts=append(ts, "b")
	ts=append(ts, "c")
	ts=append(ts, "d")
	ts=append(ts, "e")
	kv:=combine(0,3,ts,"",tagSet)
	log.Println(kv)
}