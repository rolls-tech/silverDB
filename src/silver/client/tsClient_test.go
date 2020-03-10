package client

import (
	"silver/cmd"
	"silver/storage"
	"testing"
	"time"
)

func Test_tsClient_ExecuteCmd(t *testing.T) {
	tagKv:=make(map[string]string,0)
	tagKv["k1"]="v1"
	tagKv["k2"]="v2"
	kv:=make(map[int64]float64,0)
	/*
	for n:=0; n < 10000 ;n++ {
		kv[time.Now().UnixNano()+int64(n)] = float64(n)
	}
	*/
	kv[time.Now().UnixNano()] = float64(123)
	value:=&storage.Value {
		Kv:                   kv,
	}
	filedKv:=make(map[string]*storage.Value)
	filedKv["key"]=value
	wp:=&storage.WPoint {
		DataBase:             "db1",
		TableName:            "table1",
		Tags:                 tagKv,
		Value:                filedKv,
	}
	setCmd:=&cmd.SetCmd{Wp:wp}
	cmd1:=&cmd.Cmd {
			CmdType: "set",
			Gd:      cmd.GetCmd{},
			Sd:      *setCmd,
		}
	getCmd:=&cmd.GetCmd{
		DataBase:             "db1",
		TableName:            "table1",
		Tags:                 tagKv,
		FieldKey:             "key",
		StartTime:            1581329012899665539,
		EndTime:              1581501812899665539,
	}
	cmd2:=&cmd.Cmd {
		CmdType: "get",
		Gd:      *getCmd,
		Sd:      cmd.SetCmd{},
	}
	tc:= NewTsClient("127.0.0.1:12348", "tsStorage")
	tc.ExecuteCmd(cmd1)
	tc.ExecuteCmd(cmd2)
}