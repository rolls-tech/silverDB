package client

import (
	cmd2 "silver/cmd"
	"silver/storage"
	"testing"
	"time"
)

func Test_tsClient_ExecuteCmd(t *testing.T) {
	tagKv:=make(map[string]string,0)
	tagKv["k1"]="v1"
	tagKv["k2"]="v2"
	kv:=make(map[int64][]byte,0)
	kv[time.Now().UnixNano()] = []byte("123")
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

	setCmd:=&cmd2.SetCmd{Wp:wp}
	getCmd:=&cmd2.GetCmd{
		DataBase:             "db1",
		TableName:            "table1",
		Tags:                 tagKv,
		FieldKey:             "key",
		StartTime:            1577590990861297500,
		EndTime:              1577778353908801100,
	}
	cmd1:=&cmd2.Cmd {
		CmdType: "set",
		Gd:      cmd2.GetCmd{},
		Sd:      *setCmd,
	}
	cmd2:=&cmd2.Cmd{
		CmdType: "get",
		Gd:      *getCmd,
		Sd:      cmd2.SetCmd{},
	}
	tc:= NewTsClient("127.0.0.1:12346", "tsStorage")
	tc.ExecuteCmd(cmd1)
	tc.ExecuteCmd(cmd2)
}