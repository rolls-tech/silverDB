package client

import (
	"silver/benchmark"
	"testing"
)

func TestClient_Operate(t *testing.T) {
	var cmds []*benchmark.Cmd
	cmd := benchmark.Cmd{
		Name:     "get",
		DataBase: "testdb4",
		Bucket:   "test3",
		Key:      "bbbba32",
		Value:    "",
		Error:    nil,
	}
	cmds = append(cmds, &cmd)
	c := NewClient("127.0.0.1:12346", "bolt", cmds, "get")
	c.Operate()
}
