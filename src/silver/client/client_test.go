package client

import (
	"testing"
)

func TestClient_Operate(t *testing.T) {
	var cmds []*Cmd
	cmd := Cmd{
		Name:     "",
		DataBase: "testdb1",
		Bucket:   "test",
		Key:      "bbb",
		Value:    "123",
		Error:    nil,
	}
	cmds = append(cmds, &cmd)
	c := NewClient("127.0.0.1:12346", "bolt", cmds, "get")
	c.Operate()
}
