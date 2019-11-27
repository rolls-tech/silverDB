package client

import (
	"testing"
)

func TestClient_Operate(t *testing.T) {
	var cmds []*Cmd
	cmd := Cmd{
		Name:     "set",
		DataBase: "testdb2",
		Bucket:   "test1",
		Key:      "bbb1",
		Value:    "123",
		Error:    nil,
	}
	cmds = append(cmds, &cmd)
	c := NewClient("127.0.0.1:12346", "bolt", cmds, cmd.Name)
	c.Operate()
}
