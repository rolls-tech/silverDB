package client

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/cmd"
	"silver/storage"
	"strconv"
	"strings"
)

type tsClient struct {
	server      string
	storageType string
}

type client struct {
	net.Conn
	r *bufio.Reader
}

func (tc *tsClient) newClient() *client {
	c, e := net.Dial("tcp", tc.server)
	if e != nil {
		panic(e)
	}
	r := bufio.NewReader(c)
	return &client{c, r}
}

func NewTsClient(server string, storageType string) *tsClient {
	tc := &tsClient {
		server:      server,
		storageType: storageType,
	}
	return tc
}

func (tc *tsClient) ExecuteCmd(cmd *cmd.Cmd){
	c := tc.newClient()
	tc.pipelineRun(c,cmd)
}


func (tc *tsClient) pipelineRun(c *client,cmd *cmd.Cmd) {
	if tc.storageType == "" {
		log.Println("storageType should be not nil")
		return
	}
    if cmd.CmdType == "set" {
    	c.sendSet(cmd.Sd)
		_,_=c.handleSetResponse(cmd)
	}
	if cmd.CmdType == "get" {
		c.sendGet(cmd.Gd)
		value,_:=c.handleGetResponse(cmd)
		if value.Kv != nil {
			fieldKv:=make(map[string]*storage.Value,0)
			fieldKv[cmd.Gd.FieldKey]=&value
			wp:=storage.WPoint{
				DataBase:             cmd.Gd.DataBase,
				TableName:            cmd.Gd.TableName,
				Tags:                 cmd.Gd.Tags,
				Value:                fieldKv,
			}
			log.Println(wp)
		}
	}
}

func (c *client) sendSet(setCmd cmd.SetCmd) {
	data,e:=proto.Marshal(setCmd.Wp)
	if e !=nil {
		log.Println(e.Error())
		return
	}
	dLen:=len(data)
	_, err := c.Write([]byte(fmt.Sprintf("S%d,%s",dLen,data)))
	if err != nil {
		log.Println(err.Error())
	}
}

func (c *client) sendGet(getCmd cmd.GetCmd) {
	data,e:=proto.Marshal(&getCmd)
	if e !=nil {
		log.Println(e.Error())
		return
	}
	dLen:=len(data)
	_, err := c.Write([]byte(fmt.Sprintf("G%d,%s",dLen,data)))
	if err != nil {
		log.Println(err.Error())
	}
}

func readLen(r *bufio.Reader) string {
	tmp, e := r.ReadString(',')
	if tmp == "" {
		return ""
	}
	if e != nil {
		return ""
	}
	return strings.ReplaceAll(tmp, ",", "")
}

func (c *client) handleSetResponse(cmd *cmd.Cmd) (storage.WPoint,error) {
	data:=storage.WPoint{}
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return data,nil
	}
	switch op {
	case 'R':
		v,_:=c.recvResponse()
		redirect:=strings.Split(string(v),":")
		addr:=redirect[0]
		tc := NewTsClient(addr+":12348", "tsStorage")
		client:=tc.newClient()
		tc.pipelineRun(client,cmd)
	case 'V':
		v,e:= c.recvResponse()
		_=proto.Unmarshal(v,&data)
		//log.Println("Write successfully!")
		return data,e
	}
	return data,e
}


func (c *client) handleGetResponse(cmd *cmd.Cmd) (storage.Value,error) {
	var value storage.Value
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return value,nil
	}
	switch op {
	case 'R':
		v,_:=c.recvResponse()
		redirect:=strings.Split(string(v),":")
		addr:=redirect[0]
		tc := NewTsClient(addr+":12348", "tsStorage")
		client:=tc.newClient()
		tc.pipelineRun(client,cmd)
	case 'V':
		v,e:= c.recvResponse()
		_=proto.Unmarshal(v,&value)
		return value,e
	}
	return value,e
}


func (c *client) recvResponse() ([]byte, error) {
	l1 := readLen(c.r)
	vLen, e := strconv.Atoi(l1)
	if vLen == 0 {
		return nil,nil
	}
	value := make([]byte, vLen)
	_, e = io.ReadFull(c.r, value)
	if e != nil {
		return nil,e
	}
	return value,nil
}
