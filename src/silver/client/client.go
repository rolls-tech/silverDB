package main

import (
	"bufio"
	"golang.org/x/tools/go/ssa/interp/testdata/src/errors"
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type Cmd struct {
	Name string
	Key string
	Value string
	Error error
}

type Client interface {
	Run(*Cmd)
	PipelineRun([] *Cmd)
}

func New(typ,server string) Client{
	if typ == "redis" {
		return nil
	}
	if typ == "http" {
		return nil
	}
	if typ == "tcp" {
		return newTcpClient(server)
	}
	panic("unknown client type"+typ)
}

type tcpClient struct {
	net.Conn
	r *bufio.Reader
}

func (c *tcpClient) sendGet(key string) {
	klen:=len(key)
	c.Write([]byte(fmt.Sprintf("G%d %s",klen,key)))
}

func (c *tcpClient) sendSet(key,value string) {
	klen:=len(key)
	vlen:=len(value)
	c.Write([]byte(fmt.Sprintf("S%d %d %s%s",klen,vlen,key,value)))
}

func (c *tcpClient) sendDel(key string) {
	klen:=len(key)
	c.Write([]byte(fmt.Sprintf("D%d %s",klen,key)))
}

func readLen(r *bufio.Reader) int {
	tmp,e:=r.ReadString(' ')
	if e!=nil {
		log.Println(e)
		return 0
	}
	l,e:=strconv.Atoi(strings.TrimSpace(tmp))
	if e !=nil {
		log.Println(tmp,e)
		return 0
	}
	return l
}

func (c *tcpClient) recvResponse() (string,error) {
	vlen:=readLen(c.r)
	if vlen == 0 {
		return "",nil
	}
	if vlen < 0 {
		err := make ([]byte,-vlen)
		_,e:=io.ReadFull(c.r,err)
		if e !=nil {
			return " ",e
		}
		return "",errors.New(string(err))
	}
	value:=make([]byte,vlen)
	_,e:=io.ReadFull(c.r,value)
	if e !=nil {
		return "",e
	}
	return string(value),nil
}

func (c *tcpClient) Run(cmd *Cmd) {
	if cmd.Name =="get "{
		c.sendGet(cmd.Key)
		cmd.Value,cmd.Error=c.recvResponse()
		return
	}
	if cmd.Name == "set" {
		c.sendSet(cmd.Key,cmd.Value)
		_,cmd.Error=c.recvResponse()
		return
	}
	if cmd.Name == "del" {
		c.sendDel(cmd.Key)
		_,cmd.Error=c.recvResponse()
		return
	}
	panic("unknown cmd name" + cmd.Name)
}

func (c *tcpClient) PipelineRun(cmds []*Cmd) {
	if len(cmds) == 0 {
		return
	}
	for _,cmd:=range cmds {
		if cmd.Name == "get" {
			c.sendGet(cmd.Key)
		}
		if cmd.Name == "set" {
			c.sendSet(cmd.Key,cmd.Value)
		}
		if cmd.Name == "del" {
			c.sendDel(cmd.Key)
		}
	}
	for _,cmd :=range cmds {
		cmd.Value,cmd.Error=c.recvResponse()
	}
}

func newTcpClient(server string) *tcpClient {
	c,e:=net.Dial("tcp",server+":12346")
	if e !=nil {
		panic(e)
	}
	r:=bufio.NewReader(c)
	return &tcpClient{c,r}
}















































