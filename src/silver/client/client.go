package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type Cmd struct {
	Name string
	DataBase string
	Bucket string
	Key string
	Value string
	Error error
}

type Client interface {
	Run(*Cmd)
	PipelineRun([] *Cmd)
}

func New(typ,server string) Client{
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

func (c *tcpClient) sendGet(database,bucket,key string) {
	klen:=len(key)
	dblen:=len(database)
	tlen:=len(bucket)
	_,err:=c.Write([]byte(fmt.Sprintf("G%d,%d,%d,%s%s%s",dblen,tlen,klen,database,bucket,key)))
	if err !=nil {
		log.Println(err.Error())
	}
}

func (c *tcpClient) sendSet(database,bucket,key,value string) {
	klen:=len(key)
	vlen:=len(value)
	dblen:=len(database)
	tlen:=len(bucket)
	fmt.Printf("S%d,%d,%d,%d,%s%s%s%s\n",dblen,tlen,klen,vlen,database,bucket,key,value)
	_,err:=c.Write([]byte(fmt.Sprintf("S%d,%d,%d,%d,%s%s%s%s",dblen,tlen,klen,vlen,database,bucket,key,value)))
	if err !=nil {
		log.Println(err)
	}
}

func (c *tcpClient) sendDel(database,bucket,key string) {
	dblen:=len(database)
	tlen:=len(bucket)
	klen:=len(key)
	_,err:=c.Write([]byte(fmt.Sprintf("D%d,%d,%d,%s",dblen,tlen,klen,key)))
	if err !=nil {
		log.Println(err.Error())
	}
}

func readLen(r *bufio.Reader) string {
	tmp,e:=r.ReadString(',')
	if tmp=="" {
		return ""
	}
	if e !=nil {
		return ""
	}
	return strings.ReplaceAll(tmp,",","")
}

func (c *tcpClient) recvResponse() (string,error) {
	l1:=readLen(c.r)
	vlen,e:=strconv.Atoi(l1)
	if vlen == 0 {
		return "",nil
	}
	value:=make([]byte,vlen)
	_,e=io.ReadFull(c.r,value)
	if e !=nil {
		return "",e
	}
	return string(value),nil
}

func (c *tcpClient) Run(cmd *Cmd) {
	if cmd.Name =="get"{
		c.sendGet(cmd.DataBase,cmd.Bucket,cmd.Key)
		cmd.Value,cmd.Error=c.recvResponse()
		fmt.Println(cmd.Value)
		return
	}
	if cmd.Name == "set" {
		c.sendSet(cmd.DataBase,cmd.Bucket,cmd.Key,cmd.Value)
		_,cmd.Error=c.recvResponse()
		return
	}
	if cmd.Name == "del" {
		c.sendDel(cmd.DataBase,cmd.Bucket,cmd.Key)
		_,cmd.Error=c.recvResponse()
		return
	}
	panic("unknown cmd name " + cmd.Name)
}

func (c *tcpClient) PipelineRun(cmds []*Cmd) {
	if len(cmds) == 0 {
		return
	}
	for _,cmd:=range cmds {
		if cmd.Name == "get" {
			c.sendGet(cmd.DataBase,cmd.Bucket,cmd.Key)
		}
		if cmd.Name == "set" {
			c.sendSet(cmd.DataBase,cmd.Bucket,cmd.Key,cmd.Value)
		}
		if cmd.Name == "del" {
			c.sendDel(cmd.DataBase,cmd.Bucket,cmd.Key)
		}
	}
	for _,cmd :=range cmds {
		cmd.Value,cmd.Error=c.recvResponse()
		fmt.Println(cmd.Value)
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















































