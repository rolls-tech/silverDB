package client

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type ConfigClient struct {
	Server      string
	StorageType string
	Cmds        []*Cmd
	OperateType string
}

type Cmd struct {
	Name     string
	DataBase string
	Table   string
	RowKey  string
	DataTime string
	StartTime string
	EndTime string
	Key      string
	Value    string
	Error    error
}

type Client struct {
	net.Conn
	r *bufio.Reader
}

func (conf *ConfigClient) newClient() *Client {
	c, e := net.Dial("tcp", conf.Server)
	if e != nil {
		panic(e)
	}
	r := bufio.NewReader(c)
	return &Client{c, r}
}

func NewClient(server string, storageType string, cmds []*Cmd, operateType string) *ConfigClient {
	c := ConfigClient{
		Server:      server,
		StorageType: storageType,
		Cmds:        cmds,
		OperateType: operateType,
	}
	return &c
}

func (conf *ConfigClient) Operate() {
	c := conf.newClient()
	conf.PipelineRun(c)
}

func (conf *ConfigClient) PipelineRun(c *Client) {
	if len(conf.Cmds) == 0 {
		return
	}
	for _, cmd := range conf.Cmds {
		if conf.OperateType == "get" {
			c.sendGet(cmd.DataBase, cmd.Table,cmd.RowKey,cmd.Key,conf.StorageType,cmd.StartTime,cmd.EndTime)
		}
		if conf.OperateType == "set" {
			c.sendSet(cmd.DataBase, cmd.Table, cmd.RowKey,cmd.Key,cmd.Value,conf.StorageType,cmd.DataTime)
		}
		if conf.OperateType == "del" {
			c.sendDel(cmd.DataBase, cmd.Table,cmd.RowKey, cmd.Key, conf.StorageType,cmd.DataTime,cmd.EndTime)
		}
	}
	for _, cmd := range conf.Cmds {
		cmd.Value, cmd.Error = c.processResponse(cmd)
		fmt.Println(cmd.Value)
	}
}

func (conf *ConfigClient) Run(c *Client,cmd Cmd) {
	if conf.OperateType == "get" {
		c.sendGet(cmd.DataBase, cmd.Table,cmd.RowKey,cmd.Key,conf.StorageType,cmd.StartTime,cmd.EndTime)
		cmd.Value, cmd.Error = c.processResponse(&cmd)
		fmt.Println(cmd.Value)
		return
	}
	if conf.OperateType == "set" {
		c.sendSet(cmd.DataBase, cmd.Table, cmd.RowKey,cmd.Key,cmd.Value,conf.StorageType,cmd.DataTime)
		cmd.Value, cmd.Error = c.processResponse(&cmd)
		fmt.Println(cmd.Value)
		return
	}
	if conf.OperateType == "del" {
		c.sendDel(cmd.DataBase, cmd.Table,cmd.RowKey, cmd.Key, conf.StorageType,cmd.DataTime,cmd.EndTime)
		cmd.Value, cmd.Error = c.processResponse(&cmd)
		fmt.Println(cmd.Value)
		return
	}
	panic("unknown cmd name " + cmd.Name)
}

func (c *Client) sendGet(dataBase,table,rowKey,key,storageType,startTime,endTime string) {
	var klen int
	var dblen int
	var tblen int
	klen = len(key)
	dblen= len(dataBase)
	tblen= len(table)
	switch storageType {
	case "cache":
		_, err := c.Write([]byte(fmt.Sprintf("Gc%d,%s",klen,key)))
		if err != nil {
			log.Println(err.Error())
		}
	case "bolt":
		if dblen == 0 || tblen == 0 {
			log.Println("DataBase and Table is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("Gb%d,%d,%d,%s%s%s",dblen,tblen,klen,dataBase,table,key)))
		if err != nil {
			log.Println(err.Error())
		}
	case "tss":
		rklen:=len(rowKey)
		stlen:=len(startTime)
		etlen:=len(endTime)
		if dblen == 0 || tblen == 0 || rklen == 0 || stlen == 0 || etlen == 0 {
			log.Println("DataBase and Table and rowKey and startTime and endTime is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("Gt%d,%d,%d,%d,%d,%d,%s%s%s%s%s%s",dblen,tblen,rklen,klen,stlen,etlen,dataBase,table,rowKey,key,startTime,endTime)))
		if err != nil {
			log.Println(err.Error())
		}
	default:
		log.Println("not supported storage type")
	}
}

func (c *Client) sendSet(database,table,rowKey,key,value,storageType,dataTime string){
	var klen int
	var vlen int
	var dblen int
	var tblen int
	klen = len(key)
	vlen = len(value)
	dblen= len(database)
	tblen= len(table)
	switch storageType {
	case "cache":
		_, err := c.Write([]byte(fmt.Sprintf("Sc%d,%d,%s%s",klen, vlen, key, value)))
		if err != nil {
			log.Println(err)
		}
	case "bolt":
		if dblen == 0 || tblen == 0 {
			log.Println("DataBase and Table is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("Sb%d,%d,%d,%d,%s%s%s%s", dblen,tblen,klen,vlen,database,table,key,value)))
		if err != nil {
			log.Println(err)
		}
	case "tss":
		rklen:=len(rowKey)
		dtlen:=len(dataTime)
		if dblen == 0 || tblen == 0 || rklen == 0 {
			log.Println("DataBase and Table and rowKey is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("St%d,%d,%d,%d,%d,%d,%s%s%s%s%s%s", dblen,tblen,rklen,klen,vlen,dtlen,database,table,rowKey,key,value,dataTime)))
		if err != nil {
			log.Println(err)
		}
	default:
		log.Println("not supported storage type")
	}
}

func (c *Client) sendDel(database,table,rowKey,key,storageType,dataTime,endTime string) {
	var klen int
	var dblen int
	var tblen int
	klen = len(key)
	dblen= len(database)
	tblen= len(table)
	switch storageType {
	case "cache":
		_, err := c.Write([]byte(fmt.Sprintf("Dc%d,%s",klen,key)))
		if err != nil {
			log.Println(err.Error())
		}
	case "bolt":
		if dblen == 0 || tblen == 0 {
			log.Println("DataBase and Table is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("Db%d,%d,%d,%s%s%s",dblen, tblen, klen,database,table,key)))
		if err != nil {
			log.Println(err.Error())
		}
	case "tss":
		rklen:=len(rowKey)
		dtlen:=len(dataTime)
		etlen:=len(endTime)
		if dblen == 0 || tblen == 0 || rklen ==0 || dtlen == 0 {
			log.Println("DataBase and Table and rowKey and dataTime is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("Dt%d,%d,%d,%d,%d,%d,%s%s%s%s%s%s",dblen,tblen,rklen,klen,dtlen,etlen,database,table,rowKey,key,dataTime,endTime)))
		if err != nil {
			log.Println(err.Error())
		}
	default:
		log.Println("not supported storage type")
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

func (c *Client) processResponse(cmd *Cmd) (string,error) {
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return "",nil
	}
	if op == 'R' {
		value,_:=c.recvResponse()
        redirect:=strings.Split(value,":")
        addr:=redirect[1]
		var cmds []*Cmd
		cmds = append(cmds, cmd)
		c := NewClient(addr+":12348", "bolt", cmds, "set")
		client:=c.newClient()
		c.Run(client,*cmds[0])
	}
	value,error := c.recvResponse()
	return value,error
}

func (c *Client) recvResponse() (string, error) {
	l1 := readLen(c.r)
	vlen, e := strconv.Atoi(l1)
	if vlen == 0 {
		return "", nil
	}
	value := make([]byte, vlen)
	_, e = io.ReadFull(c.r, value)
	if e != nil {
		return "", e
	}
	return string(value), nil
}
