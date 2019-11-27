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
	Bucket   string
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
			c.sendGet(cmd.DataBase, cmd.Bucket, cmd.Key, conf.StorageType)
		}
		if conf.OperateType == "set" {
			c.sendSet(cmd.DataBase, cmd.Bucket, cmd.Key, cmd.Value, conf.StorageType)
		}
		if conf.OperateType == "del" {
			c.sendDel(cmd.DataBase, cmd.Bucket, cmd.Key, conf.StorageType)
		}
	}
	for _, cmd := range conf.Cmds {
		cmd.Value, cmd.Error = c.recvResponse()
		fmt.Println(cmd.Value)
	}
}

func (c *Client) sendGet(dataBase, bucket, key, storageType string) {
	var klen int
	klen = len(key)
	if storageType == "cache" {
		_, err := c.Write([]byte(fmt.Sprintf("G%d,%s", klen, key)))
		if err != nil {
			log.Println(err.Error())
		}
	} else {
		dblen := len(dataBase)
		tlen := len(bucket)
		if dblen == 0 || tlen == 0 {
			log.Println("DataBase and Table is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("G%d,%d,%d,%s%s%s", dblen, tlen, klen, dataBase, bucket, key)))
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func (c *Client) sendSet(database, bucket, key, value, storageType string) {
	var klen int
	var vlen int
	klen = len(key)
	vlen = len(value)
	if storageType == "cache" {
		_, err := c.Write([]byte(fmt.Sprintf("S%d,%d,%s%s", klen, vlen, key, value)))
		if err != nil {
			log.Println(err)
		}
	} else {
		dblen := len(database)
		tlen := len(bucket)
		if dblen == 0 || tlen == 0 {
			log.Println("DataBase and Table is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("S%d,%d,%d,%d,%s%s%s%s", dblen, tlen, klen, vlen, database, bucket, key, value)))
		if err != nil {
			log.Println(err)
		}
	}
}

func (c *Client) sendDel(database, bucket, key, storageType string) {
	var klen int
	klen = len(key)
	if storageType == "cache" {
		_, err := c.Write([]byte(fmt.Sprintf("D%d,%s", klen, key)))
		if err != nil {
			log.Println(err.Error())
		}
	} else {
		dblen := len(database)
		tlen := len(bucket)
		if dblen == 0 || tlen == 0 {
			log.Println("DataBase and Table is required not null!")
		}
		_, err := c.Write([]byte(fmt.Sprintf("D%d,%d,%d,%s", dblen, tlen, klen, key)))
		if err != nil {
			log.Println(err.Error())
		}
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
