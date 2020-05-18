package client

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/node/point"
	"strconv"
	"strings"
	"sync"
)

type tcpClient struct {
	serverAddr string
	mu sync.RWMutex
}


func NewClient(addr string) *tcpClient {
	return &tcpClient{serverAddr:addr}
}

type client struct {
	net.Conn
	r *bufio.Reader
}


func (tc *tcpClient) ExecuteWrite(writeList []*point.WritePoint) {
	c:=newClient(tc.serverAddr)
	for _,wp:=range writeList {
		  tc.run(c,wp)
	}
}

func newClient(server string) *client {
	conn, e := net.Dial("tcp", server)
	if e != nil {
		panic(e)
	}
	request := bufio.NewReader(conn)
	return &client{conn, request}
}

func(tc *tcpClient) run(c *client,wp *point.WritePoint) {
	 c.writeRequest(wp)
	 c.processWriteResponse(wp)
}


func(c *client) writeRequest(wp *point.WritePoint) {
	data,e:=proto.Marshal(wp)
	if e !=nil {
		log.Println(e)
	}
	dLen:=len(data)
	_,e=c.Write([]byte(fmt.Sprintf("S%d,%s",dLen,data)))
	if e !=nil {
		log.Println("client send write request failed !",e)
	}
}


func (c *client) proxyWriteRequest(data []byte) {
	dLen:=len(data)
	_,e:=c.Write([]byte(fmt.Sprintf("S%d,%s",dLen,data)))
	if e !=nil {
		log.Println("proxy client send write request failed !",e)
	}
}

func (c *client) processProxyWriteResponse() bool {
	op,e:=c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return false
	}
	if op == 'V' {
		v, e := c.recvResponse()
		if e != nil {
			log.Println("proxy client write response failed !", e)
			return false
		}
		if strings.Compare(string(v),"f") == 0 {
			log.Println("client write request failed !")
			return false
		}
		return true

	}
	return false
}


func (tc *tcpClient) ExecuteProxyWrite(data []byte) bool {
	c:=newClient(tc.serverAddr)
	c.proxyWriteRequest(data)
	return c.processProxyWriteResponse()
}

func (c *client) processWriteResponse(wp *point.WritePoint) {
	op,e:=c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return
	}
	if op == 'V' {
		v, e := c.recvResponse()
		if e != nil {
			log.Println("proxy client write response failed !", e)
		}
		if strings.Compare(string(v),"f") == 0 {
			log.Println("client write request failed !")
		}
	}
}

func (tc *tcpClient) ExecuteRead(rp *point.ReadPoint) {
	c:=newClient(tc.serverAddr)
	c.readRequest(rp)
	c.processReadResponse()
}


func (tc *tcpClient) ExecuteProxyRead(data []byte,rp chan *point.ReadPoint) {
	c:=newClient(tc.serverAddr)
	dLen:=len(data)
	_,e:=c.Write([]byte(fmt.Sprintf("P%d,%s",dLen,data)))
	if e !=nil {
		log.Println("client send proxy read request failed !",e)
	}
	c.processProxyReadResponse(rp)
}

func (c *client) processProxyReadResponse(rp chan *point.ReadPoint)  {
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return
	}
	switch op {
	case 'f':
		log.Println("client read proxy response failed !", e)
		return
	case 'V':
		v, e := c.recvResponse()
		if e != nil {
			log.Println("client read proxy response failed !", e)
			return
		}
		data:=&point.ReadPoint{}
		e=proto.Unmarshal(v,data)
		if e != nil {
			log.Println(" proxy readPoint deserialization failed ! ",e.Error())
			return
		}
		rp <-data
	}
	return
}

func (c *client) readRequest(rp *point.ReadPoint) {
	data,e:=proto.Marshal(rp)
	if e !=nil {
		log.Println(e.Error())
	}
	dLen:=len(data)
	_,e=c.Write([]byte(fmt.Sprintf("G%d,%s",dLen,data)))
	if e !=nil {
		log.Println("client send read request failed !",e)
	}
}

func (c *client) processReadResponse() {
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return
	}
	switch op {
	case 'f':
		log.Println("client read response failed !", e)
		return
	case 'V':
		v, e := c.recvResponse()
		if e != nil {
			log.Println("client read response failed !", e)
			return
		}
		data:=&point.ReadPoint{}
		e=proto.Unmarshal(v,data)
		if e != nil {
			log.Println(" readPoint deserialization failed ! ",e.Error())
			return
		}
		log.Println(data)
	}
	return
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