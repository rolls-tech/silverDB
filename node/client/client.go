package client

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silverDB/node/point"
	"strconv"
	"strings"
)

type tcpClient struct {
	serverAddr string
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
	c, e := net.Dial("tcp", server)
	if e != nil {
		panic(e)
	}
	r := bufio.NewReader(c)
	return &client{c, r}
}

func(tc *tcpClient) run(c *client,wp *point.WritePoint) {
	c.writeRequest(wp)
	c.processWriteResponse(wp)
}

func(c *client) writeRequest(wp *point.WritePoint) {
	data,e:=proto.Marshal(wp)
	if e !=nil {
		log.Println(e.Error())
		return
	}
	dLen:=len(data)
	_, err := c.Write([]byte(fmt.Sprintf("S%d,%s",dLen,data)))
	if err != nil {
		log.Println("client write request failed !",err)
	}
}


func (c *client) processWriteResponse(wp *point.WritePoint) {
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return
	}
	switch op {
	case 'R':
		v, e := c.recvResponse()
		if e != nil {
			log.Println("client redirect addr failed !", e)
			return
		}
		if v != nil {
			/*redirect := strings.Split(string(v), ":")
			addr := redirect[0]*/
			c:=newClient("127.0.0.1:12348")
			c.writeRequest(wp)
			c.processWriteResponse(wp)
			return
		}
	case 'V':
		v, e := c.recvResponse()
		if e != nil {
			log.Println("client write response failed !", e)
			return
		}
		if strings.Compare(string(v),"f") == 0 {
			log.Println("client write failed !")
			return
		}
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

