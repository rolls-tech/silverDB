package node

import (
	"bufio"
	"io"
	"log"
	"net"
	"silverDB/node/metastore"
	"time"
)

type Server struct {
	Storage
	Node
	*metastore.Discovery
}

func (s *Server) Listen(addr string) {
	l, e := net.Listen("tcp", addr)
	defer l.Close()
	if e != nil {
		panic(e)
	}
	for {
		c, e := l.Accept()
		if e != nil {
			panic(e)
		}
		go s.process(c)
	}
}

func NewWorker(storage Storage, node Node, discovery *metastore.Discovery,chanSize int32) *Server {
	return &Server{storage, node,discovery}
}


func (s *Server) process(conn net.Conn) {
		request := bufio.NewReader(conn)
	    writeResultCh:=make(chan chan bool,0)
	    defer close(writeResultCh)
		go writeResponse(conn,writeResultCh)
	    go s.metaData()
		for {
			op, e := request.ReadByte()
			if e != nil {
				if e != io.EOF {
					log.Println("close connection due to error:", e)
				}
				return
			}
			if op == 'S' {
				e=s.writeRequest(writeResultCh,conn,request)
				if e !=nil {
					log.Println(s.Addr() + "write request failed ",e)
					return
				}
				log.Println(s.Addr() +" received write request")
			}else if op == 'D' {
				log.Println("D")
			}else if op == 'G' {
				log.Println("G")
			}
		}
}


func (s *Server) writeRequest(ch chan chan bool,conn net.Conn, request *bufio.Reader) error {
	    c:=make(chan bool,0)
	    ch <- c
	    wp,tagKv,buf,e:=s.resolveWriteRequest(conn,request)
	    if e !=nil {
	    	log.Println(e.Error())
			return e
		}
	    if wp != nil {
				e:=s.WriteTsData(wp,tagKv,buf,len(buf),time.Now().Unix(),0)
				if e != nil {
					log.Println(s.Addr()+ " write data failed !" ,e)
					c <- false
				} else {
					c <- true
				}

		}
	    return e
}








