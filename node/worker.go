package node

import (
	"bufio"
	"io"
	"log"
	"net"
	"silver/metastore"
	"silver/node/client"
	"silver/node/point"
	"strings"
)

type Server struct {
	Storage
	Node
	*metastore.Listener
}

func (s *Server) Listen(addr string) {
	l, e := net.Listen("tcp", addr)
	defer l.Close()
	if e != nil {
		panic(e)
	}
	go s.metaDataService()
	go s.nodeDataService()
	for {
		c, e := l.Accept()
		if e != nil {
			panic(e)
		}
		go s.process(c)
	}
}

func NewWorker(storage Storage, node Node, listener1 *metastore.Listener,chanSize int32) *Server {
	return &Server{storage, node,listener1}
}


func (s *Server) process(conn net.Conn) {
	    request := bufio.NewReader(conn)
	    writeResultCh:=make(chan chan bool,100)
	    readResultCh:=make(chan chan *point.ReadPoint,0)
	    defer close(writeResultCh)
	    defer close(readResultCh)
		go writeResponse(conn,writeResultCh)
	    go readResponse(conn,readResultCh)
		for {
			op, e := request.ReadByte()
			if e != nil {
				if e != io.EOF {
					log.Println("close connection due to error: ", e)
				}
				return
			}
			if op == 'S' {
				s.writeRequest(writeResultCh,request)
			}else if op == 'D' {
				log.Println("D")
			}else if op == 'G' {
				e=s.readRequest(readResultCh,conn,request)
			} else if op == 'P' {
				e=s.proxyReadRequest(readResultCh,conn,request)
			}else {
				log.Println("unsupported operate type ",string(op))
				return
			}
		}
}

func (s *Server) writeRequest(ch chan chan bool,request *bufio.Reader) {
	    c:=make(chan bool,0)
	    ch <- c
	    wp,tagKv,buf,e:=s.resolveWriteRequest(request,c)
	    if e != nil {
	    	log.Println(e)
			return
		}
	    if wp != nil  {
	    	    go func() {
					e=s.WriteTsData(wp,tagKv,buf)
					if e != nil {
						log.Println(s.Addr()+ " write data failed !" ,e)
						c <- false
					} else {
						c <- true
					}
				}()
		}
}

func (s *Server) readRequest(ch chan chan *point.ReadPoint,conn net.Conn, request *bufio.Reader) error {
	rp,buf,tagKv,addrList,e:=s.resolverReadRequest(conn, request)
	if e !=nil {
		log.Println("parse read request info failed !",e)
	}
	if addrList != nil && len(addrList) > 0 {
		c:=make(chan *point.ReadPoint,len(addrList))
		ch <- c
		for addr,_:=range addrList {
			if strings.Compare(addr, s.StorageAddr()) == 0 {
				if rp != nil {
					s.ReadTsData(rp, tagKv, c)
				}
			} else {
				go func() {
					proxy := client.NewClient(addr)
					proxy.ExecuteProxyRead(buf,c)
				}()
			}
		}
	}
	return e
}

func (s *Server) proxyReadRequest(ch chan chan *point.ReadPoint,conn net.Conn,request *bufio.Reader) error {
	c:=make(chan *point.ReadPoint,0)
	ch <- c
	rp,tagKv,e:=s.resolverProxyRequest(request)
	if e !=nil {
		log.Println("parse read request info failed !",e)
	}

	if rp !=nil {
		s.ReadTsData(rp,tagKv,c)
	}
	return e
}








