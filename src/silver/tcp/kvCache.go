package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/result"
	"strconv"
)

func (s *Server) readKvSetData(r *bufio.Reader,conn net.Conn) (string,[]byte,error){
	key,value,e := parseKvSetData(r)
	if e != nil {
		return "",nil,e
	}
	addr, ok := s.ShouldProcess(key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "",nil,errors.New("redirect " + addr)
	}
	return key,value,nil
}

func parseKvSetData(r *bufio.Reader) (string,[]byte,error){
	l1, e := readLen(r)
	l2, e := readLen(r)
	klen, e := strconv.Atoi(l1)
	vlen, e := strconv.Atoi(l2)
	buf := make([]byte, klen+vlen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return "",nil,e
	}
	key := string(buf)[:klen]
	value := buf[klen:klen+vlen]
	return key,value,nil
}

func (s *Server) readKvGetData(r *bufio.Reader,conn net.Conn) (string,error){
	key,e := parseKvGetData(r)
	if e != nil {
		return "",e
	}
	addr, ok := s.ShouldProcess(key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "",errors.New("redirect " + addr)
	}
	return key,nil
}

func parseKvGetData(r *bufio.Reader) (string,error) {
	l1, e := readLen(r)
	klen, e := strconv.Atoi(l1)
	buf := make([]byte, klen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return "",e
	}
	key := string(buf)[:klen]
	return key,nil
}

func (s *Server) kvGet(conn net.Conn, r *bufio.Reader) error {
	key,e := s.readKvGetData(r,conn)
	if e != nil {
		return e
	}
	v,e := s.GetKv(key)
	return sendResponse(v, e, conn)
}

func (s *Server) kvSet(conn net.Conn, r *bufio.Reader) error {
	key,value,e := s.readKvSetData(r,conn)
	if e != nil {
		return e
	}
	return sendResponse(nil, s.SetKv(key,value), conn)
}

func (s *Server) kvDel(conn net.Conn, r *bufio.Reader) error {
	key,e := s.readKvGetData(r,conn)
	if e != nil {
		return e
	}
	return sendResponse(nil,s.DelKv(key), conn)
}

func kvReply(conn net.Conn,resultCh chan chan *result.KvResult) {
	defer conn.Close()
	for {
		c,open := <- resultCh
		if !open {
			return
		}
		r:=<-c
		e:=sendKvResponse(r,nil,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}

func sendKvResponse(kvResult *result.KvResult,err error, conn net.Conn) error{
	if err != nil {
		errString := err.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	data,_:=proto.Marshal(kvResult)
	_, e := conn.Write([]byte(fmt.Sprintf("V%d,%s", len(data), data)))
	return e
}