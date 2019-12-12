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

func (s *Server) readDbSetData(r *bufio.Reader,conn net.Conn) (string,string,string,[]byte,error){
	database,table,key,value,e := parseDbSetData(r)
	if e != nil {
		return "","","",nil,e
	}
	addr, ok := s.ShouldProcess(database+table+key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "","","",nil,errors.New("redirect " + addr)
	}
	return database,table,key,value,nil
}

func parseDbSetData(r *bufio.Reader) (string,string,string,[]byte,error){
	l1, e := readLen(r)
	l2, e := readLen(r)
	l3, e := readLen(r)
	l4, e := readLen(r)
	dblen, e := strconv.Atoi(l1)
	tblen, e := strconv.Atoi(l2)
	klen, e := strconv.Atoi(l3)
	vlen, e := strconv.Atoi(l4)
	buf := make([]byte, dblen+tblen+klen+vlen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return "", "", "",nil,e
	}
	database := string(buf)[:dblen]
	table := string(buf)[dblen : dblen+tblen]
	key := string(buf)[dblen+tblen : dblen+tblen+klen]
	value := buf[dblen+tblen+klen:]
	return database,table,key,value,nil
}

func (s *Server) readDbGetData(r *bufio.Reader,conn net.Conn)  (string,string,string,error) {
	database,table,key,e := parseDbGetData(r)
	if e != nil {
		return "","","",e
	}
	addr, ok := s.ShouldProcess(database+table+key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "","","",errors.New("redirect " + addr)
	}
	return database,table,key,nil
}

func parseDbGetData(r *bufio.Reader) (string,string,string,error){
	l1, e := readLen(r)
	l2, e := readLen(r)
	l3, e := readLen(r)
	dblen, e := strconv.Atoi(l1)
	tblen, e := strconv.Atoi(l2)
	klen, e := strconv.Atoi(l3)
	buf := make([]byte, dblen+tblen+klen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return "","","",e
	}
	database := string(buf)[:dblen]
	table := string(buf)[dblen : dblen+tblen]
	key := string(buf)[dblen+tblen : dblen+tblen+klen]
	return database,table,key,nil
}

func (s *Server) dbGet(conn net.Conn, r *bufio.Reader) error {
	database,table,key,e := s.readDbGetData(r,conn)
	if e != nil {
		return e
	}
	v, db, e := s.GetDBandKV(database, table, key)
	defer db.Close()
	return sendResponse(v, e, conn)
}

func (s *Server) dbSet(conn net.Conn, r *bufio.Reader) error {
	database,table,key,value,e := s.readDbSetData(r,conn)
	if e != nil {
		return e
	}
	return sendResponse(nil, s.SetDBandKV(database, table,key,value), conn)
}

func (s *Server) dbDel(conn net.Conn, r *bufio.Reader) error {
	database,table,key,e := s.readDbGetData(r,conn)
	if e != nil {
		return e
	}
	db, e := s.DelDBandKV(database, table, key)
	defer db.Close()
	return sendResponse(nil, e, conn)
}

func dbReply(conn net.Conn,resultCh chan chan *result.DbResult) {
	defer conn.Close()
	for {
		c,open := <- resultCh
		if !open {
			return
		}
		r:=<-c
		e:=sendDbResponse(r,nil,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}

func sendDbResponse(dbResult *result.DbResult,err error, conn net.Conn) error{
	if err != nil {
		errString := err.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	data,_:=proto.Marshal(dbResult)
	_, e := conn.Write([]byte(fmt.Sprintf("V%d,%s", len(data), data)))
	return e
}