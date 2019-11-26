package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"silver/cluster"
	"silver/storage"
	"strconv"
	"strings"
)

type Server struct {
	storage.Storage
	cluster.Node
}

func (s *Server) Listen() {
	l, e := net.Listen("tcp", ":12346")
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

func New(c storage.Storage, n cluster.Node) *Server {
	return &Server{c, n}
}

func (s *Server) readKey(r *bufio.Reader) (string, error) {
	db, table, key, e := parseGetData(r)
	if e != nil {
		return "", e
	}
	addr, ok := s.ShouldProcess(db + table + key)
	if !ok {
		return "", errors.New("redirect " + addr)
	}
	return key, nil
}

func (s *Server) readSetDataInfo(r *bufio.Reader) (string, string, string, []byte, error) {
	db, table, key, value, e := parseSetData(r)
	if e != nil {
		return "", "", "", nil, e
	}
	addr, ok := s.ShouldProcess(db + table + key)
	if !ok {
		//此处应该返回客户端，告知客户端处理的地址，客户端重新发送请求
		return "", "", "", nil, errors.New("redirect " + addr)
	}
	return db, table, key, value, nil
}

func (s *Server) readGetDataInfo(r *bufio.Reader) (string, string, string, error) {
	db, table, key, e := parseGetData(r)
	if e != nil {
		return "", "", "", e
	}
	addr, ok := s.ShouldProcess(db + table + key)
	if !ok {
		//此处应该返回客户端，告知客户端处理的地址，客户端重新发送请求
		return "", "", "", errors.New("redirect " + addr)
	}
	return db, table, key, nil
}

func readLen(r *bufio.Reader) (string, error) {
	tmp, e := r.ReadString(',')
	if tmp == "" {
		return "", nil
	}
	if e != nil {
		return "", e
	}
	return strings.ReplaceAll(tmp, ",", ""), nil
}

func parseSetData(r *bufio.Reader) (string, string, string, []byte, error) {
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
		return "", "", "", nil, e
	}
	db := string(buf)[:dblen]
	table := string(buf)[dblen : dblen+tblen]
	key := string(buf)[dblen+tblen : dblen+tblen+klen]
	value := buf[dblen+tblen+klen:]
	return db, table, key, value, nil
}

func parseGetData(r *bufio.Reader) (string, string, string, error) {
	l1, e := readLen(r)
	l2, e := readLen(r)
	l3, e := readLen(r)
	dblen, e := strconv.Atoi(l1)
	tblen, e := strconv.Atoi(l2)
	klen, e := strconv.Atoi(l3)
	buf := make([]byte, dblen+tblen+klen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return "", "", "", e
	}
	db := string(buf)[:dblen]
	table := string(buf)[dblen : dblen+tblen]
	key := string(buf)[dblen+tblen : dblen+tblen+klen]
	return db, table, key, nil
}

func sendResponse(value []byte, err error, conn net.Conn) error {
	if err != nil {
		errString := err.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	vlen := fmt.Sprintf("%d,", len(value))
	_, e := conn.Write(append([]byte(vlen), value...))
	return e
}

func (s *Server) get(conn net.Conn, r *bufio.Reader) error {
	database, table, k, e := s.readGetDataInfo(r)
	log.Println(database, table, k)
	if e != nil {
		return e
	}
	v, db, e := s.Get(database, table, k)
	defer db.Close()
	return sendResponse(v, e, conn)
}

func (s *Server) set(conn net.Conn, r *bufio.Reader) error {
	database, table, k, v, e := s.readSetDataInfo(r)
	if e != nil {
		return e
	}
	return sendResponse(nil, s.Set(database, table, k, v), conn)
}

func (s *Server) del(conn net.Conn, r *bufio.Reader) error {
	database, table, k, e := s.readGetDataInfo(r)
	if e != nil {
		return e
	}
	db, e := s.Del(database, table, k)
	defer db.Close()
	return sendResponse(nil, e, conn)
}

func (s *Server) process(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		op, e := r.ReadByte()
		if e != nil {
			if e != io.EOF {
				log.Println("close connection due to error:", e)
			}
			return
		}
		if op == 'S' {
			e = s.set(conn, r)
		} else if op == 'G' {
			e = s.get(conn, r)
		} else if op == 'D' {
			e = s.del(conn, r)
		} else {
			log.Println("close connection due to invalid operation:", op)
			return
		}
		if e != nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}
