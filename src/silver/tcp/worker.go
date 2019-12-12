package tcp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"silver/cluster"
	"silver/result"
	"silver/storage"
	"strconv"
	"strings"
)

type Server struct {
	storage.Storage
	cluster.Node
}

func (s *Server) Listen(storageTyp,addr string) {
	l, e := net.Listen("tcp", addr)
	if e != nil {
		panic(e)
	}
	for {
		c, e := l.Accept()
		if e != nil {
			panic(e)
		}
		go s.process(c,storageTyp)
	}
}

func New(c storage.Storage, n cluster.Node) *Server {
	return &Server{c, n}
}

/*
func (s *Server) readSetDataInfo(r *bufio.Reader,conn net.Conn) (string, string, string,string, []byte,string,string,error) {
	database,table,rowKey,key,value,dataTime,st,e := parseSetData(r)
	if e != nil {
		return "", "", "", "",nil,"",st,e
	}
	addr, ok := s.ShouldProcess(database+table+rowKey+key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "", "", "", "",nil,"",st,errors.New("redirect " + addr)
	}
	return database,table,rowKey,key,value,dataTime,st,nil
}

func (s *Server) readGetDataInfo(r *bufio.Reader,conn net.Conn) (string, string, string,string,string,string,string,error) {
	database,table,rowKey,key,startTime,endTime,st,e := parseGetData(r)
	if e != nil {
		return "", "", "","","","",st,e
	}
	addr, ok := s.ShouldProcess(database+table+rowKey+key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "", "", "","","","",st,errors.New("redirect " + addr)
	}
	return database,table,rowKey,key,startTime,endTime,st,nil
}
*/
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

func parseSetData(r *bufio.Reader) (string,string,string,string,[]byte,string,string,error) {
	t, e := r.ReadByte()
	switch t {
	case 'c':
		l1, e := readLen(r)
		l2, e := readLen(r)
		klen, e := strconv.Atoi(l1)
		vlen, e := strconv.Atoi(l2)
		buf := make([]byte, klen+vlen)
		_, e = io.ReadFull(r, buf)
		if e != nil {
			return "", "", "","", nil,"","c",e
		}
		key := string(buf)[:klen]
		value := buf[klen:klen+vlen]
		return "","","",key,value,"","c",nil
	case 'b':
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
			return "", "", "","", nil,"","b",e
		}
		database := string(buf)[:dblen]
		table := string(buf)[dblen : dblen+tblen]
		key := string(buf)[dblen+tblen : dblen+tblen+klen]
		value := buf[dblen+tblen+klen:]
		return database, table,"",key,value,"","b",nil
	case 't':
		l1, e := readLen(r)
		l2, e := readLen(r)
		l3, e := readLen(r)
		l4, e := readLen(r)
		l5, e := readLen(r)
		l6, e := readLen(r)
		dblen, e := strconv.Atoi(l1)
		tblen, e := strconv.Atoi(l2)
		rklen, e := strconv.Atoi(l3)
		klen, e := strconv.Atoi(l4)
		vlen,e:=strconv.Atoi(l5)
		dtlen,e:=strconv.Atoi(l6)
		buf := make([]byte, dblen+tblen+rklen+klen+vlen+dtlen)
		_, e = io.ReadFull(r, buf)
		if e != nil {
			return "", "", "","", nil,"","t",e
		}
		database := string(buf)[:dblen]
		table := string(buf)[dblen : dblen+tblen]
		rowKey := string(buf)[dblen+tblen : dblen+tblen+rklen]
		key:=string(buf)[dblen+tblen+rklen:dblen+tblen+rklen+klen]
		value := buf[dblen+tblen+rklen+klen:dblen+tblen+rklen+klen+vlen]
		dataTime:=string(buf)[dblen+tblen+rklen+klen+vlen:]
		return database,table,rowKey,key,value,dataTime,"t",nil
	default:
		log.Println("Unknown Storage Type")
		return "", "", "","", nil,"","",e
	}
}

func parseGetData(r *bufio.Reader) (string,string,string,string,string,string,string,error) {
	t, e := r.ReadByte()
	switch t {
	case 'c':
		l1, e := readLen(r)
		klen, e := strconv.Atoi(l1)
		buf := make([]byte, klen)
		_, e = io.ReadFull(r, buf)
		if e != nil {
			return "", "", "","","","","c",e
		}
		key := string(buf)[:klen]
		return "","","",key,"","","c",nil
	case 'b':
		l1, e := readLen(r)
		l2, e := readLen(r)
		l3, e := readLen(r)
		dblen, e := strconv.Atoi(l1)
		tblen, e := strconv.Atoi(l2)
		klen, e := strconv.Atoi(l3)
		buf := make([]byte, dblen+tblen+klen)
		_, e = io.ReadFull(r, buf)
		if e != nil {
			return "", "", "","","","","b",e
		}
		database := string(buf)[:dblen]
		table := string(buf)[dblen : dblen+tblen]
		key := string(buf)[dblen+tblen : dblen+tblen+klen]
		return database, table,"",key,"","","b",nil
	case 't':
		l1, e := readLen(r)
		l2, e := readLen(r)
		l3, e := readLen(r)
		l4, e := readLen(r)
		l6, e := readLen(r)
		l7, e := readLen(r)
		dblen, e := strconv.Atoi(l1)
		tblen, e := strconv.Atoi(l2)
		rklen, e := strconv.Atoi(l3)
		klen, e := strconv.Atoi(l4)
		stlen,e:=strconv.Atoi(l6)
		etlen,e:=strconv.Atoi(l7)
		buf := make([]byte, dblen+tblen+rklen+klen+stlen+etlen)
		_, e = io.ReadFull(r, buf)
		if e != nil {
			return "", "", "","","","","t",e
		}
		database := string(buf)[:dblen]
		table := string(buf)[dblen : dblen+tblen]
		rowKey := string(buf)[dblen+tblen : dblen+tblen+rklen]
		key:=string(buf)[dblen+tblen+rklen:dblen+tblen+rklen+klen]
		startTime:=string(buf)[dblen+tblen+rklen+klen:dblen+tblen+rklen+klen+stlen]
		endTime:=string(buf)[dblen+tblen+rklen+klen+stlen:]
		return database,table,rowKey,key,startTime,endTime,"t",nil
	default:
		log.Println("Unknown Storage Type")
		return "", "", "","","","","",e
	}
}

func sendResponse(value []byte, err error, conn net.Conn) error {
	if err != nil {
		errString := err.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	data := fmt.Sprintf("V%d,%s", len(value),value)
	_, e := conn.Write(append([]byte(data)))
	return e
}


/*
func (s *Server) get(conn net.Conn, r *bufio.Reader) error {
	database,table,rowKey,key,startTime,endTime,st,e := s.readGetDataInfo(r,conn)
	if e != nil {
		return e
	}
	switch st {
	case "c":
		v,e := s.GetKv(key)
		return sendResponse(v, e, conn)
	case "b":
		v, db, e := s.GetDBandKV(database, table, key)
		defer db.Close()
		return sendResponse(v, e, conn)
	case "t":
		st,_:= strconv.ParseInt(startTime, 10, 64)
		et,_:=strconv.ParseInt(endTime, 10, 64)
		v, db, e := s.GetTimeRangeData(database,table,rowKey,key,st,et)
		defer db.Close()
		return sendTsResponse(v, e, conn)
	default:
		return nil
	}
}

func (s *Server) set(conn net.Conn, r *bufio.Reader) error {
	database,table,rowKey,key,value,dataTime,st,e := s.readSetDataInfo(r,conn)
	if e != nil {
		return e
	}
	switch st {
	case "c":
		return sendResponse(nil, s.SetKv(key,value), conn)
	case "b":
		return sendResponse(nil, s.SetDBandKV(database, table,key,value), conn)
	case "t":
		dt,_:= strconv.ParseInt(dataTime, 10, 64)
		return sendResponse(nil, s.SetTSData(database,table,rowKey,key,value,dt),conn)
	default:
		return nil
	}
}

func (s *Server) del(conn net.Conn, r *bufio.Reader) error {
	database,table,rowKey,key,startTime,endTime,st,e := s.readGetDataInfo(r,conn)
	if e != nil {
		return e
	}
	switch st {
	case "c":
		e := s.DelKv(key)
		return sendResponse(nil,e, conn)
	case "b":
		db, e := s.DelDBandKV(database, table, key)
		defer db.Close()
		return sendResponse(nil, e, conn)
	case "t":
		st,_:= strconv.ParseInt(startTime, 10, 64)
		et,_:=strconv.ParseInt(endTime, 10, 64)
		db, e := s.DelTSData(database,table,rowKey,key,st,et)
		defer db.Close()
		return sendResponse(nil,e,conn)
	default:
		return nil
	}

}
*/



func (s *Server) process(conn net.Conn,storageTyp string) {
	switch storageTyp {
	case "kvCache":
		r := bufio.NewReader(conn)
		resultCh:=make(chan chan *result.KvResult,5000)
		defer close(resultCh)
		go kvReply(conn,resultCh)
		for {
			op, e := r.ReadByte()
			if e != nil {
				if e != io.EOF {
					log.Println("close connection due to error:", e)
				}
				return
			}
			if op == 'S' {
				e = s.kvSet(conn, r)
			} else if op == 'G' {
				e = s.kvGet(conn, r)
			} else if op == 'D' {
				e = s.kvDel(conn, r)
			} else {
				log.Println("close connection due to invalid operation:", op)
				return
			}
			if e != nil {
				log.Println("close connection due to error:", e)
				return
			}
		}
	case "dbStorage":
		r := bufio.NewReader(conn)
		resultCh:=make(chan chan *result.DbResult,5000)
		defer close(resultCh)
		go dbReply(conn,resultCh)
		for {
			op, e := r.ReadByte()
			if e != nil {
				if e != io.EOF {
					log.Println("close connection due to error:", e)
				}
				return
			}
			if op == 'S' {
				e = s.dbSet(conn, r)
			} else if op == 'G' {
				e = s.dbGet(conn, r)
			} else if op == 'D' {
				e = s.dbDel(conn, r)
			} else {
				log.Println("close connection due to invalid operation:", op)
				return
			}
			if e != nil {
				log.Println("close connection due to error:", e)
				return
			}
		}
	case "tsStorage":
		r := bufio.NewReader(conn)
		resultCh:=make(chan chan *result.TsResult,5000)
		defer close(resultCh)
		go tsReply(conn,resultCh)
		for {
			op, e := r.ReadByte()
			if e != nil {
				if e != io.EOF {
					log.Println("close connection due to error:", e)
				}
				return
			}
			if op == 'S' {
				e = s.tsSet(resultCh,conn, r)
			} else if op == 'G' {
				e = s.tsGet(resultCh,conn,r)
			} else if op == 'D' {
				e = s.tsDel(resultCh,conn, r)
			} else {
				log.Println("close connection due to invalid operation:", op)
				return
			}
			if e != nil {
				log.Println("close connection due to error:", e)
				return
			}
		}
	default:
		 log.Println("Not Supported Storage Type",storageTyp)
	}
}

