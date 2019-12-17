package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/result"
	"strconv"
	"sync"
)

func (s *Server) readTsSetData(r *bufio.Reader,conn net.Conn) (string,string,string,string,[]byte,string,error){
	database,table,rowKey,key,value,dataTime,e := parseTsSetData(r)
	if e != nil {
		return "","","","",nil,"",e
	}
	addr, ok := s.ShouldProcess(database+table+rowKey+key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "","","","",nil,"",errors.New("redirect " + addr)
	}
	return database,table,rowKey,key,value,dataTime,nil
}

func parseTsSetData(r *bufio.Reader) (string,string,string,string,[]byte,string,error){
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
		return "", "", "","", nil,"",e
	}
	database := string(buf)[:dblen]
	table := string(buf)[dblen : dblen+tblen]
	rowKey := string(buf)[dblen+tblen : dblen+tblen+rklen]
	key:=string(buf)[dblen+tblen+rklen:dblen+tblen+rklen+klen]
	value := buf[dblen+tblen+rklen+klen:dblen+tblen+rklen+klen+vlen]
	dataTime:=string(buf)[dblen+tblen+rklen+klen+vlen:]
	return database,table,rowKey,key,value,dataTime,nil
}

func (s *Server) readTsGetData(r *bufio.Reader,conn net.Conn) (string,string,string,string,string,string,error) {
	database,table,rowKey,key,startTime,endTime,e := parseTsGetData(r)
	if e != nil {
		return "","","","","","",e
	}
	addr, ok := s.ShouldProcess(database+table+rowKey+key)
	if !ok {
		alen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",alen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return "","","","","","",errors.New("redirect " + addr)
	}
	return database,table,rowKey,key,startTime,endTime,nil
}

func parseTsGetData(r *bufio.Reader) (string,string,string,string,string,string,error){
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
		return "", "", "","","","",e
	}
	database := string(buf)[:dblen]
	table := string(buf)[dblen : dblen+tblen]
	rowKey := string(buf)[dblen+tblen : dblen+tblen+rklen]
	key:=string(buf)[dblen+tblen+rklen:dblen+tblen+rklen+klen]
	startTime:=string(buf)[dblen+tblen+rklen+klen:dblen+tblen+rklen+klen+stlen]
	endTime:=string(buf)[dblen+tblen+rklen+klen+stlen:]
	return database,table,rowKey,key,startTime,endTime,nil
}

func (s *Server) tsGet(ch chan chan *result.TsResult,conn net.Conn, r *bufio.Reader,dbCh chan []*bolt.DB) error {
	     c:=make(chan *result.TsResult)
	     ch <-c
	     database,table,rowKey,key,startTime,endTime,e := s.readTsGetData(r,conn)
	     if e !=nil {
	         c<-&result.TsResult{}
	         log.Println(e)
	         return e
	     }

	     tr:=&result.TsResult{
		DataBase:             database,
		TableName:            table,
		RowKey:               rowKey,
		Key:                  key,
		Data:                 make([]*result.TsField,0),
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
		}
	     st,_:= strconv.ParseInt(startTime, 10, 64)
	     et,_:=strconv.ParseInt(endTime, 10, 64)
	     tableFileList := s.GetStorageFile(database, table, st, et)
	     var mu sync.RWMutex
	     var wg sync.WaitGroup
	     var data []*result.TsField
	     var dbList []*bolt.DB
	     for i,_:=range tableFileList {
	     	 wg.Add(1)
	     	 tableFile:=tableFileList[i]
	     	 go func(dbs *[]*bolt.DB,reData *[]*result.TsField,wg *sync.WaitGroup) {
				 mu.Lock()
				 db:=s.OpenDB(tableFile)
				 *reData=append(*reData,s.GetTimeRangeData(db,rowKey,key,st,et)...)
				 *dbs=append(*dbs,db)
				 mu.Unlock()
				 wg.Done()
			}(&dbList,&data,&wg)
		 }
	     wg.Wait()
	     tr.Data=data
	     c <- tr
	     dbCh <-dbList
	return nil
}

func (s *Server) tsSet(ch chan chan *result.TsResult,conn net.Conn, r *bufio.Reader) error {
	c:=make(chan *result.TsResult)
	ch <- c
	database,table,rowKey,key,value,dataTime,e := s.readTsSetData(r,conn)
	    if e != nil {
	    	c <- &result.TsResult{}
		    return e
	    }
	go func() {
		dt,_:= strconv.ParseInt(dataTime, 10, 64)
		e:=sendResponse(nil, s.SetTSData(database,table,rowKey,key,value,dt),conn)
		if e != nil {
			log.Println(e)
		}
		c <- &result.TsResult{}
	}()
	return nil
}

func (s *Server) tsDel(ch chan chan *result.TsResult,conn net.Conn, r *bufio.Reader) error {
	   c:=make(chan *result.TsResult)
	   ch <- c
	   database,table,rowKey,key,startTime,endTime,e := s.readTsGetData(r,conn)
	    if e != nil {
	    	c <- &result.TsResult{}
		    return e
	    }
	   go func() {
		   st,_:= strconv.ParseInt(startTime, 10, 64)
		   et,_:=strconv.ParseInt(endTime, 10, 64)
		   db, e := s.DelTSData(database,table,rowKey,key,st,et)
		   if e !=nil {
		   	log.Println(e)
		   }
		   c <- &result.TsResult{}
		   defer db.Close()
	   }()
	   return nil
}

func tsReply(conn net.Conn,resultCh chan chan *result.TsResult,dbCh chan []*bolt.DB) {
	defer conn.Close()
	for {
		c,open := <- resultCh
		if !open {
			return
		}
		r := <- c
		if r == nil {
			return
		}
		if !open{
			return
		}
		e:=sendTsResponse(r,nil,conn,dbCh)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}

func sendTsResponse(value *result.TsResult, err error, conn net.Conn,dbCh chan []*bolt.DB) error {
	if err != nil {
		errString := err.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	data,e:=proto.Marshal(value)
	if e !=nil {
		log.Println(e.Error())
		return e
	}
	_,e= conn.Write(append([]byte(fmt.Sprintf("V%d,%s,",len(data),string(data)))))
	dbList:=<-dbCh
	var wg sync.WaitGroup
	for n,_:=range dbList {
		db:=dbList[n]
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			wg.Done()
			defer db.Close()
		}(&wg)
	}
	wg.Wait()
	return e
}

