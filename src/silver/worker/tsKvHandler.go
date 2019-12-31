package worker

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/cmd"
	"silver/storage"
	"strconv"
	"strings"
	"sync"
)


func (s *Server) handleTsSetData(r *bufio.Reader,conn net.Conn) (*storage.WPoint,string,error){
	wp,e := parseWPoint(r)
	if e != nil {
		return nil,"",e
	}
	dataBase:=wp.DataBase
	tableName:=wp.TableName
	var tagKv string
	if wp.Tags != nil {
		for tagK,tagV:=range wp.Tags {
            tagKv+=tagK+tagV
		}
	}
	seriesKey:=dataBase+tableName+tagKv
	addr, ok := s.ShouldProcess(seriesKey)
	if !ok {
		aLen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",aLen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return nil,"",errors.New("redirect " + addr)
	}
	return wp,tagKv,nil
}


func parseWPoint(r *bufio.Reader) (*storage.WPoint,error) {
	l1,e:= readLen(r)
	dLen, e := strconv.Atoi(l1)
	buf := make([]byte,dLen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return nil,e
	}
	data:=&storage.WPoint{}
	e=proto.Unmarshal(buf,data)
	if e != nil {
		log.Println("wPoint Deserialization failed",e)
		return nil,e
	}
	return data,e
}


func (s *Server) handleTsGetData(r *bufio.Reader,conn net.Conn) (*cmd.GetCmd,string,error){
	  gc,e := parseGetCmd(r)
	  if e != nil {
		return nil,"",e
	  }
	  var tagKv string
	  if gc.Tags != nil {
		for tagK,tagV:=range gc.Tags {
			tagKv+=tagK+tagV
		}
	  }
	  seriesKey:=gc.DataBase+gc.TableName+tagKv
	  addr, ok := s.ShouldProcess(seriesKey)
	  if !ok {
		aLen:= len(addr)
		_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",aLen,addr)))
		if e != nil {
			log.Println(e.Error())
		}
		return nil,"",errors.New("redirect " + addr)
	  }
	return gc,tagKv,nil
}

func parseGetCmd(r *bufio.Reader) (*cmd.GetCmd,error) {
	l1, e := readLen(r)
	dLen, e := strconv.Atoi(l1)
	buf := make([]byte,dLen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return nil,e
	}
	data:=&cmd.GetCmd{}
	e=proto.Unmarshal(buf,data)
	if e != nil {
		log.Println("getCmd Deserialization failed",e)
		return nil,e
	}
	return data,e
}


func (s *Server) tsGet(ch chan chan  *storage.Value,conn net.Conn, r *bufio.Reader,dbCh chan []*bolt.DB) error {
	c:=make(chan *storage.Value)
	ch <-c
	gc,tagKv,e:=s.handleTsGetData(r,conn)
	if e !=nil {
		c<-&storage.Value{}
		log.Println(e)
		return e
	}
	kv,dbList:=s.ReadTsData(gc.DataBase,gc.TableName,tagKv,gc.FieldKey,gc.Tags,gc.StartTime,gc.EndTime)
	log.Println("dbList",dbList)
	value:=&storage.Value {
		Kv:                   kv,
	}
	c <- value
	if len(dbList) != 0 {
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
	}
	return nil
}

func (s *Server) tsSet(ch chan chan *storage.WPoint,conn net.Conn, r *bufio.Reader) error {
	c:=make(chan *storage.WPoint)
	ch <- c
	wp,tagKv,e := s.handleTsSetData(r,conn)
	if e != nil {
		c <- &storage.WPoint{}
		return e
	}
	if wp.Value != nil {
		for fieldKey,v:=range wp.Value {
			e:=s.WriteTsData(wp.DataBase,wp.TableName,tagKv,fieldKey,wp.Tags,v.Kv)
			if e != nil {
				log.Println(e)
			}
		}
		c <- &storage.WPoint{}
	}
	return nil
}

func tsGetReply(conn net.Conn,resultCh chan chan *storage.Value,dbCh chan []*bolt.DB) {
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
		e:=sendGetResponse(r,nil,conn,dbCh)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}

func sendGetResponse(value *storage.Value, err error, conn net.Conn,dbCh chan []*bolt.DB) error {
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
	_,e= conn.Write(append([]byte(fmt.Sprintf("V%d,%s,",len(data),data))))
   /*dbList:=<-dbCh
	var wg sync.WaitGroup
	for n,_:=range dbList {
		db:=dbList[n]
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			wg.Done()
			defer db.Close()
		}(&wg)
	}
	wg.Wait()*/
	return e
}


func tsSetReply(conn net.Conn,resultCh chan chan *storage.WPoint) {
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
		if !open {
			return
		}
		e:= sendSetResponse(r,nil,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
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

func sendSetResponse (wp *storage.WPoint,e error,conn net.Conn) error {
	if e != nil {
		errString := e.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	wPoint,e:=proto.Marshal(wp)
	data := fmt.Sprintf("V%d,%s", len(wPoint),wPoint)
	_,e= conn.Write(append([]byte(data)))
	return e
}


