package worker

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/client"
	"silver/cmd"
	"silver/storage"
	"strconv"
	"strings"
)


func (s *Server) handleTsSetData(r *bufio.Reader,conn net.Conn) (*storage.WPoint,string,error) {
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


func (s *Server) getMetaData(metaPrefix string) {
	e:=s.GetService(metaPrefix)
	if e !=nil {
		log.Println(e)
	}
	fmt.Println("Get metaData:",s.ServerList)
	select{

	}
}


func (s *Server) handleTsGetIndex (r *bufio.Reader,conn net.Conn) (*cmd.GetCmd,string,error){
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

	/*
	  1、根据database+tableName,获取元数据对应IP

	  2、然后根据对应IP和查询条件，查询索引，获取 tags , 进行数据查询，并返回结果

	*/
	nodeMap,ok:=s.ServerList[gc.DataBase+gc.TableName]
	if !ok {
		return nil,"",e
	}
	if nodeMap != nil {
		for addr,_:=range nodeMap {
			tc:= client.NewTsClient (addr, "tsStorage")
            // 去查询索引
            // 在节点获取tags，直接返回查询结果
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

func (s *Server) tsGet(ch chan chan  *storage.Value,conn net.Conn, r *bufio.Reader) error {
	c:=make(chan *storage.Value,0)
	ch <-c
	gc,tagKv,e:=s.handleTsGetData(r,conn)
	if e !=nil {
		c <- &storage.Value{}
		log.Println(e)
		return e
	}
	kv:=s.ReadTsData(gc.DataBase,gc.TableName,tagKv,gc.FieldKey,gc.Tags,gc.StartTime,gc.EndTime)
	if kv != nil {
		value:=&storage.Value {
			Kv:                   kv,
		}
		c <- value
	}
	return nil
}

func (s *Server) tsSet(ch chan chan *storage.WPoint,conn net.Conn, r *bufio.Reader) error {
	c:=make(chan *storage.WPoint,0)
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

func tsGetReply(conn net.Conn,resultCh chan chan *storage.Value) {
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
		e:=sendGetResponse(r,nil,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}

func sendGetResponse(value *storage.Value, err error, conn net.Conn) error {
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
	if value.Kv != nil {
		_,e= conn.Write(append([]byte(fmt.Sprintf("V%d,%s,",len(data),data))))
	}
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


