package node

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silver/node/point"
	"silver/utils"
	"sort"
	"strconv"
	"strings"
)


func (s *Server) resolveWriteRequest(conn net.Conn, request *bufio.Reader) (*point.WritePoint,string,[]byte,error){
    wp,buf,e:=s.writePoint(request)
	if e != nil {
		return wp,"",buf,e
	}
	dataBase:=wp.DataBase
	tableName:=wp.TableName
	var tagKv string
	if wp.Tags != nil {
		st := utils.NewSortTags(wp.Tags)
		sort.Sort(st)
		for _, tags := range st {
			temp:=tags.TagK+"="+tags.TagV+";"
			tagKv+=temp
		}
	}
	seriesKey:=dataBase+tableName+tagKv
	addr, ok := s.ShouldProcess(seriesKey)
	if !ok {
		if addr !="" {
			aLen:= len(addr)
			_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",aLen,addr)))
			if e != nil {
				return wp, tagKv, buf, e
			}
		}
		return wp,tagKv,buf,errors.New("redirect addr is "+addr)
	}
	return wp,tagKv,buf,nil
}

func (s *Server) resolverReadRequest(conn net.Conn, request *bufio.Reader) (*point.ReadPoint,string,map[string]bool,error) {
	rp,e := s.readPoint(request)
	var tagKv string
	if e != nil {
		return rp,tagKv,nil,e
	}
	if rp.Tags != nil {
		st := utils.NewSortTags(rp.Tags)
		sort.Sort(st)
		for _, tags := range st {
			temp:=tags.TagK+tags.TagV
			tagKv+=temp
		}
	}
	databaseName:=rp.DataBase
	tableName:=rp.TableName
	addrList,ok:=s.LocalMeta[databaseName+tableName]
	if !ok {
		log.Println("not find meta data ",databaseName,tableName)
		_, e := conn.Write([]byte(fmt.Sprintf("M%d,%s", len("f"),"f")))
		if e != nil {
			log.Println("client meta data return failed",e.Error())
			return rp,tagKv,addrList,e
		}
	}
	return rp,tagKv,addrList,e
}

func (s *Server) resolverProxyRequest(request *bufio.Reader) (*point.ReadPoint,string,error) {
	var tagKv string
	rp,e:=s.readPoint(request)
	if e !=nil {
		return rp,tagKv,e
	}
	if rp.Tags != nil {
		st := utils.NewSortTags(rp.Tags)
		sort.Sort(st)
		for _, tags := range st {
			tagKv+=tags.TagK+tags.TagV
		}
	}
	return rp,tagKv,nil
}


func (s *Server) readPoint(request *bufio.Reader) (*point.ReadPoint,error) {
	l1,e:= readLen(request)
	if e !=nil {
		log.Println("not support message format !",e.Error())
	}
	dLen, e := strconv.Atoi(l1)
	buf := make([]byte,dLen)
	_, e = io.ReadFull(request, buf)
	if e != nil {
		return nil,e
	}
	data:=&point.ReadPoint{}
	e=proto.Unmarshal(buf,data)
	if e != nil {
		log.Println(" readPoint deserialization failed! ",e.Error())
		return nil,e
	}
	return data,e

}


func (s *Server) writePoint(request *bufio.Reader) (*point.WritePoint,[]byte,error){
	l1,e:= readLen(request)
	if e !=nil {
		log.Println("not support message format !",e.Error())
		return nil,nil,e
	}
	dLen, e := strconv.Atoi(l1)
	buf := make([]byte,dLen)
	_, e = io.ReadFull(request, buf)
	if e != nil {
		return nil,nil,e
	}
	data:=&point.WritePoint{}
	e=proto.Unmarshal(buf,data)
	if e != nil {
		log.Println(" writePoint deserialization failed! ",e.Error())
		return nil,nil,e
	}
	return data,buf,e
}

func (s *Server) metaDataService() {
	e:=s.MetaDataService()
	if e !=nil {
		log.Println(e)
	}
}


func readLen(r *bufio.Reader) (string, error) {
	tmp, e := r.ReadString(',')
	if tmp == ""  {
		return "", nil
	}
	if e != nil {
		log.Println("parse request failed ! ",e.Error())
		return "", e
	}
	return strings.ReplaceAll(tmp,",",""),nil
}





