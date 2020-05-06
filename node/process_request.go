package node

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"net"
	"silverDB/node/point"
	"silverDB/utils"
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
			tagKv+=tags.TagK+tags.TagV
		}
	}
	seriesKey:=dataBase+tableName+tagKv
	addr, ok := s.ShouldProcess(seriesKey)
	if !ok {
		log.Println(seriesKey)
		if addr !="" {
			aLen:= len(addr)
			log.Println("process_request "+addr)
			_, e := conn.Write([]byte(fmt.Sprintf("R%d,%s",aLen,addr)))
			if e != nil {
				log.Println(e.Error())
			}
			return wp,tagKv,buf,errors.New("redirect " + addr)
		} else {
			log.Println("redirect addr is nil")
			return wp,tagKv,buf,nil
		}
	}
	return wp,tagKv,buf,nil
}

func (s *Server) writePoint(request *bufio.Reader) (*point.WritePoint,[]byte,error){
	l1,e:= readLen(request)
	if e !=nil {
		log.Println("not support message format !",e.Error())
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

func (s *Server) metaData() {
	e:=s.MetaDataService()
	if e !=nil {
		log.Println(e)
	}
	fmt.Println("metaData: ",s.ServerList)
	select {

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





