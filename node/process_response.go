package node

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"log"
	"net"
	"silver/node/point"
)

func writeResponse(conn net.Conn,writeResultCh chan chan bool) {
	defer conn.Close()
	for {
		c,open := <- writeResultCh
		if !open {
			return
		}
		r:= <- c
		e:= sendWriteResponse(r,conn)
		if e !=nil {
			log.Println("return write response failed !", e)
			return
		}
	}
}


func sendWriteResponse (r bool,conn net.Conn) error {
	var data string
	if r {
		data= fmt.Sprintf("V%d,%s", len("s"),"s")
	} else {
		data= fmt.Sprintf("V%d,%s", len("f"),"f")
	}
	if data != "" {
		_,e:= conn.Write([]byte(data))
		return e
	}
	return nil
}


func readResponse(conn net.Conn,readResultCh chan chan *point.ReadPoint) {
	defer conn.Close()
	for {
		c,open := <- readResultCh
		if !open {
			return
		}
		r := <- c
		e:= sendReadResponse(r,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}


func sendReadResponse(rp *point.ReadPoint,conn net.Conn) error {
   if rp !=nil && len(rp.Metrics) > 0 {
	   data,e:=proto.Marshal(rp)
	   if e !=nil {
		   log.Println(e.Error())
		   return e
	   }
	   dLen:=len(data)
	   _, e = conn.Write([]byte(fmt.Sprintf("V%d,%s",dLen,data)))
	   if e != nil {
		   log.Println("process send read response failed !",e)
		   return  e
	   }
   } else {
   	   _,e:= conn.Write([]byte(fmt.Sprintf("f%d,%s", len("f"),"f")))
	   if e != nil {
		   log.Println("process send read response failed !",e)
		   return  e
	   }
   }
   return  nil
}