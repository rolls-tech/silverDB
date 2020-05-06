package node

import (
	"fmt"
	"log"
	"net"
)

func writeResponse(conn net.Conn,writeResultCh chan chan bool) {
	defer conn.Close()
	for {
		c,open := <- writeResultCh
		if !open {
			return
		}
		r := <- c
		e:= sendWriteResponse(r,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}


func sendWriteResponse (r bool,conn net.Conn) error {
	var data string
	if r {
		data= fmt.Sprintf("V%d,%s",1,"s")
	} else {
		data= fmt.Sprintf("V%d,%s",1,"f")
	}
	_,e:= conn.Write(append([]byte(data)))
	return e
}