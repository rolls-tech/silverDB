package utils

import (
	"log"
	"reflect"
	"time"
)

const (
     NS =  iota
     US
     MS
	 S
     )

const (
	Float  = iota
    Double
	Int
	Long
	Bool
    )

const RFC3339Nano = "2006-01-02 15:04:05.999999999Z07:00"


func TransDataType (data interface{}) ([]byte,int32) {
	b:=make([]byte,0)
	var dataType int32
	switch reflect.TypeOf(data).Name() {
	case "float32":
		b=Float32ToByte(data.(float32))
		dataType=Float
		break
	case "float64":
		b=Float64ToByte(data.(float64))
		dataType=Double
		break
	case "int32":
		b=Int32ToByte(data.(int32))
		dataType=Int
		break
	case "int64":
		b=Int64ToByte(data.(int64))
		dataType=Long
		break
	case "bool":
		if data == true {
			b=append(b,1)
		} else {
		   b=append(b,0)
		}
		dataType=Bool
		break
	case "int":
		b=IntToByte(data.(int))
		dataType=Int
		break
	}
	return b,dataType
}


func TransByteToFloat64(dataType int32, data []byte) float64 {
	var td float64
	switch dataType {
	case Float:
		if len(data) == 4 {
			d:=make([]byte,4)
			d=append(d,data...)
			td=ByteToFloat64(d)
		}
        break
	case Double:
		if len(data) == 8 {
			td=ByteToFloat64(data)
		}
		break
	case Int:
		if len(data) == 4 {
			d:=make([]byte,4)
			d=append(d,data...)
			td=ByteToFloat64(d)
		}
		break
	case Long:
		if len(data) == 8 {
			td=ByteToFloat64(data)
		}
		break
	case Bool:
		if len(data) == 1 {
			d:=make([]byte,7)
			d=append(d,data...)
			td=ByteToFloat64(d)
		}
		break
	default:
         log.Println("not support ")
		}
	return td
}


func TransByteToData(dataType int32,data []byte) interface{} {
	var d interface{}
	switch dataType {
	case Double:
		d=ByteToFloat64(data)
		break
	case Float:
		d=ByteToFloat32(data[4:])
		break
	case Long:
		d=ByteToInt64(data)
		break
	case Int:
		d=ByteToInt32(data[4:])
		break
	case Bool:
        if ByteToInt32(data[4:]) == 0 {
          d=false
		} else {
		  d=true
		}
        break
	}
	return d
}


func TransTimestamp (originTime int64,precision int32) string {
	switch precision {
	case S:
		tt:=time.Unix(0,originTime*1e9).Format(RFC3339Nano)
		return tt
	case MS:
		tt:=time.Unix(0,originTime*1e6).Format(RFC3339Nano)
		return tt
	case US:
		tt:=time.Unix(0,originTime*1e3).Format(RFC3339Nano)
		return tt
	case NS:
		tt:=time.Unix(0,originTime).Format(RFC3339Nano)
		return tt
	default:
		log.Println("Time format not supported ! ",originTime , precision)
		return ""
	}
}