package utils

import (
	"reflect"
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
