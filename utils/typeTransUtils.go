package utils

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"time"
)

const RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"

func Float64ToByte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}


func ByteToFloat64(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}


func Int64ToByte(num int64) []byte{
	var buffer bytes.Buffer
	err :=binary.Write(&buffer,binary.BigEndian,num)
	if err !=nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func IntToByte(num int) []byte {
	x := int32(num)
	var buffer bytes.Buffer
	err :=binary.Write(&buffer,binary.BigEndian,x)
	if err !=nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func ByteToInt64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func ByteToInt(data []byte) int {
	return int(binary.BigEndian.Uint32(data))
}



func TransTime (originTime int64) string {
	timePrecision:=len(Int64ToByte(originTime))
	switch timePrecision {
	case 10:
		tt:=time.Unix(0,originTime*1e9).Format(RFC3339Nano)
		return tt
	case 13:
		tt:=time.Unix(0,originTime*1e6).Format(RFC3339Nano)
		return tt
	case 19:
		tt:=time.Unix(0,originTime).Format(RFC3339Nano)
		return tt
	default:
		log.Println("Time format not supported")
		return ""
	}
}