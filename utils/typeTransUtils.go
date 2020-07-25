package utils

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
)

func Float64ToByte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func Float32ToByte(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint32(bytes, bits)
	return bytes
}

func ByteToFloat64(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	return math.Float64frombits(bits)
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.BigEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func Int64ToByte(num int64) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, num)
	if err != nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func Int32ToByte(num int32) []byte {
	x := num
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, x)
	if err != nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func IntToByte(num int) []byte {
	x := int32(num)
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, x)
	if err != nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func ByteToInt64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func ByteToInt32(data []byte) int32 {
	return int32(binary.BigEndian.Uint32(data))
}
