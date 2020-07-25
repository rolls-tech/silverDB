package main

import (
	"log"
	"strings"
	_ "time"
)

func main() {

	/*count:=utils.ByteToInt(countByte)
	switch utils.ByteToInt32(metricTypeByte) {
	case utils.Int:
		minValue:=utils.ByteToInt32(minValueByte)
		maxVlaue:=utils.ByteToInt32(maxValueByte)
	case utils.Long:
		minValue:=utils.ByteToInt64(minValueByte)
		maxVlaue:=utils.ByteToInt64(maxValueByte)
	case utils.Float:
		minValue:=utils.ByteToFloat32(minValueByte)
		maxVlaue:=utils.ByteToFloat32(maxValueByte)
	case utils.Double:
		minValue:=utils.ByteToFloat64(minValueByte)
		maxVlaue:=utils.ByteToFloat64(maxValueByte)
	case utils.Bool:
		minValue:=minValueByte
		maxVlaue:=maxValueByte
	default:
		log.Println("not support data type !", utils.ByteToInt32(metricTypeByte))
	}*/

	n := strings.LastIndex("a1=vv3;b2=vv4;c3=kk3;m1", ";")
	log.Println(len("a1=vv3;b2=vv4;c3=kk3;m1"))
	log.Println(n)
	log.Println("a1=vv3;b2=vv4;c3=kk3;m1"[21:])
	log.Println("a1=vv3;b2=vv4;c3=kk3;m1"[:21])
}
