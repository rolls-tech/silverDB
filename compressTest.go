package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/jwilder/encoding/simple8b"
	"github.com/jwilder/encoding/simple9"
	"github.com/prometheus/common/log"
	"github.com/tj/go-rle"
	"silver/compress"
)


func runLength() {
	in:=make([]int64,10000)
	for i := 0; i < 10000; i++ {
		in[i]=int64(i)
	}
	b:=rle.EncodeInt64(in)
	//fmt.Printf("buf: %#x\n", b)
	fmt.Printf("data len: %v , run-length encoding len: %v \n", len(in) * 8,len(b))
	// Output:
	// buf: 0x0210fa01020210
	// len: 7

}


func main() {
	enc := simple8b.NewEncoder()
	in1 := make([]uint64, 10000)
	in2 := make([]uint32,10000)
	for i := 0; i < 10000; i++ {
		in1[i] = uint64(i)
		in2[i] = uint32(i)
		enc.Write(in1[i])
	}
	encoded1, err := enc.Bytes()

	encoded2, err:=simple9.EncodeAll(in2)
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
    fmt.Printf("data len: %v, simple8b encoding len: %v \n", len(in1) * 8 ,len(encoded1))
	fmt.Printf("data len: %v ,simple9b encoding len: %v \n", len(in2) * 4 ,len(encoded2) * 4)
	runLength()
	b1:=compress.ZigZagInt64Encode(-23)
    fmt.Println(b1)
	b2:=compress.ZigZagInt64Decode(b1)
	buf:=new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,b2)

	var b byte

	b=1

	b = b << 1

	fmt.Println(b)
	b |= 1
	fmt.Println(b)

	fmt.Printf("data len: %v,zigzag encoding len: %v \n", len(buf.Bytes()), len(b1))
}

