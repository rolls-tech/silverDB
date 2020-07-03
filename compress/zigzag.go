package compress

import (
	"bytes"
	"encoding/binary"
)

func ZigZagInt64Encode(n int64) []byte {
	z:=(n << 1) ^ (n >> 63)
	b:=make([]byte,0)
	for i := 7; i >= 0 ; i-- {
		if byte(z >>(i*8)) > 0 {
			b=append(b,byte(z >>(i*8)))
		}
	}
	return b
}

func ZigZagInt64Decode(z []byte) int64 {
	b:=make([]byte,8-len(z))
	if len(z) < 8 {
		for i:=0;i < len(z) ;i ++ {
			b=append(b,z[i])
		}
	}
    byteBuffer:=bytes.NewBuffer(b)
    var data int64
    binary.Read(byteBuffer,binary.BigEndian,&data)
    n:=(data >> 1) ^ (-(data & 1))
    return n
}

func ZigZagInt32Encode(n int32) []byte {
	z:=(n << 1) ^ (n >> 31)
	b:=make([]byte,0)
	for i := 3; i >= 0 ; i-- {
		if byte(z >>(i*8)) > 0 {
	      b=append(b,byte(z >>(i*8)))
		}
	}
	return b
}

func ZigZagInt32Decode(z []byte) int32 {
	b:=make([]byte,4-len(z))
	if len(z) < 4 {
	 for i:=0;i < len(z) ;i ++ {
	 	b=append(b,z[i])
	  }
	}
	buf := bytes.NewReader(b)
	var data int32
	binary.Read(buf,binary.BigEndian,&data)
	n:=(data >> 1) ^ (-(data & 1))
	return n
}

