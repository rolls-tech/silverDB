package main

import (
	"crypto/md5"
	"log"
)

func encodedSeriesKey(seriesKey string) {
	if len(seriesKey) > 0 {
		md:=md5.New()
		md.Write([]byte(seriesKey))
		result:=md.Sum([]byte(""))
		log.Println(result)
		log.Println(len(string(result)))
	}

}

func main() {
	s:="eweweadsadadasadsadadsadsadsadasdsadad"
	log.Println(len(s))
	encodedSeriesKey(s)
}
