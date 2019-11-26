package config

import (
	"github.com/toml"
	"log"
)

type Cache struct {
	Enable bool
	List   []string
}

type Storage struct {
	Type     string
	Node     string
	HostName string
	HttpPort int
	TcpPort  int
	Cluster  []string
	DataPath []string
}

func GetStorageConf(path string) *Storage {
	var c Storage
	if _, err := toml.DecodeFile(path, &c); err != nil {
		panic(err)
	}
	log.Println(c)
	return &c
}
