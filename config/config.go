package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type NodeConfig struct {
	NodeName string  `yaml:"nodeName"`
	NodeAddr NodeAddr `yaml:"nodeAddr"`
	NodeId int64 `yaml:"nodeId"`
	DataDir []string  `yaml:"dataDir,flow"`
	IndexDir []string `yaml:"indexDir,flow"`
	Wal Wal  `yaml:"wal"`
	Flush Flush `yaml:"flush"`
	HeatBeat HeatBeat `yaml:"heatbeat"`
	MetaStore MetaStore `yaml:"metastore"`
	Discovery Discovery `yaml:"discovery"`
	Compressed bool `yaml:"compressed"`
	CompressCount int `yaml:"compressCount"`
	Indexed bool `yaml:"indexed"`
}

type Wal struct {
	WalDir string  `yaml:"walDir"`
	TTL int64 `yaml:"ttl"`
	NodeNums int  `yaml:"nodeNums"`
	ListNums int  `yaml:"listNums"`
	Size int64 `yaml:"size"`
}


type NodeAddr struct {
	HttpAddr string `yaml:"httpAddr"`
	TcpAddr string `yaml:"tcpAddr"`
	CluAddr string `yaml:"cluAddr"`
}

type Flush struct {
	Count int `yaml:"count"`
	TTL int64 `yaml:"ttl"`
}

type HeatBeat struct {
	Tick int64 `yaml:"tick"`
	Timeout int64 `yaml:"timeout"`
}

type MetaStore struct {
	MetaAddr []string `yaml:"metaAddr,flow"`
	NodePrefix string `yaml:"nodePrefix"`
	MetaPrefix string `yaml:"metaPrefix"`
	Timeout int64 `yaml:"timeout"`
	HeartBeat int64 `yaml:"heartbeat"`
}

type Discovery struct {
	Timeout int64 `yaml:"timeout"`
}

func LoadConfigInfo(configFile string) NodeConfig {
	config,e:=ioutil.ReadFile(configFile)
	if e !=nil {
		log.Fatal(e)
	}

	nodes:=new(NodeConfig)
	e=yaml.Unmarshal(config,nodes)
	if e !=nil {
		log.Println(e)
	}
	log.Println("finished load config info ",nodes)
	return *nodes

}

