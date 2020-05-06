package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type NodeConfig struct {
	NodeName string  `yaml:"nodeName"`
	NodeAddr NodeAddr `yaml:"nodeAddr"`
	NodeData []string  `yaml:"nodeData,flow"`
	IndexData []string `yaml:"indexData,flow"`
	Wal Wal  `yaml:"wal"`
	Flush Flush `yaml:"flush"`
	HeatBeat HeatBeat `yaml:"heatbeat"`
	MetaStore MetaStore `yaml:"metastore"`
	Discovery Discovery `yaml:"discovery"`
	Compressed bool `yaml:"compressed"`
	Indexed bool `yaml:"indexed"`
}

type Wal struct {
	WalData string  `yaml:"walData"`
	TTL int64 `yaml:"ttl"`
	Nums int  `yaml:"nums"`
	Size int64  `yaml:"size"`
}


type NodeAddr struct {
	HttpAddr string `yaml:"httpAddr"`
	TcpAddr string `yaml:"tcpAddr"`
	CluAddr string `yaml:"cluAddr"`
}

type Flush struct {
	Count int `yaml:"count"`
	Timeout int64 `yaml:"timeout"`
}

type HeatBeat struct {
	Tick int64 `yaml:"tick"`
	Timeout int64 `yaml:"timeout"`
}

type MetaStore struct {
	NodeAddr []string `yaml:"nodeAddr,flow"`
	NodePath string `yaml:"nodePath"`
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

