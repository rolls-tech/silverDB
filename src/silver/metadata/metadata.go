package metadata

import (
	"io/ioutil"
	"log"
	"silver/register"
	"silver/util"
	"strings"
)

type MetaData interface {
	GetMetaData([]string) map[string]string
	GetLocalNode(string) (string,string)
}

type Meta struct {
	MetaData map[string]string
	NodeAddr string
	*register.NodeRegister
}


func (m *Meta) getLocalNode(addr string){
	if addr !="" {
		m.NodeAddr=addr
	}
}


func (m *Meta) getMataData(dataDir []string){
	if len(dataDir) !=0 {
	   for _,dir:=range dataDir {
		   exist:=util.CheckFileIsExist(dir)
		   if exist {
			   dbList, err := ioutil.ReadDir(dir)
			   if err != nil {
				   log.Println(err)
			   }
			   for _,db:=range dbList {
			   	   if db.IsDir() {
					   fileList, _ := ioutil.ReadDir(dir+db.Name())
					   for _,tb:=range fileList {
						   if !tb.IsDir() {
						   	  m.MetaData[db.Name()]=strings.Split(tb.Name(),"-")[0]
						   }
					   }
				   }
			   }
		   }
	   }
	}
}


func NewMeta(dir []string,addr string) *Meta {
	m:=&Meta {
		MetaData: make(map[string]string,0),
	}
	m.getLocalNode(addr)
	m.getMataData(dir)
	return m
}

