package metastore

import (
	"io/ioutil"
	"log"
	"silver/utils"
	"strings"
)


type MetaStore struct {
	MetaData map[string]string
	NodeAddr string
	*nodeRegister
}


func (m *MetaStore) getLocalNodeAddr(addr string){
	if addr !="" {
		m.NodeAddr=addr
	}
}

func (m *MetaStore) getLocalMataData(dataDir []string){
	if len(dataDir) !=0 {
		for _,dir:=range dataDir {
			exist:=utils.CheckFileIsExist(dir)
			if exist {
				dbList, err := ioutil.ReadDir(dir)
				if err != nil {
					log.Println(err)
				}
				for _,db:=range dbList {
					if db.IsDir() {
						fileList, _ := ioutil.ReadDir(dir+db.Name())
						for _,tb:=range fileList {
							if !tb.IsDir() && strings.HasSuffix(tb.Name(),"db") && !strings.Contains(tb.Name(),"_index.db") {
								m.MetaData[db.Name()]=strings.Split(tb.Name(),"-")[0]
							}
						}
					}
				}
			}
		}
	}
}


func NewMetaStore(dir []string,addr string) *MetaStore {
	m:=&MetaStore {
		MetaData: make(map[string]string,0),
	}
	m.getLocalNodeAddr(addr)
	m.getLocalMataData(dir)
	return m
}
