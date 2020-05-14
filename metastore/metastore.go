package metastore

import (
	"io/ioutil"
	"log"
	"silver/utils"
	"strings"
)


type MetaStore struct {
	MetaData map[string]map[string]bool
	NodeAddr string
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
				if len(dbList) > 0 {
					for _,db:=range dbList {
						if db.IsDir() {
							_,ok:=m.MetaData[db.Name()]
							if !ok  {
								m.MetaData[db.Name()]=make(map[string]bool,0)
							}
							fileList, _ := ioutil.ReadDir(dir+db.Name())
							if len(fileList) < 0 {
								for _,tb:=range fileList {
									if !tb.IsDir() && strings.HasSuffix(tb.Name(),"db") && !strings.Contains(tb.Name(),"_index.db") {
										_,ok:=m.MetaData[db.Name()][strings.Split(tb.Name(),"-")[0]]
										if !ok {
											m.MetaData[db.Name()][strings.Split(tb.Name(),"-")[0]]=true
										}
									}
								}
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
		MetaData: make(map[string]map[string]bool,0),
	}
	m.getLocalNodeAddr(addr)
	m.getLocalMataData(dir)
	return m
}
