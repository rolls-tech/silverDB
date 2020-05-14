package storage

import (
	"github.com/golang/snappy"
	"io/ioutil"
	"log"
	"os"
	"silver/config"
	"silver/utils"
	"strconv"
	"sync"
)

type Wal struct {
	mutex     sync.RWMutex
	size uint64
	walDir string
	maxSize int64
}

func NewWal(config config.NodeConfig) *Wal {
	if  config.Wal.WalDir != "" {
		ok:=utils.CheckFileIsExist(config.Wal.WalDir)
			if !ok {
				e:=os.MkdirAll(config.Wal.WalDir,os.ModePerm)
				if e !=nil {
					log.Println("failed create wal dir",config.Wal.WalDir,e)
				}
		}
	}
    return &Wal {
		mutex:   sync.RWMutex{},
		walDir:  config.Wal.WalDir,
		maxSize: config.Wal.Size,
	}
}

func(w *Wal) writeData(data *walData) error {
	//walCh:=make(chan bool,5000)
	_,e:=w.writeWal(data)
	/*walCh <- ok
	w.dataBuffer.writeData(data.wp,data.tagKv,walCh)*/
	return e
}




func (w *Wal) writeWal(data *walData) (bool,error) {
		timestamp:=utils.IntToByte(data.timestamp)
	    sequenceId:=utils.IntToByte(data.sequenceId)
	    buf:=make([]byte, len(timestamp)+len(sequenceId)+len(data.data))
	    buf=append(buf,timestamp...)
	    buf=append(buf,sequenceId...)
	    buf=append(buf,data.data...)
	    buf=append(buf,'\n')
	    getWp:=snappy.Encode(nil,buf)
	    walFile,n:=w.getWalFile()
	    exist:=utils.CheckFileIsExist(walFile)
	if !exist {
		walFile:=w.createWalFile(n)
		e:=w.writeWalFile(walFile,getWp)
		return true,e
	}
	fileInfo,e:=os.Stat(walFile)
	if e !=nil {
		log.Println("no get wal file: "+walFile+" info ",e)
		return false,e
	}
	fileSize:=fileInfo.Size()
	if fileSize > w.maxSize * (1 << 20) {
		walFile := w.createWalFile(n)
		e = w.writeWalFile(walFile, getWp)
		if e != nil {
			return false,e
		}
		return true,e
	}
	e = w.writeWalFile(walFile, getWp)
	return true,e
}


func (w *Wal) writeWalFile(walFile string,getWp []byte) error {
	f,e:= os.OpenFile(walFile, os.O_APPEND, 0666)
	if e !=nil {
		log.Println("open wal file: "+walFile+" failed",e)
		return e
	}
	defer f.Close()
	w.mutex.Lock()
	n,e:=f.Write(getWp)
	log.Println("write wal data size: ", n/1024,"kb")
	if e !=nil {
		log.Println("write wal file: "+walFile+" failed",e)
		return e
	}
	w.mutex.Unlock()
	e=f.Sync()
	return e
}


func (w *Wal) createWalFile(n int) string {
	exist :=utils.CheckFileIsExist(w.walDir)
	if !exist {
		e:=os.MkdirAll(w.walDir,666)
		if e !=nil {
			log.Println("create wal dir failed ! ",e)
		}
	}
	m:=n+1
	walFile:=w.walDir+"_"+strconv.Itoa(m)+".wal"
	_,e:=os.Create(walFile)
	if e !=nil {
		log.Println(e,walFile)
	}
	return walFile

}


func (w *Wal) getWalFile() (string,int) {
	exist :=utils.CheckFileIsExist(w.walDir)
	if exist == true {
		fileList, err := ioutil.ReadDir(w.walDir)
		if err != nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			walFile:=w.walDir+"_1.wal"
			_,e:=os.Create(walFile)
			if e !=nil {
				log.Println(e,walFile)
			}
			return walFile,1
		}
		walFile:=w.walDir+"_"+strconv.Itoa(len(fileList))+".wal"
		return walFile,len(fileList)
	}
	return "",0
}