package storage

import (
	"bufio"
	"encoding/binary"
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
	_,e:=w.writeWal(data)
	return e
}


func (w *Wal) writeWal(data *walData) (bool,error) {
	    dst:=make([]byte,8)
	    binary.BigEndian.PutUint64(dst[:8],uint64(data.sequenceId))
	    dst=append(dst,data.data...)
	    dst=append(dst,'\n')
	    buffer:=make([]byte, snappy.MaxEncodedLen(len(dst)))
	    b:=snappy.Encode(buffer,dst)
	    walFile,n:=w.getWalFile()
	    exist:=utils.CheckFileIsExist(walFile)
	if !exist {
		walFile:=w.createWalFile(n)
		e:=w.writeWalFile(walFile,b)
		return true,e
	}
	fileInfo,e:=os.Stat(walFile)
	if e !=nil {
		log.Println("no get wal file: "+walFile+" info ",e)
		return false,e
	}
	fileSize:=fileInfo.Size()

	if fileSize > w.maxSize * ( 1024 << 14 ) {
		walFile := w.createWalFile(n)
		e = w.writeWalFile(walFile, b)
		if e != nil {
			return false,e
		}
		return true,e
	}
	e = w.writeWalFile(walFile, b)
	return true,e
}


func (w *Wal) writeWalFile(walFile string,getWp []byte) error {
	f,e:= os.OpenFile(walFile, os.O_APPEND | os.O_WRONLY, os.ModeAppend)
	if e !=nil {
		log.Println("open wal file: "+walFile+" failed",e)
		return e
	}
	defer f.Close()
	writer:=bufio.NewWriterSize(f,16 * 1024)
	w.mutex.Lock()
	_,e=writer.Write(getWp)
	//log.Println("write wal data size: ", n / 1024,"kb")
	if e !=nil {
		log.Println("write wal file: "+walFile+" failed",e)
		return e
	}
	w.mutex.Unlock()
    e=writer.Flush()
	//e=f.Sync()
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
		//wal目录下为空的时候，则新建第一个wal文件
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