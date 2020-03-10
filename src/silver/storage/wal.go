package storage

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"io/ioutil"
	"log"
	"os"
	"silver/util"
	"strconv"
	"sync"
	"time"
)

type tsWal struct {
	mutex     sync.RWMutex
	walDir   string
	*tsBufferData
	*tsIndex
	size uint64
	maxSize uint64
	count uint64
	ttl time.Duration
}


func NewTsWal(ttl int,walDir string,indexDir []string,tss *tss,flushCount int64) *tsWal {
	return  &tsWal{
		mutex:        sync.RWMutex{},
		walDir:      walDir,
		tsBufferData: NewTsBufferData(ttl,tss,flushCount),
		tsIndex:NewTsIndex(ttl,indexDir),
		size:         0,
		maxSize:      0,
		count:        0,
		ttl:          time.Duration(ttl) * time.Second,
	}
}



func(w *tsWal) writeTsData(dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64]float64) error {
    walCh:=make(chan bool,5000)
	ok,e:=w.writeTsWal(dataBase,tableName,tagKv,fieldKey,tags,value)
    walCh <- ok
    go w.writeData(dataBase,tableName,tagKv,fieldKey,tags,value,walCh)
   // go w.writeIndex(tableName,fieldKey,tags,walCh)
	return e
}


func (w *tsWal) writeTsWal (dataBase,tableName,tagKv,fieldKey string,
	tags map[string]string,value map[int64]float64) (bool,error) {
	tsValue:=&Value {
		Kv:value,
	}
	w.mutex.Lock()
	defer w.mutex.Unlock()
	fieldKeyValue:=make(map[string]*Value,0)
	fieldKeyValue[fieldKey]=tsValue
	wp:=&WPoint {
			DataBase:             dataBase,
			TableName:            tableName,
			Tags:                 tags,
			Value:                fieldKeyValue,
	}
	serializeWp,e:=proto.Marshal(wp)
	if e !=nil {
		log.Println("serialize wPoint or wData failed",e)
		return false,e
	}
    getWp:=snappy.Encode(nil,serializeWp)
    walFile,n:=w.getWalFile()
    exist:=util.CheckFileIsExist(walFile)
    if !exist {
		_,e:=os.Create(walFile)
		if e !=nil {
			log.Println(e,walFile)
		}
		e=w.writeWalFile(walFile,getWp)
		if e !=nil {
			log.Println(e)
			return false,e
		}
    	return true,e
	}
    fileInfo,e:=os.Stat(walFile)
    if e !=nil {
    	log.Println(e)
    	return false,e
	}
    fileSize:=fileInfo.Size()
    if fileSize > 25 * (1 << 20) {
		walFile := w.createWalFile(n)
		e = w.writeWalFile(walFile, getWp)
		if e != nil {
			log.Println(e)
			return false,e
		}
		return true,e
	}
    e = w.writeWalFile(walFile, getWp)
       if e != nil {
			log.Println(e)
			return false,e
		}
    return true,e
}

func (w *tsWal) createWalFile(n int) string {
      m:=n+1
	  walFile:=w.walDir+"_"+strconv.Itoa(m)+".wal"
	  _,e:=os.Create(walFile)
	  if e !=nil {
	  	log.Println(e,walFile)
	  }
	  return walFile
}


func (w *tsWal) writeWalFile(walFile string,getWp []byte) error {
	f,e:= os.OpenFile(walFile, os.O_APPEND, 0666)
	if e !=nil {
		log.Println(e)
	}
	defer f.Close()
	timeByte:=util.IntToByte(time.Now().UnixNano())
	buf:=make([]byte,len(timeByte)+len(getWp))
	buf=append(buf,timeByte...)
	buf=append(buf,getWp...)
	_,e=f.Write(buf)
	//fmt.Printf("write %d byte",n)
	e=f.Sync()
	return e
}


func (w *tsWal) getWalFile() (string,int) {
	exist :=util.CheckFileIsExist(w.walDir)
	if exist == true {
		fileList, err := ioutil.ReadDir(w.walDir)
		if err != nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			walFile:=w.walDir+"_1.wal"
			return walFile,1
		}
		walFile:=w.walDir+"_"+strconv.Itoa(len(fileList))+".wal"
		return walFile,len(fileList)
	}
	return "",0
}


