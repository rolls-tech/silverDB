package storage

/*
type TsStorage struct {
	Cache
	Buffer
	Tss
}

func NewTsStorage(ttl int,dataPath []string) *TsStorage{
	return &TsStorage{
		Cache:  *NewtsCache(ttl,dataPath),
		Buffer: *NewtsBuffer(ttl,dataPath),
	}
}

type Tss struct {
	mutex     sync.RWMutex
	dataDir   []string
}

func (t *Tss) SetTsData(p *TsBuffer) error {
	var rowKey string
	dataBase:=p.Key.RowKey.DataBase
	tableName:=p.Key.RowKey.TableName
	tagKv:=p.Key.RowKey.TagKV.TagKv
	if tagKv !=nil {
		for tagK,tagV:=range tagKv {
			rowKey+=tagK+tagV
		}
	}
	fieldKey:=p.Key.ValueKey
	Kv:=p.Value.Kv
	tableFileKv:=t.setTableFile(dataBase,tableName,Kv)
	for tableFile,Kv:=range tableFileKv {
		go func() {
			db := t.OpenDB(tableFile)
			defer db.Close()
			if err := db.Batch(func(tx *bolt.Tx) error {
				rootTable, err := tx.CreateBucketIfNotExists([]byte(rowKey))
				if err != nil {
					return err
				}
				table, err := rootTable.CreateBucketIfNotExists([]byte(fieldKey))
				if err != nil {
					return err
				}
				if Kv != nil {
					for k,v:=range Kv {
						err = table.Put(intToByte(k),v)
						if err != nil {
							return err
						}
					}
				}
				return nil
			}); err != nil {
				log.Println(err)
			}
		}()
	}
	return nil
}

func (t *Tss) GetTimeRangeData(db *bolt.DB,tsField  *TsCache,rowKey, key string,startTime,endTime int64) *TsCache {
	st := strconv.FormatInt(startTime, 10)
	et := strconv.FormatInt(endTime, 10)
	value:=make([]*TsValue,0)
	if err:= db.View(func(tx *bolt.Tx) error {
					rootTable := tx.Bucket([]byte(rowKey))
					b:=rootTable.Bucket([]byte(key))
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						if strings.Compare(string(k), st) >= 0 && strings.Compare(string(k), et) <= 0 {
							tsValue := &TsValue{
								T:int64(binary.BigEndian.Uint64(k)),
								V:v,
							}
							value = append(value, tsValue)
						}
					}
					return nil
				}); err != nil {
					log.Println(err)
				}
	    tsField.Value=value
		return tsField
}

func (t *Tss) DelTSData(dataBase, tableName, rowKey, key string,startTime,endTime int64) (*bolt.DB, error) {
	return nil,nil
}

func NewTss(dataDir []string) *Tss {
	return &Tss{
		mutex:         sync.RWMutex{},
		dataDir:       dataDir,
	}
}

func (t *Tss) OpenDB(dataFile string) *bolt.DB {
	db, err := bolt.Open(dataFile, 777, nil)
	if err != nil {
		log.Println(err.Error())
	}
	return db
}

func (t *Tss) setTableFile(dataBase,tableName string,Kv map[int64][]byte) map[string]map[int64][]byte {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var tableFile string
	var endTime int64
	tableFileKv:=make(map[string]map[int64][]byte,0)
	if Kv != nil {
		for k,v:=range Kv {
			endTime=getEndTime(k,24*time.Hour)
			tableFile=t.scanDataDir(dataBase,tableName,k,endTime)
			tableFileKv[tableFile][k]=v
		}
	}
	return tableFileKv
}

func (t *Tss) GetStorageFile(dataBase,tableName string,startTime,endTime int64) []string {
	sTime := strconv.FormatInt(startTime, 10)
	eTime := strconv.FormatInt(endTime, 10)
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	var tableFile string
	var tableFileList []string
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist == true {
		fileList,err:=ioutil.ReadDir(dataBaseDir)
		if err !=nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			return nil
		}
		for _,file:=range fileList {
			tableInfo:=strings.Split(file.Name(),"-")
			tn:=tableInfo[0]
			st:=tableInfo[1]
			et:=strings.Split(tableInfo[2],".")[0]
			if strings.Compare(tn,tableName) == 0 {
				if strings.Compare(sTime,st) >= 0 && strings.Compare(eTime,et) <= 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(sTime,st) < 0 && strings.Compare(eTime,st) >0 && strings.Compare(eTime,et) < 0 {
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
				if strings.Compare(sTime,st) > 0 && strings.Compare(eTime,et) >0 && strings.Compare(sTime,et) <0{
					tableFile=dataBaseDir+sep+file.Name()
					tableFileList=append(tableFileList,tableFile)
				}
			}
		}
	}
	return tableFileList
}

func (t *Tss) scanDataDir(dataBase,tableName string,st,et int64) string {
	var tableFile string
	startTime := strconv.FormatInt(st,10)
	endTime := strconv.FormatInt(et,10)
	dataBaseDir,exist:=t.dataBaseDirIsExist(dataBase)
	if exist == true {
		fileList,err:=ioutil.ReadDir(dataBaseDir)
		if err !=nil {
			log.Println(err)
		}
		if len(fileList) == 0 {
			tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+endTime+".db"
			return tableFile
		}
		for _,file:=range fileList {
			tableInfo:=strings.Split(file.Name(),"-")
			tn:=tableInfo[0]
			st:=tableInfo[1]
			et:=strings.Split(tableInfo[2],".")[0]
			if strings.Compare(tn,tableName)==0 && strings.Compare(startTime,st) == 1 && strings.Compare(startTime,et)==-1 {
				tableFile=dataBaseDir+sep+file.Name()
				return tableFile
			}
		 }
			tableFile=dataBaseDir+sep+tableName+"-"+startTime+"-"+endTime+".db"
			return tableFile
	 }
		rand.Seed(time.Now().Unix())
		n := rand.Intn(len(t.dataDir))
		err:=os.MkdirAll(t.dataDir[n]+dataBase,os.ModePerm)
		if err !=nil {
			log.Println(err)
		}
		tableFile=t.dataDir[n]+dataBase+sep+tableName+"-"+startTime+"-"+endTime+".db"
		return tableFile
}

func (t *Tss) dataBaseDirIsExist(dataBase string) (string,bool) {
	for _,dir:= range t.dataDir {
		if t.fileIsExist(dir+dataBase) {
			return dir+dataBase,true
		}
	}
	return "",false
}

func (t *Tss) fileIsExist(file string) bool {
	_, err := os.Stat(file)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func intToByte(num int64) []byte{
	var buffer bytes.Buffer
	err :=binary.Write(&buffer,binary.LittleEndian,num)
	if err !=nil {
		log.Println(err)
	}
	return buffer.Bytes()
}

func transTime (originTime int64) string {
	timePrecision:=len(intToByte(originTime))
	switch timePrecision {
	case 10:
		tt:=time.Unix(0,originTime*1e9).Format(RFC3339Nano)
		return tt
	case 13:
		tt:=time.Unix(0,originTime*1e6).Format(RFC3339Nano)
		return tt
	case 19:
		tt:=time.Unix(0,originTime).Format(RFC3339Nano)
		return tt
	default:
		log.Println("Time format not supported")
		return ""
	}
}

func getEndTime (dataTime int64,durationTime time.Duration) int64 {
	endTime:=dataTime+durationTime.Nanoseconds()
	return endTime
}

*/