package worker



/*
func (s *Server) handleTsSetData(r *bufio.Reader,conn net.Conn) ([]*storage.TsBuffer,error){
	wp,e := parseWPoint(r)
	Fields:=make([]*storage.TsBuffer,0)
	if e != nil {
		return nil,e
	}
	dataBase:=wp.DataBase
	tableName
	if len(tsSetData) != 0 {
		for i,p:=range tsSetData {
			var rowKey string
			dataBase:=p.Key.RowKey.DataBase
			tableName:=p.Key.RowKey.TableName
			tagKv:=p.Key.RowKey.TagKV.TagKv
			if tagKv !=nil {
				for tagK,tagV:=range tagKv {
					rowKey+=tagK+tagV
				}
			}
			n:=strconv.Itoa(i)
			addr, ok := s.ShouldProcess(dataBase+tableName+rowKey)
			if ! ok {
				aLen:= len(addr)
				nLen:=len(n)
				_, e := conn.Write([]byte(fmt.Sprintf("R%d,%d,%s%s",aLen,nLen,addr,n)))
				if e != nil {
					log.Println(e.Error())
				}
				return nil,errors.New("redirect " + addr)
			}
			Fields=append(Fields,p)
		}
	}
	return Fields,nil
}


func parseWPoint(r *bufio.Reader) (*storage.WPoint,error) {
	l1,e:= readLen(r)
	dLen, e := strconv.Atoi(l1)
	buf := make([]byte,dLen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return nil,e
	}
	data:=&storage.WPoint{}
	e=proto.Unmarshal(buf,data)
	if e != nil {
		log.Println("wPoint Deserialization failed",e)
		return nil,e
	}
	return data,e
}


func (s *Server) readTsGetData(r *bufio.Reader,conn net.Conn) ([]*storage.TsCache,[]byte,[]byte,error) {
	tsData,startTime,endTime,e := parseTsGetData(r)
	Fields:=make([]*storage.TsCache,0)
	if e != nil {
		return nil,nil,nil,e
	}
	if len(tsData) != 0 {
		for i,p:=range tsData {
			var rowKey string
			dataBase:=p.Key.RowKey.DataBase
			tableName:=p.Key.RowKey.TableName
			tagKv:=p.Key.RowKey.TagKV.TagKv
			if tagKv !=nil {
				for tagK,tagV:=range tagKv {
					rowKey+=tagK+tagV
				}
			}
			n:=strconv.Itoa(i)
			addr, ok := s.ShouldProcess(dataBase+tableName+rowKey)
			if ! ok {
				aLen:= len(addr)
				nLen:=len(n)
				_, e := conn.Write([]byte(fmt.Sprintf("R%d,%d,%s%s",aLen,nLen,addr,n)))
				if e != nil {
					log.Println(e.Error())
				}
				return nil,nil,nil,errors.New("redirect " + addr)
			}
			Fields=append(Fields,p)
		}
	}
	return Fields,startTime,endTime,nil
}


func parseTsGetData(r *bufio.Reader) ([]*storage.TsCache,[]byte,[]byte,error) {
	l1, e := readLen(r)
	l2, e := readLen(r)
	l3, e := readLen(r)
	dLen, e := strconv.Atoi(l1)
	sLen, e := strconv.Atoi(l2)
	eLen, e := strconv.Atoi(l3)
	buf := make([]byte,dLen+sLen+eLen)
	_, e = io.ReadFull(r, buf)
	if e != nil {
		return nil,nil,nil,e
	}
	data:=(buf)[:dLen]
	startTime:=(buf)[dLen:sLen]
	endTime:=(buf)[sLen:]
	tsData:=&storage.TsCacheData{}
	e=proto.Unmarshal(data,tsData)
	if e != nil {
		log.Println("TsData Deserialization failed",e)
		return nil,nil,nil,e
	}
	return tsData.GetTsGetData(),startTime,endTime,nil
}


func (s *Server) tsGet(ch chan chan  *storage.TsCacheData,conn net.Conn, r *bufio.Reader,dbCh chan []*bolt.DB) error {
	     c:=make(chan *storage.TsCacheData)
	     ch <-c
	     tsFields,st,et,e := s.readTsGetData(r,conn)
	     startTime:=int64(binary.BigEndian.Uint64(st))
	     endTime:=int64(binary.BigEndian.Uint64(et))
	     if e !=nil {
		     c<-&storage.TsCacheData{}
		     log.Println(e)
		     return e
	     }
	     tsData:=&storage.TsCacheData {
			 TsGetData: make([]*storage.TsCache,0),
	     }
	     for i,_:=range tsFields {
			 p := tsFields[i]
			 dataBase := p.Key.RowKey.DataBase
			 tableName := p.Key.RowKey.TableName
			 var rowKey string
			 var tags string
			 tagKv := p.Key.RowKey.TagKV.TagKv
			 if tagKv != nil {
				 for tagK, tagV := range tagKv {
					 tags += tagK + tagV
				 }
			 }
			 rowKey = dataBase + tableName + tags
			 fieldKey := p.Key.ValueKey
			 tableFileList:=s.GetStorageFile(dataBase,tableName,startTime,endTime)
			 var mu sync.RWMutex
			 var wg sync.WaitGroup
			 var dbList []*bolt.DB
			 wg.Add(1)
			 ok := s.CacheIsExist(tableName, startTime, endTime)
			 cache := s.GetCache()
			 if !ok {
			    s.scanTableFile(c,dbCh,tableFileList,rowKey,fieldKey,startTime,endTime,p,cache,dbList,tsData,&wg,mu)
			 }
			 s.queryData(c,dbCh,dataBase,tableName,rowKey,fieldKey,p,cache,dbList,tsData,startTime,endTime,&wg,mu)
		 }
	return nil
}

func (s *Server) scanTableFile(c chan *storage.TsCacheData,dbCh chan []*bolt.DB,tableFileList []string,rowKey,fieldKey string,startTime,endTime int64,p *storage.TsCache,
	cache *storage.Cache,dbs []*bolt.DB,tsData *storage.TsCacheData,wg *sync.WaitGroup,mu sync.RWMutex) {
	for i,_:=range tableFileList {
		wg.Add(1)
		tableFile:=tableFileList[i]
		go s.queryStorageData(c,dbCh,tableFile,rowKey,fieldKey,startTime,endTime,p,cache,dbs,tsData,wg,mu)
	}
}


func (s *Server) queryStorageData(c chan *storage.TsCacheData,dbCh chan []*bolt.DB,tableFile,rowKey,fieldKey string,startTime,endTime int64,p *storage.TsCache,
	cache *storage.Cache,dbs []*bolt.DB,tsData *storage.TsCacheData,wg *sync.WaitGroup,mu sync.RWMutex) {
	var timeCacheKv storage.TimeCacheKv
	mu.Lock()
	db:=s.OpenDB(tableFile)
	fieldData:=s.GetTimeRangeData(db,p,rowKey,fieldKey,startTime,endTime)
	//将结果缓存到tsCache中
	timeCacheKv.StartTime=startTime
	timeCacheKv.EndTime=endTime
	timeCacheKv.Created=time.Now()
	timeCacheKv.FieldData.TsGetData=append(timeCacheKv.FieldData.TsGetData,fieldData)
	fileInfo:=strings.Split(tableFile,"-")
	tn:=fileInfo[0]
	cache.Cache[tn]=&timeCacheKv
	tsData.TsGetData=append(tsData.TsGetData,fieldData)
	dbs=append(dbs,db)
	mu.Unlock()
	wg.Done()
}

func (s *Server) queryData(c chan *storage.TsCacheData,dbCh chan []*bolt.DB,dataBase,tableName,rowKey,fieldKey string,
	point *storage.TsCache,cache *storage.Cache,dbs []*bolt.DB,tsData *storage.TsCacheData,
	startTime,endTime int64,wg *sync.WaitGroup,mu sync.RWMutex) {
	cache.Mutex.RLock()
	cacheStartTime:= cache.Cache[tableName].StartTime
	cacheEndTime:=cache.Cache[tableName].EndTime
	cache.Mutex.RUnlock()
	if startTime >= cacheStartTime && endTime <= cacheEndTime {
		tsData.TsGetData=append(tsData.TsGetData,s.ScanCacheByTable(tableName,point,startTime, endTime)...)
	}
	if startTime < cacheStartTime && endTime > cacheEndTime {
		var tableFileList []string
		tableFile1:=s.GetStorageFile(dataBase,tableName,startTime,cacheStartTime)
		tableFileList=append(tableFileList,tableFile1...)
		tableFile2:=s.GetStorageFile(dataBase,tableName,cacheEndTime,endTime)
		tableFileList=append(tableFileList,tableFile2...)
		s.scanTableFile(c,dbCh,tableFileList,rowKey,fieldKey,startTime,endTime,point,cache,dbs,tsData,wg,mu)
		tsData.TsGetData=append(tsData.TsGetData,s.ScanCacheByTable(tableName,point,cacheStartTime,cacheEndTime)...)
		wg.Wait()
		c <- tsData
		dbCh <- dbs
	}
	if startTime < cacheStartTime && endTime < cacheEndTime {
		var tableFileList []string
		tableFile1:=s.GetStorageFile(dataBase,tableName,startTime,cacheStartTime)
		tableFileList=append(tableFileList,tableFile1...)
		s.scanTableFile(c,dbCh,tableFileList,rowKey,fieldKey,startTime,endTime,point,cache,dbs,tsData,wg,mu,)
		tsData.TsGetData=append(tsData.TsGetData,s.ScanCacheByTable(tableName,point,cacheStartTime,endTime)...)
		wg.Wait()
		c <- tsData
		dbCh <- dbs

	}
	if startTime > cacheStartTime && endTime > cacheEndTime {
		var tableFileList []string
		tableFile2:=s.GetStorageFile(dataBase,tableName,cacheEndTime,endTime)
		tableFileList=append(tableFileList,tableFile2...)
		s.scanTableFile(c,dbCh,tableFileList,rowKey,fieldKey,startTime,endTime,point,cache,dbs,tsData,wg,mu,)
		tsData.TsGetData=append(tsData.TsGetData,s.ScanCacheByTable(tableName,point,startTime,cacheEndTime)...)
		wg.Wait()
		c <- tsData
		dbCh <- dbs
	}
}


func (s *Server) tsSet(ch chan chan *storage.TsBufferData,conn net.Conn, r *bufio.Reader) error {
	c:=make(chan *storage.TsBufferData)
	ch <- c
	tsFields,e := s.readTsSetData(r,conn)
	    if e != nil {
	    	c <- &storage.TsBufferData{}
		    return e
	    }
	go func() {
		for _,p:=range tsFields {
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
			e:= sendResponse(nil, s.SetTsBuffer(dataBase+tableName+rowKey,fieldKey,p),conn)
			if e != nil {
				log.Println(e)
			}
			c <- &storage.TsBufferData{}
		}
	}()
	return nil
}

func (s *Server) tsDel(ch chan chan *result.TsResult,conn net.Conn, r *bufio.Reader) error {
	   c:=make(chan *result.TsResult)
	   ch <- c
	   database,table,rowKey,key,startTime,endTime,e := s.readTsGetData(r,conn)
	    if e != nil {
	    	c <- &result.TsResult{}
		    return e
	    }
	   go func() {
		   st,_:= strconv.ParseInt(startTime, 10, 64)
		   et,_:=strconv.ParseInt(endTime, 10, 64)
		   db, e := s.DelTSData(database,table,rowKey,key,st,et)
		   if e !=nil {
		   	log.Println(e)
		   }
		   c <- &result.TsResult{}
		   defer db.Close()
	   }()
	   return nil
}

func tsGetReply(conn net.Conn,resultCh chan chan *storage.TsCacheData,dbCh chan []*bolt.DB) {
	defer conn.Close()
	for {
		c,open := <- resultCh
		if !open {
			return
		}
		r := <- c
		if r == nil {
			return
		}
		if !open{
			return
		}
		e:=sendTsResponse(r,nil,conn,dbCh)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}


func tsSetReply(conn net.Conn,resultCh chan chan *storage.TsBufferData) {
	defer conn.Close()
	for {
		c,open := <- resultCh
		if !open {
			return
		}
		r := <- c
		if r == nil {
			return
		}
		if !open{
			return
		}
		e:= sendResponse(r,true,conn)
		if e !=nil {
			log.Println("close connection due to error:", e)
			return
		}
	}
}

func sendTsResponse(value *storage.TsCacheData, err error, conn net.Conn,dbCh chan []*bolt.DB) error {
	if err != nil {
		errString := err.Error()
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	data,e:=proto.Marshal(value)
	if e !=nil {
		log.Println(e.Error())
		return e
	}
	_,e= conn.Write(append([]byte(fmt.Sprintf("V%d,%s,",len(data),data))))
	dbList:=<-dbCh
	var wg sync.WaitGroup
	for n,_:=range dbList {
		db:=dbList[n]
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			wg.Done()
			defer db.Close()
		}(&wg)
	}
	wg.Wait()
	return e
}

func readLen(r *bufio.Reader) (string, error) {
	tmp, e := r.ReadString(',')
	if tmp == "" {
		return "", nil
	}
	if e != nil {
		return "", e
	}
	return strings.ReplaceAll(tmp, ",", ""), nil
}

func sendResponse(value *storage.TsBufferData, ok bool, conn net.Conn) error {
	if !ok {
		errString := "mistake"
		tmp := fmt.Sprintf("-%d", len((errString)+errString))
		_, e := conn.Write([]byte(tmp))
		return e
	}
	tsData,e:=proto.Marshal(value)
	data := fmt.Sprintf("V%d,%s", len(tsData),tsData)
	_,e= conn.Write(append([]byte(data)))
	return e
}

*/