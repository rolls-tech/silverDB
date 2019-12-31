package client

/*
type TsClient struct {
	Server      string
	StorageType string
	OperateType string
}

/*

type Cmd struct {
	Name     string
	storage.TsCacheData
	storage.TsBufferData
	StartTime int64
	EndTime int64
	Error    error
}

func NewCmd (name string,tsGetData storage.TsCacheData,tsSetData storage.TsBufferData,startTime,endTime int64,err error) *Cmd {
	cmd:=&Cmd{
		Name:         name,
		TsCacheData:  storage.TsCacheData{},
		TsBufferData: storage.TsBufferData{},
		StartTime:    startTime,
		EndTime:     endTime,
	}
	switch name {
	case "get":
       cmd.TsCacheData=tsGetData
	case "set":
	   cmd.TsBufferData=tsSetData
	}
	return cmd
}



type Client struct {
	net.Conn
	r *bufio.Reader
}

func (client *TsClient) newClient() *Client {
	c, e := net.Dial("tcp", client.Server)
	if e != nil {
		panic(e)
	}
	r := bufio.NewReader(c)
	return &Client{c, r}
}




func NewTsClient(server string, storageType string, cmd *Cmd, operateType string) *TsClient {
	c := TsClient {
		Server:      server,
		StorageType: storageType,
		Cmd:        cmd,
		OperateType: operateType,
	}
	return &c
}

func (client *TsClient) StartRun() {
	c := client.newClient()
	client.PipelineRun(c)
}

func (client *TsClient) PipelineRun(c *Client) {
	if client.StorageType == "" || client.OperateType=="" {
		log.Println("Storage Type and OperateType and Data should be not nil")
		return
	}
	if client.OperateType == "get" {
		c.sendGet(&client.TsCacheData, client.StartTime, client.EndTime)
	}
	if client.OperateType == "set" {
		c.sendSet(&client.TsBufferData)
	}
	if client.OperateType == "del" {
		c.sendDel(&client.TsCacheData, client.StartTime, client.EndTime)
	}
	getData,e:=c.processGetResponse(client)
	log.Println(getData,e)
	setData,e:=c.processSetResponse(client)
	log.Println(setData,e)

}


func (c *Client) sendGet(tsData *storage.TsCacheData,startTime,endTime int64) {
	data,e:=proto.Marshal(tsData)
	if e !=nil {
		log.Println(e.Error())
		return
	}
	st := strconv.FormatInt(startTime, 10)
	et := strconv.FormatInt(endTime, 10)
	dLen:=len(data)
	sLen:=len(st)
	eLen:=len(et)
	_, err := c.Write([]byte(fmt.Sprintf("G%d,%d,%d,%s%s%s",dLen,sLen,eLen,data,st,et)))
	if err != nil {
		log.Println(err.Error())
	}
}

func (c *Client) sendSet(tsData *storage.TsBufferData) {
	data,e:=proto.Marshal(tsData)
	if e !=nil {
		log.Println(e.Error())
		return
	}
	dLen:=len(data)
	_, err := c.Write([]byte(fmt.Sprintf("S%d,%s",dLen,data)))
	if err != nil {
		log.Println(err.Error())
	}
}

func (c *Client) sendDel(tsData *storage.TsCacheData,startTime,endTime int64) {
	data,e:=proto.Marshal(tsData)
	if e !=nil {
		log.Println(e.Error())
		return
	}
	st := strconv.FormatInt(startTime, 10)
	et := strconv.FormatInt(endTime, 10)
	dLen:=len(data)
	sLen:=len(st)
	eLen:=len(et)
	_, err := c.Write([]byte(fmt.Sprintf("D%d,%d,%d,%s%s%s",dLen,sLen,eLen,data,st,et)))
	if err != nil {
		log.Println(err.Error())
	}
}

func readLen(r *bufio.Reader) string {
	tmp, e := r.ReadString(',')
	if tmp == "" {
		return ""
	}
	if e != nil {
		return ""
	}
	return strings.ReplaceAll(tmp, ",", "")
}

func (c *Client) processGetResponse(client *TsClient) (storage.TsCacheData,error) {
	var field *storage.TsCache
	var data storage.TsCacheData
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return data,nil
	}
	switch op {
	case 'R':
		v,n,_:=c.recvResponse()
		redirect:=strings.Split(string(v),":")
		addr:=redirect[0]
		field=client.TsGetData[int(binary.BigEndian.Uint64(n))]
		data.TsGetData=append(data.TsGetData,field)
		cmd:=NewCmd(client.Name,data,client.TsBufferData,client.StartTime,client.EndTime,client.Error)
		c := NewTsClient(addr+":12348", "tsStorage",cmd,cmd.Name)
		client:=c.newClient()
		c.PipelineRun(client)
	case 'V':
		v,_,e:= c.recvResponse()
		_=proto.Unmarshal(v,&data)
		return data,e
	}
	return data,e
}

func (c *Client) processSetResponse(client *TsClient) (storage.TsBufferData,error) {
	var field *storage.TsBuffer
	var data storage.TsBufferData
	op, e := c.r.ReadByte()
	if e != nil {
		if e != io.EOF {
			log.Println("close connection due to error:", e)
		}
		return data,nil
	}
	switch op {
	case 'R':
		v,n,_:=c.recvResponse()
		redirect:=strings.Split(string(v),":")
		addr:=redirect[0]
		field=client.TsBufferData.TsSetData[int(binary.BigEndian.Uint64(n))]
		data.TsSetData=append(data.TsSetData,field)
		cmd:=NewCmd(client.Name,client.TsCacheData,data,client.StartTime,client.EndTime,client.Error)
		c := NewTsClient(addr+":12348", "tsStorage",cmd,cmd.Name)
		client:=c.newClient()
		c.PipelineRun(client)
	case 'V':
		v,_,e:= c.recvResponse()
		_=proto.Unmarshal(v,&data)
		return data,e
	}
	return data,e
}

func (c *Client) recvResponse() ([]byte,[]byte, error) {
	l1 := readLen(c.r)
	l2 := readLen(c.r)
	vLen, e := strconv.Atoi(l1)
	nLen,e := strconv.Atoi(l2)
	if vLen == 0 {
		return nil,nil,nil
	}
	value := make([]byte, vLen)
	_, e = io.ReadFull(c.r, value)
	if e != nil {
		return nil,nil, e
	}
	n := make([]byte, nLen)
	_, e = io.ReadFull(c.r, n)
	if e != nil {
		return nil,nil,e
	}
	return value,n,nil
}


 */