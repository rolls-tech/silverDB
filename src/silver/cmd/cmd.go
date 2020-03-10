package cmd

import (
	"log"
	"silver/storage"
)

type Cmd struct {
	CmdType string
	Gd  GetCmd
	Sd  SetCmd
}


type SetCmd struct {
    Wp *storage.WPoint
}

func NewGetCmd(dataBase,tableName,fieldKey string,tags map[string]string,startTime,endTime int64) *GetCmd {
	gc := &GetCmd {
		DataBase:  dataBase,
		TableName: tableName,
		Tags:      tags,
		FieldKey:  fieldKey,
		StartTime: startTime,
		EndTime:   endTime,
	}
	return gc
}

func NewSetCmd(dataBase,tableName,fieldKey string,tags map[string]string, fieldValue map[int64]float64) *SetCmd {
   kv:=&storage.Value{
	   Kv:            fieldValue,
   }
   value:=make(map[string]*storage.Value)
   value[fieldKey]=kv
   wp := &storage.WPoint{
	   DataBase:             dataBase,
	   TableName:            tableName,
	   Tags:                 tags,
	   Value:                value,
   }
   sc := &SetCmd {
   	 Wp:wp,
   }
   return sc
}

func NewCmd (cmdType,dataBase,tableName,fieldKey string,tags map[string]string,
	startTime,endTime int64,fieldValue map[int64]float64) *Cmd {
		c:=&Cmd{
			CmdType: cmdType,
			Gd:  GetCmd{},
			Sd:  SetCmd{},
		}
	switch cmdType {
	case "set":
		c.Sd=*NewSetCmd(dataBase,tableName,fieldKey,tags,fieldValue)
		return c
	case "get":
		c.Gd=*NewGetCmd(dataBase,tableName,fieldKey,tags,startTime,endTime)
		return c
	}
	log.Println("Unknown command !")
	return nil
}


