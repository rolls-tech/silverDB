package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type deviceData struct {
	deviceId string
	deviceGroup string
	deviceName string
	temperature int
	humidity float64
	status bool
}

const groupPrefix="group"
const namePrefix="dev"


func generatorTemperature() int {
	// 0-100
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100)
}

func generatorHumidity() float64 {
	//0-1
	rand.Seed(time.Now().UnixNano())
	return rand.Float64()
}

func generatorStatus() bool {
	rand.Seed(time.Now().UnixNano())
	status:=rand.Intn(2)
	if status == 1 {
		return true
	}
	return false
}

func NewDeviceData(id int) *deviceData {
	return &deviceData {
		deviceId: strconv.Itoa(id),
		deviceGroup: fmt.Sprintf("%s%d",groupPrefix,id),
		deviceName: fmt.Sprintf("%s%d",namePrefix,id),
		temperature: generatorTemperature(),
		humidity: generatorHumidity(),
		status: generatorStatus(),
	}
}

func (d *deviceData) generatorData(id int64) string {
  data:= fmt.Sprintf("%s,%s,%s %d,%f,%t,%d",d.deviceId,d.deviceGroup,d.deviceName,
  	d.temperature,d.humidity,d.status,time.Now().UnixNano()+id)
  return data
}

var dataDir string
var fileNums,deviceNums,deviceRecords int

func init() {
	flag.StringVar(&dataDir, "dir", "/Volumes/info/data/", "device data dir")
	flag.IntVar(&fileNums, "fn", 10, "number of data file")
	flag.IntVar(&deviceNums, "dn", 1000, "number of device")
	flag.IntVar(&deviceRecords, "dr", 10000, "rows of every device")
	flag.Parse()
	fmt.Println("device data dir", dataDir)
	fmt.Println("number of data file", fileNums)
	fmt.Println("number of device", deviceNums)
	fmt.Println("rows of every device", deviceRecords)
}

func main() {
	_,err:=os.Stat(dataDir)
	if os.IsNotExist(err) {
       err:=os.Mkdir(dataDir,os.ModePerm)
       if err !=nil {
       	   log.Fatal("mkdir "+dataDir+" is failed ! ",err)
	   }
	}
	if deviceNums*deviceRecords % fileNums == 0 && deviceNums > fileNums {
	  var wg sync.WaitGroup
	  //var mu sync.Mutex
	  nums:=deviceNums*deviceRecords / fileNums
	  for i:=0; i< fileNums; i++ {
	  	  wg.Add(1)
		  file,err:=os.Create(dataDir+fmt.Sprintf("%s%d","data_",i))
		  if err != nil {
			  log.Fatal("create dataFile is failed ! ",err)
		  }
		  go func(file *os.File,wg *sync.WaitGroup,i int) {
		  	defer file.Close()
		  	j:=0
		  	recordN:=1
		  	writer:=bufio.NewWriterSize(file,1 << 20 )
		  	for recordN <= deviceNums / fileNums  && j < nums {
				  recordM:=0
				  for recordM < deviceRecords {
					  deviceData:=NewDeviceData(recordN+(deviceNums/fileNums) * i)
					  data:=deviceData.generatorData(int64(recordM))
					  _,err:=writer.WriteString(data+"\n")
					  if err != nil {
						  log.Fatal("write data file is failed ! ",err)
					  }
					  recordM++
					  j++
				  }
				  recordN++
			  }
			  writer.Flush()
			  wg.Done()
		  }(file,&wg,i)
	  }
	  wg.Wait()
    } else {
  	  log.Fatal("ensure every dataFile record nums is same")
    }
}
