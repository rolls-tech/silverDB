package client

import (
	"testing"
)

const timeTemplate  = "2006-01-02 15:04:05:00"
const RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"

func TestClient_Operate(t *testing.T) {
	/*t2:=time.Now().UnixNano()
	fmt.Println(t2)
	t4 := time.Unix(0,t2).Format(RFC3339Nano)
	fmt.Println(t4)
	startTime:=time.Now().UnixNano()
	addTime:=startTime+time.Hour.Nanoseconds()
	fmt.Println(addTime)
	sstartTime := strconv.FormatInt(startTime,10)
	fmt.Println(sstartTime)
	i, _ := strconv.ParseInt(sstartTime, 10, 64)
	fmt.Println(i)
	t5 := time.Unix(startTime,0).Format(RFC3339Nano)
	fmt.Println(t5)
	fmt.Println(time.Hour.Nanoseconds())
	t6,_:=time.ParseDuration("24h")
	t7:=time.Now().Add(t6).Format(RFC3339Nano)
	log.Println(t7) */


	var cmds []*Cmd
	cmd := Cmd {
		Name:     "get",
		DataBase: "test1",
		Table:   "test1",
		RowKey:  "zzz",
		DataTime: "",
		StartTime: "1576072645676694700",
		EndTime: "1576159045676694700",
		Key:      "bbb",
		Value:    "",
		Error:    nil,
	}
	cmds = append(cmds, &cmd)
	c := NewClient("127.0.0.1:12346", "tsStorage", cmds, cmd.Name)
	c.Operate()
}
