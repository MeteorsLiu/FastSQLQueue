package mysqlqueue

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type SQLQueue struct {
	in         chan string
	key        chan string
	value      chan interface{}
	ListSignal chan struct{}
	DoneSignal chan struct{}
	safeLock   *sync.Mutex
}
// MySQL Params Escape 
// For Safety, please call this function when you call Query
// MySQL字符串过滤，由于Golang MySQLDriver没有内置，因而自己参考MySQL的C库写了一个
// 为了安全，请务必在调用Query前调用这个函数过滤字符串
func mysql_real_escape_string(param string) string {
	var sb strings.Builder
	//Source: #789 escape_string_for_mysql https://github.com/mysql/mysql-server/blob/5.7/mysys/charset.c
	for _, v := range []byte(param) {
		switch v {
		case '\n':
			sb.WriteByte('\\')
			sb.WriteByte('n')
		case '\r':
			sb.WriteByte('\\')
			sb.WriteByte('r')
		case 0:
			sb.WriteByte('\\')
			sb.WriteByte('0')
		case '\\':
			sb.WriteByte('\\')
			sb.WriteByte('\\')
		case '\'':
			sb.WriteByte('\\')
			sb.WriteByte('\'')
		case '"':
			sb.WriteByte('\\')
			sb.WriteByte('"')
		case '\032':
			sb.WriteByte('\\') /* This gives problems on Win32 */
			sb.WriteByte('Z')
		default:
			sb.WriteByte(v)
		}
	}
	return sb.String()
}

// Init
// Params Introduction:
// Please attention:
// You NEED to fill this params
// THIS FUNCTION WON'T HELP YOU SET DEFAULT VALUE
// addr MySQL Server Addr
// port MySQL Server Port
// user MySQL User
// password MySQL Password
// db MySQL Database
// sysSignal A shutdonw signal for shuting down goroutine daemon worker to release the memory
//
// Usage:
// sigCh := make(chan struct{})
// client := mysqlqueue.NewMySQLQueue(..., sigCh)
// When you are about to exit the main
// close(sigCh) This will shut down the goroutine
// 初始化
// 参数解释:
// 请注意:
// 这个参数没有初始值，务必填写完整
// addr MySQL 服务器地址
// port MySQL 服务器端口
// user MySQL 服务器用户
// password MySQL 服务器密码
// db MySQL Database
// sysSignal 用户关闭位于后台的Goroutine，帮助GoGC回收
//
// 使用方法：
// sigCh := make(chan struct{})
// client := mysqlqueue.NewMySQLQueue(..., sigCh)
// 当你main要退出的时候
// close(sigCh) 这就能安全关闭了
func NewMySQLQueue(addr, port, user, password, db string, sysSignal <-chan struct{}) SQLQueue {
	in := make(chan string)
	key := make(chan string)
	value := make(chan interface{})
	ListSignal := make(chan struct{})
	DoneSignal := make(chan struct{})
	var Lock sync.Mutex
	go func(in chan string, key chan string, value chan interface{}, ListSignal chan struct{}, DoneSignal chan struct{}) {
		var columns []string
		var count int

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", user, password, addr, port, db))
		defer db.Close()
		//Ensure sender goroutine exits when this goroutine exits unexpectedly
		defer close(DoneSignal)
		if err != nil {
			//连都连不上还处理蛇皮
			value <- err
			return
		}
		for {
			select {
			case v := <-in:
				query, err := db.Query(v)
				if err != nil {
					value <- err
					continue
				}
				//I don't want to write this code.However,it's necessary
				//And these slices are dynamic.I think there's no way to optimize them.
				//Source: https://stackoverflow.com/questions/17845619/how-to-call-the-scan-variadic-function-using-reflection
				columns, _ = query.Columns()
				count = len(columns)
				if count > 0 {
					values := make([]interface{}, count)
					valuePtrs := make([]interface{}, count)
					for query.Next() {
						for i := range columns {
							valuePtrs[i] = &values[i]
						}
						query.Scan(valuePtrs...)
						for i, col := range columns {
							value <- values[i]
							key <- col
						}
						ListSignal <- struct{}{}
					}
					//Clear slices and free resources.
					values = nil
					valuePtrs = nil
				}
				query.Close()
				DoneSignal <- struct{}{}

			case <-sysSignal:
				return
			}

		}
	}(in, key, value, ListSignal, DoneSignal)

	return SQLQueue{
		in:         in,
		key:        key,
		value:      value,
		ListSignal: ListSignal,
		DoneSignal: DoneSignal,
		safeLock:   &Lock,
	}
}

//How to use
//1. Call init func NewMySQLQueue
//Like client := NewMySQLQueue()
//2. Format your SQL
// Remeber Call Escape func when you do this.
//Like fmt.Sprintf("SELECT * FROM xxx WHERE xxx=%s", xxx)
//3. Call Query
// val, err := range client.Query(SQL)
// val is a slice of map.
// for _, v := range val {
// v["xxx"]
//}
// Result Format:
// Slice
// Error
// v[Column Name] = Value
//如何使用
//在完成NewMySQLQueue初始化后
// 记得Sprintf前先调用escape
// val, err :=  Client.Query(fmt.Sprintf("SELECT * FROM xxx WHERE xxx=%s", xxx))
// val是包含许多组map的slice
// for _, v := range val {
// v["xxx"]
//}
// 返回格式:
// Slice和Error
// v[Column Name] = Value
func (s SQLQueue) Query(SQL string) ([]map[string]string, error) {
	s.safeLock.Lock()
	defer s.safeLock.Unlock()
	s.in <- SQL
	var tempMap = map[string]string{}
	var MapSlice = []map[string]string{}
	for {
		select {
		case <-s.DoneSignal:
			return MapSlice, nil
		case <-s.ListSignal:
			MapSlice = append(MapSlice, tempMap)
			tempMap = map[string]string{}
		case val := <-s.value:
			//Empty interface{}
			key := <-s.key

			switch v := val.(type) {
			case []byte:
				tempMap[key] = string(v)
			case error:
				return nil, v
			default:
				tempMap[key] = ""

			}

		}

	}
	return MapSlice, nil

}

// This Function is to call those SQL cmds without result
// Like DELETE FROM xxx
// err := Client.Exec(Formatted SQL)
// Remeber Call Escape func when you do this.
// 这个函数用于执行那些没有返回值的SQL
// 如DELETE
// err := Client.Exec(写好的SQL)
func (s SQLQueue) Exec(SQL string) error {
	s.safeLock.Lock()
	defer s.safeLock.Unlock()
	s.in <- SQL
	for {
		select {
		case <-s.DoneSignal:
			return nil
		case val := <-s.value:
			//By default, no value will be received
			switch v := val.(type) {
			case error:
				return v
			default:
				return nil
			}
		}

	}

	return nil

}
