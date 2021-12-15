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

//Read-only Channel: in
//Send-only Channel: out
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
		if err != nil {
			//连都连不上还处理蛇皮
			log.Fatal(err)
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
//Like fmt.Sprintf("SELECT * FROM xxx WHERE xxx=%s", xxx)
//3. Call Query
//for i,v := range client.Query(SQL)

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
