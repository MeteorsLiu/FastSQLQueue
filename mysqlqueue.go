package mysqlqueue

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type SQLQueue struct {
	in         chan string
	key        chan string
	value      chan interface{}
	DoneSignal chan struct{}
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
func NewMySQLQueue(addr, port, user, password, db string, sysSignal <-chan struct{}) *SQLQueue {
	in := make(chan string)
	key := make(chan string)
	value := make(chan interface{})
	DoneSignal := make(chan struct{})
	go func(in chan string, key chan string, value chan interface{}, DoneSignal chan struct{}) {
		var columns []string
		var count int

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", user, password, addr, port, db))
		defer db.Close()
		if err != nil {
			log.Fatal(err)
		}
		for {
			select {
			case v := <-in:
				query, err := db.Query(v)
				if err != nil {
					log.Fatal(err)
				}
				//I don't want to write this code.However,it's necessary
				//And these slices are dynamic.I think there's no way to optimize them.
				//Source: https://stackoverflow.com/questions/17845619/how-to-call-the-scan-variadic-function-using-reflection
				columns, _ = query.Columns()
				count = len(columns)
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
				}

				//Clear slices and free resources.
				values = nil
				valuePtrs = nil
				query.Close()
				DoneSignal <- struct{}{}
				value <- nil
				key <- ""
				

			case <-sysSignal:
				return
			}

		}
	}(in, key, value, DoneSignal)

	return &SQLQueue{
		in:         in,
		key:        key,
		value:      value,
		DoneSignal: DoneSignal,
	}
}

//How to use
//1. Call init func NewMySQLQueue
//Like client := NewMySQLQueue()
//2. Format your SQL
//Like fmt.Sprintf("SELECT * FROM xxx WHERE xxx=%s", xxx)
//3. Call Query
//for i,v := range client.Query(SQL)

func (s *SQLQueue) Query(SQL string)  map[int]map[string]string {
	s.in <- SQL
	count := 0
	var MapSlice = map[int]map[string]string{}
	for {
		select {
		case <-s.DoneSignal:
			return MapSlice
		case val := <-s.value:
			MapSlice[count] = map[string]string{}
			key := <-s.key
			switch v := val.(type) {
			case []byte:
				MapSlice[count][key] = string(v)
			default:
				MapSlice[count][key] = ""

			}
			
			count++
		}

	}

	return MapSlice

}
