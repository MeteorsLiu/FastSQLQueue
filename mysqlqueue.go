package mysqlqueue

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type SQLQueue struct {
	in  <-chan string
	out chan<- map[string]interface{}
}

//Read-only Channel: in
//Send-only Channel: out
func NewMySQLQueue(addr, port, user, password, db string, sysSignal <-chan struct{}) SQLQueue {
	in := make(chan string)
	out := make(chan map[string]interface{})
	go func(in <-chan string, out chan<- map[string]interface{}) {
		var columns []string
		var count int

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", user, password, addr, port, db))
		defer db.Close()
		if err != nil {
			return
		}
		for {
			select {
			case v, ok := <-in:
				if !ok {
					return
				}
				query, err := db.Query(v)
				if err != nil {
					out <- nil
				}
				//I don't want to write this code.However,it's necessary
				//And these slices are dynamic.I think there's no way to optimize them.
				//Source: https://stackoverflow.com/questions/17845619/how-to-call-the-scan-variadic-function-using-reflection
				columns, _ = query.Columns()
				count = len(columns)
				values := make([]interface{}, count)
				valuePtrs := make([]interface{}, count)
				var retmap map[string]interface{}
				for query.Next() {
					for i := range columns {
						valuePtrs[i] = &values[i]
					}
					rows.Scan(valuePtrs...)
					for i, col := range columns {
						//Type assertions
						//Fuck U
						switch v := values[i].(type) {
							case int:
								retmap[col] = values[i].(int)
							case int32:
								retmap[col] = values[i].(int32)
							case int64:
								retmap[col] = values[i].(int64)
							case string:
								retmap[col] = values[i].(string)
							case float32:
								retmap[col] = values[i].(float32)
							case float64:
								retmap[col] = values[i].(float64)
							case uint:
								retmap[col] = values[i].(uint)
							case uint32:
								retmap[col] = values[i].(uint32)
							case uint64:
								retmap[col] = values[i].(uint64)

							default:
								retmap[col] = values[i].(interface{})
						}
						retmap = append(retmap, retmap[col])
					}

					out <- retmap
				}
				//Clear slices and free resources.
				values = nil
				valuePtrs = nil
				query.Close()
			case <-sysSignal:
				return
			}


		}
	}(in, out)

	return &SQLQueue{
		in:  in,
		out: out,
	}
}

//How to use
//1. Call init func NewMySQLQueue
//Like client := NewMySQLQueue()
//2. Format your SQL
//Like fmt.Sprintf("SELECT * FROM xxx WHERE xxx=%s", xxx)
//3. Call Query
//for i,v := range client.Query(SQL)

func (s *SQLQueue) Query(SQL string) chan<- map[string]interface{} {
	s.in <- SQL
	return <-s.out
}