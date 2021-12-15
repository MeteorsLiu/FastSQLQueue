package main

import (
	Queue "github.com/MeteorsLiu/MySQLQueue"
	"fmt"
	"sync"
)


func main() {
	goCh := make(chan struct{})
	Client := Queue.NewMySQLQueue("localhost", "3306", "test", "123456", "test", goCh)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sql, _ := Client.Query(fmt.Sprintf("SELECT text FROM test WHERE name='%s'", Queue.Mysql_real_escape_string("TestES")))
		for _, v := range sql {
			fmt.Println(v["text"])
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()
		sql, _ := Client.Query(fmt.Sprintf("SELECT text FROM test WHERE name='%s'", Queue.Mysql_real_escape_string("TestES")))
		for _, v := range sql {
			fmt.Println(v["text"])
		}
	}()

	fmt.Println(Client.Query("SELECT * FROM test"))
	wg.Wait()

	//Shutdown Goroutine Worker
	close(goCh)
}
