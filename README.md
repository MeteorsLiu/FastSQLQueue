# FastSQLQueue
A Simple MySQL Queue for Golang

# Feature

1. Thread Safe
2. High Performance and fast
3. Lightweight
4. SQL Queue and re-format to Map type
5. Easy to use


# 设计思路

利用Channel特性，完成多Goroutine下排队读写的问题，一个Channel两端会有任意一方被block，直到任务完成为止

在Golang设计中，Channel是一个阻塞的FIFO队列，但goroutine是无序的，尽管channel是队列，但仍不知道哪个goroutine会优先

例如：

A,B,C三人同时请求

那么A或B或C随机一方的SQL被后台的Goroutine接收，开始执行任务，而Channel一旦被接收后，由于case里面代码仍未执行完毕，因此Channel又可处于发送状态，又可以发送，但发送过后由于后台Goroutine仍未完成请求，因此会被暂停接收，直到任务完成为止，才重新开始继续接收，这样达到了前面排队的目的。而出channel是为了确保数据唯一。

# 局限性

没有多worker执行任务的功能，原因是作者明确说明了不允许多goroutine同时操作

# 性能测试

测试机器：

中国香港腾讯云轻量(E5 26xx V4 1核心,2GB)

测试MariaDB版本：
10.2

测试命令：`go test -bench . -benchtime=10000x`

测试函数

`db.Query`

`mysqlqueue中的Query`

测试语句：`SELECT * FROM test`

测试方法：固定10000次，不限时间

测试结果：

本库的Query完成一万次仅需要0.8s

db.Query需要2.5s

是原生库的三倍

```
go test -bench . -benchtime=10000x
goos: linux
goarch: amd64
pkg: github.com/MeteorsLiu/MySQLQueue
cpu: Intel(R) Xeon(R) CPU E5-26xx v4
BenchmarkDB        	   10000	     72392 ns/op
BenchmarkFormat    	   10000	     65299 ns/op
BenchmarkFor       	   10000	     65766 ns/op
BenchmarkNative    	   10000	    237251 ns/op
BenchmarkNativeFor 	   10000	    231025 ns/op
PASS
ok  	github.com/MeteorsLiu/MySQLQueue	6.740s

```



# 有待优化

~~1. Query会新产生map，这是完全可以避免的~~

~~2. 没有Context~~

3. 每一个查询请求都会产生两个slices，无疑增大GoGC压力，想办法解决中

# 文档

## 使用方法

`ctx := context.WithCancel(context.Backgroud)`
`Client := mysqlqueue.NewMySQLQueue(...各种参数, ctx)`

为了更贴合PHP Prepare 语句

我提供了一个函数来格式化SQL

BindParam()

和

AutoBindParam()


举个例子

`SQL, err := mysqlqueue.BindParam("SELECT name FROM test WHERE ID=?", "d", id)`

`SQL, err := mysqlqueue.AutoBindParam("SELECT name FROM test WHERE ID=?", id)`

**解释**

?为代表绑定参数的

d代表绑定参数类型，遵循Golang Sprintf Format格式

d/i为int

s为string

id为要绑定的变量

具体用法和PHP Bind Param一致

AutoBindParam则和原生sql库差不多，不需要写类型

**务必判断err不为nil**

是不是很像PHP Bind Params?

还有更像的

格式化URL后可以直接调用了

`Result, err := Client.Query(SQL)`

使用之前务必判断err是否等于nil哦

`
for _, val := range Result {
  val[行名(Column Name)] = 对应的值
}
`

是不是很像PHP的Fetch Assoc？

哈哈，我喜欢PHP，我写golang时候都想着它

那么如何换db？

首先先关闭goroutine，我提供了ctx来方便用户关闭

`cancel()`

即可

然后再重复

`Client = mysqlqueue.NewMySQLQueue(...各种参数, ctx)`





# 参数解释

NewMySQLQueue函数:

`addr MySQL Server Addr`
`port MySQL Server Port`
`user MySQL User`
`password MySQL Password`
`db MySQL Database`

Query:

SQL 格式化好的字符串

返回值：多个Map

Exec:

SQL 格式化好的字符串

返回值：nil(空)

BindParam():

用法见上

