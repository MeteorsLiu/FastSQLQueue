# FastSQLQueue
A Simple MySQL Queue for Golang


# 有待优化

1. Query会新产生map，这是完全可以避免的

2. 没有Context

3. 每一个查询请求都会产生两个slices，无疑增大GoGC压力，想办法解决中

