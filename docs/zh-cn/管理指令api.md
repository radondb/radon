Table of Contents
=================

   * [API](#api)
      * [背景](#背景)
      * [radon](#radon)
         * [配置管理(config)](#配置管理config)
         * [设置radon只读(readonly)](#设置radon只读readonly)
         * [限流(throttle)](#限流throttle)
         * [获取radon状态(status)](#获取radon状态status)
      * [分片信息(shard)](#分片信息shard)
         * [shardz](#shardz)
         * [全局表(globals)](#全局表globals)
         * [数据重分布指令(balanceadvice)](#数据重分布指令balanceadvice)
         * [分区表迁移(shift)](#分区表迁移shift)
         * [重新加载路由数据信息(reload)](#重新加载路由数据信息reload)
         * [迁移(migrate)](#迁移migrate)
      * [后端节点相关操作](#后端节点相关操作)
         * [后端分区健康探测](#后端分区健康探测)
         * [添加一个后端分区节点](#添加一个后端分区节点)
         * [移除一个后端分区节点](#移除一个后端分区节点)
      * [版本元数据](#版本元数据)
         * [节点版本信息获取](#节点版本信息获取)
         * [版本检查](#版本检查)
         * [元数据](#元数据)
      * [调试](#调试)
         * [用户活跃连接情况(processlist)](#用户活跃连接情况processlist)
         * [radon内部事务执行情况(txnz)](#radon内部事务执行情况txnz)
         * [radon各分区执行情况(queryz)](#radon各分区执行情况queryz)
         * [Radon配置文件信息(configz)](#radon配置文件信息configz)
         * [显示所有后端节点(backendz)](#显示所有后端节点backendz)
         * [显示当前RadonDB所有的用户库表分区信息(schemaz)](#显示当前radondb所有的用户库表分区信息schemaz)
      * [radon集群节点操作(peers)](#radon集群节点操作peers)
         * [添加一个节点(add peer)](#添加一个节点add-peer)
         * [显示当前radon集群包含的节点(peerz)](#显示当前radon集群包含的节点peerz)
         * [移除一个radon节点(remove peer)](#移除一个radon节点remove-peer)
      * [用户账户(users)](#用户账户users)
         * [创建账户(create user)](#创建账户create-user)
         * [更新用户账户密码](#更新用户账户密码)
         * [删除账户](#删除账户)
         * [显示当前用户账户信息(get users)](#显示当前用户账户信息get-users)

# API

## 背景

RadonDB支持RESTFUL API管理, 大多数的操作任务都可以在WebUI完成.

## radon

### 配置管理(config)

```
Path:    /v1/radon/config
Method:  PUT
Request: {
			"max-connections": 最大可允许的client连接数,
			"max-result-size": 一条查询最大支持的结果集,
			"max-join-rows":   内存中贮存join中间结果的的最大行数,
			"ddl-timeout":     ddl语句执行超时时间(毫秒ms),
			"query-timeout":   DML语句执行超时时间(毫秒ms),
			"twopc-enable":    是否开启两阶段提交(true or false)在radon开启分布式事务时有效,
			"allowip":         ["allow-ip-1", "allow-ip-2", "allow-ip-regexp"],
			"audit-mode":      审计日志模式, "N": 不开启, "R": 只读, "W": 只写, "A": 读写,
			"blocks-readonly": The size of a block when create hash tables,
			"load-balance":    开启读写分离开关(0 or 1, 1是开启) 负载均衡,
         }
         
```

`allowip:`
```
指定具体的ip或者正则表达式形式都是支持的, 我们使用正则表达式来定义指定本地局域网IP段(10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
情形 1: 通配符 "*", 允许所有的ip连接到radon.
情形 2: 我们想指定本地局域网IP段(例如 10.0.0.0/8) 以`10.12`开始, 正则表达式为`10.12.*` or `10.12.[0-9]+.[0-9]+`
情形 3: 我们想指定本地局域网IP段(例如 192.168.0.0/16) 类似`192.168.%.3`, 正则表达式为`192.168.[0-9]+.3`
情形 4: 或者列出所有指定的IP, 比如: "192.168.1.1", "192.168.1.2", "192.168.1.3" ... 
```

`返回状态:`
```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```
`示例: `
```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "max-join-rows":32768, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":true, "load-balance" 1, "allowip": ["192.168.1.1", "192.168.1.2", "172.10.[0-9]+.[0-9]+"]}' http://127.0.0.1:8080/v1/radon/config

---Response---
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 16:19:44 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### 设置radon只读(readonly)

```
Path:    /v1/radon/readonly
Method:  PUT
Request: {
			"readonly":  参数read-only值(true) 或者(false),	                                [required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"readonly":true}' http://127.0.0.1:8080/v1/radon/readonly
		
---Response---
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 16:28:40 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### 限流(throttle)

```
Path:    /v1/radon/throttle
Method:  PUT
Request: {
			"limits": 每秒最大的请求数，默认0(没有限制),  [required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例:`

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"limits":4800}' http://127.0.0.1:8080/v1/radon/throttle

---Response---
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 16:32:43 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### 获取radon状态(status)

```
Path:    /v1/radon/status
Method:  GET
Response:{
			readonly:true(只读)/false(可读可写)
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/radon/status

---Response---
{"readonly":true}
```

## 分片信息(shard)

### shardz

这条命令用于获取路由信息中所有的分表信息

```
Path:    /v1/shard/shardz
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down.
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/shard/shardz

---Response---
{"Schemas":[{"DB":"db_test1","Tables":[{"Name":"t2","ShardKey":"id","Partition":{"Segments":[{"Table":"t2_0000","Backend":"backend1","Range":{"Start":0,"End":128}},{"Table":"t2_0001","Backend":"backend1","Range":{"Start":128,"End":256}},{"Table":"t2_0002","Backend":"backend1","Range":{"Start":256,"End":384}},

......

{"Start":3584,"End":3712}},{"Table":"t1_0029","Backend":"backend1","Range":{"Start":3712,"End":3840}},{"Table":"t1_0030","Backend":"backend1","Range":{"Start":3840,"End":3968}},{"Table":"t1_0031","Backend":"backend1","Range":{"Start":3968,"End":4096}}]}}]}]}
```

### 全局表(globals)

这条指令用于获取路由信息中所有的全局表

```
Path:    /v1/shard/globals
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down.
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/shard/globals

---Response---
{"schemas":[{"database":"db1","tables":["tb2","tb5"]},{"database":"zzq","tables":["tb2"]}]}
```

### 数据重分布指令(balanceadvice)

这条指令用于获取一张最合适的表从数据量最大的节点迁移到最小的节点

```
Path:    /v1/shard/balanceadvice
Method:  GET

Response: [{
			"from-address":     源backend ip(host:port).
			"from-datasize":    源backend数据大小(MB).
			"from-user":        源backend MySQL用户.
			"from-password":    源backend MySQL密码.
			"to-address":       目标backend ip(host:port).
			"to-datasize":      目标backend数据大小(MB).
			"to-user":          目标backend MySQL用户.
			"to-password":      目标backend MySQL密码.
			"database":         所迁移表所在的database.
			"table":            迁移表表名.
			"tablesize":        迁移表的大小.
         }]

```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, radon has no advice.

注意:
如果返回空，意味着不需要做数据重分布。
```

`示例:`

```
$ curl http://127.0.0.1:8080/v1/shard/balanceadvice

---Response---
null
```

### 分区表迁移(shift)

这条api用来从一个后端迁移一张分区表到另一个后端

```
Path:    /v1/shard/shift
Method:  POST
Request: {
			"database":	"database name",	[required]
			"table":	 "table name",	    [required]
			"from-address":	"the from backend address(host:port)",	[required]
			"to-address":	"the to backend address(host:port)",	[required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, radon has no advice.
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"database": "db_test1", "table": "t1", "from-address": "127.0.0.1:3306", "to-address": "127.0.0.1:3306"} http://127.0.0.1:8080/v1/shard/shift
```


### 重新加载路由数据信息(reload)

该api用来从metadir重新加载路由信息.

```
Path:    /v1/shard/reload
Method:  POST
Request: NIL
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, radon has no advice.
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X POST http://127.0.0.1:8080/v1/shard/reload

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 02:07:15 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```


### 迁移(migrate)

该api用来迁移一个后端节点数据到另一个后端节点。

```
Path:    /v1/shard/migrate
Method:  POST
Request: {
			"from":                      "Source MySQL backend(host:port)",                                   [required]
			"from-user":                 "MySQL user, must have replication privilege",                       [required]
			"from-password":             "MySQL user password",                                               [required]
			"from-database":             "Source database",                                                   [required]
			"from-table":                "Source table",                                                      [required]
			"to":                        "Destination MySQL backend(host:port)",                              [required]
			"to-user":                   "MySQL user, must have replication privilege",                       [required]
			"to-password":               "MySQL user password",                                               [required]
			"to-database":               "Destination database",                                              [required]
			"to-table":                  "Destination table",                                                 [required]
			"radonurl":                  "Radon RESTful api(default: http://peer-address)",
			"rebalance":                 "Rebalance means a rebalance operation, which from table need cleanup after shifted(default false)",
			"cleanup":                   "Cleanup the from table after shifted(defaults false)",
			"mysqldump":                 "mysqldump path",
			"threads":                   "shift threads num(defaults 16)",
			"behinds":                   "position behinds num(default 2048)",
			"checksum":                  "Checksum the from table and to table after shifted(defaults true)",
			"wait-time-before-checksum": "seconds sleep before checksum"
         }
```

`返回状态:`

```
	200: StatusOK
	204: StatusNoContent
	500: StatusInternalServerError
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"from": "127.0.0.1:3000","from-user":"usr","from-password":"123456","from-table":"t1","from-database":"test", "to":"127.0.0.1:4000","to-user":"usr","to-password":"123456","to-database":"test","to-table":"t1","cleanup":true}' http://127.0.0.1:8080/v1/shard/migrate

---Response---
HTTP/1.1 200 OK
Date: Fri, 10 Jan 2020 10:56:31 GMT
Content-Length: 0
```

## 后端节点相关操作

### 后端分区健康探测

该api可以通过发送PING(select 1) 命令来探测后端节点的健康状态.

```
Path:    /v1/radon/ping
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down.
```

`示例:`

```
$ curl http://127.0.0.1:8080/v1/radon/ping
```

### 添加一个后端分区节点

该api用来添加一个后端配置信息.

```
Path:    /v1/radon/backend
Method:  POST
Request: {
			"name":            "The unique name of this backend",												[required]
			"address":         "The endpoint of this backend",													[required]
			"replica-address": "The slave node of this backend, readonly",
			"user":            "The user(super) for radon to be able to connect to the backend MySQL server",	[required]
			"password":        "The password of the user",														[required]
			"max-connections": The maximum permitted number of backend connection pool,							[optional]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend1", "address": "127.0.0.1:3306", "replica-address": "127.0.0.1:3306", "user": "root", "password": "318831", "max-connections":1024}' http://127.0.0.1:8080/v1/radon/backend

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 06:13:59 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### 移除一个后端分区节点
该api用来移除一个后端分区节点

```
Path:    /v1/radon/backend/{backend-name}
Method:  DELETE
```
`返回状态:`
```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```
`示例: `
```
$ curl -X DELETE http://127.0.0.1:8080/v1/radon/backend/backend1
```

## 版本元数据

该API用来显示当前节点radon主从同步版本信息。

### 节点版本信息获取

```
Path:    /v1/meta/versions
Method:  GET
Response:{
			Ts int64 `json:"version"`
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/meta/versions

---Response---
{"version":1523328058632112022}
```


### 版本检查

```
Path:    /v1/meta/versioncheck
Method:  GET
Response:{
			"latest":true,
			"peers":["127.0.0.1:8080"]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`示例:`

```
$ curl http://127.0.0.1:8080/v1/meta/versioncheck

---Response---
{"latest":true,"peers":["127.0.0.1:8080"]}
```

### 元数据

```
Path:    /v1/meta/metas
Method:  GET
Response:{
			Metas map[string]string `json:"metas"`
         }

```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/meta/metas

---Response---
{"metas":{"backend.json":"{\n\t\"backends\": null\n}","db_test1/t1.json":"{\n\t\"name\": \"t1\",\n\t\"shardtype\": \"HASH\",\n\t\"shardkey\": \"id\",\n\t\"partitions\": [\n\t\t{\n
.....
.....
t\t{\n\t\t\t\"table\": \"t2_0029\",\n\t\t\t\"segment\": \"3712-3840\",\n\t\t\t\"backend\": \"backend1\"\n\t\t},\n\t\t{\n\t\t\t\
```

## 调试

### 用户活跃连接情况(processlist)
该api显示当前活跃的用户连接.

```
Path:    /v1/debug/processlist
Method:  GET
Response: [{
			"id":      The connection identifier.
			"user":    The radon user who issued the statement.
			"host":    The host name of the client issuing the statement.
			"db":      The default database.
			"command": The type of command the thread is executing.
			"time":    The time in seconds that the thread has been in its current state.
			"state":   The state what the thread is doing.
			"info":    The statement the thread is executing.
         }]
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`示例:`

```
$ curl http://127.0.0.1:8080/v1/debug/processlist
---Response---
[{"id":1,"user":"root","host":"127.0.0.1:40742","db":"","command":"Sleep","time":41263,"state":"","info":""}]
```

### radon内部事务执行情况(txnz)
该api用来显示哪些事务正在运行.

```
Path:    /v1/debug/txnz/:limit
Method:  GET
Response: [{
			"txnid":    The transaction identifier.
			"start":    The transaction start time.
			"duration": The transatcion duration time.
			"state":    The statement the transaction is executing.
			"xaid":     The xa identifier if the twopc is enabled.
			"sending":  The backend numbers which the transaction fanout to.
         }]
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/debug/txnz/10
---Response(now backend does nothing, return null)--
null
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

### radon各分区执行情况(queryz)
该api显示哪些查询正在运行.

```
Path:    /v1/debug/queryz/:limit
Method:  GET
Response: [{
			"id":       The connection ID.
			"host":     The backend address who is issuing this query.
			"start":    The query start time.
			"duration": The query duration time.
			"query":    The query which is executing.
         }]
```

`示例:`

``` 
$ curl http://127.0.0.1:8080/v1/debug/queryz/10
---Response---
null
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

### Radon配置文件信息(configz)
该api显示RadonDB的配置文件信息.

```
Path:    /v1/debug/configz
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例:`

```
$ curl http://127.0.0.1:8080/v1/debug/configz

---Response---
{"proxy":{"allowip":["127.0.0.1","127.0.0.2"],"meta-dir":"bin/radon-meta","endpoint":":3306","twopc-enable":true,"max-connections":1024,"max-result-size":1073741824,"ddl-timeout":3600,"query-timeout":600,"peer-address":"127.0.0.1:8080"},"audit":{"mode":"N","audit-dir":"bin/radon-audit","max-size":268435456,"expire-hours":1},"router":{"slots-readonly":4096,"blocks-readonly":128},"binlog":{"binlog-dir":"bin/radon-binlog","max-size":134217728},"log":{"level":"INFO"}}
```

### 显示所有后端节点(backendz)
显示RadonDB所有后端节点.

```
Path:    /v1/debug/backendz
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/debug/backendz

---Response---
[]
```

### 显示当前RadonDB所有的用户库表分区信息(schemaz)

```
Path:    /v1/debug/schemaz
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl http://127.0.0.1:8080/v1/debug/schemaz

---Response---
{"db_test1":{"DB":"db_test1","Tables":{"t1":{"Name":"t1","ShardKey":"id","Partition":{"Segments":[{"Table":"t1_0000","Backend":"backend1","Range":{"Start":0,"End":128}},{"Table":"t1_0001","Backend":"backend1","Range":{"Start":128,"End":256}},{"Table":"t1_0002","Backend":"backend1","Range":{"Start":256,"End":384}},
....
....
:"backend1","Range":{"Start":3712,"End":3840}},{"Table":"t2_0030","Backend":"backend1","Range":{"Start":3840,"End":3968}},{"Table":"t2_0031","Backend":"backend1","Range":{"Start":3968,"End":4096}}]}}}}}
```

## radon集群节点操作(peers)

### 添加一个节点(add peer)

```
Path:    /v1/peer/add
Method:  POST
Request: {
			"address":         "The REST address of this peer",													[required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "127.0.0.1:8080"}' http://127.0.0.1:8080/v1/peer/add

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:17:30 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```


### 显示当前radon集群包含的节点(peerz)

```
Path:    /v1/peer/peerz
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```
`示例:`
```
$ curl http://127.0.0.1:8080/v1/peer/peerz

---Response---
["127.0.0.1:8080"]
```

### 移除一个radon节点(remove peer)

```
Path:    /v1/peer/remove
Method:  POST
Request: {
		"address":  "The REST address of this peer",     [required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "127.0.0.1:8080"}' http://127.0.0.1:8080/v1/peer/remove

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:21:09 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

## 用户账户(users)

可以连接到radon的账户密码.

### 创建账户(create user)

```
Path:    /v1/user/add
Method:  POST
Request: {
			"databases": "database1,database2", [optional]
			"user": "user name",	[required]
			"password": "password",	[required]
			"privilege": "select, insert, update, delete",	[optional]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down
```

`示例: `

"databases" 是授权访问的数据库列表, 如果是空，默认授权所有的数据库都可以访问.
"privilege" 由[select | insert | update | delete]组成, 通过","分隔. 如果为空，默认授权所有权限.

```
---backend should not be null---
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend1", "address": "127.0.0.1:3306", "user": "root", "password": "318831", "max-connections":1024}' http://127.0.0.1:8080/v1/radon/backend
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:35:22 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8

$ curl -i -H 'Content-Type: application/json' -X POST -d '{"databases":"db1,db2", "user": "test", "password": "test", "privilege": "select, update"}' http://127.0.0.1:8080/v1/user/add
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:35:27 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8

$ curl -i -H 'Content-Type: application/json' -X POST -d '{"databases":"", "user": "u2", "password": "111111", "privilege": "select, update, delete"}' http://127.0.0.1:8080/v1/user/add
HTTP/1.1 200 OK
Date: Wed, 15 May 2019 02:46:42 GMT
Content-Length: 0

$ curl -i -H 'Content-Type: application/json' -X POST -d '{"user": "u3", "password": "111111", "privilege": ""}' http://127.0.0.1:8080/v1/user/add
HTTP/1.1 200 OK
Date: Wed, 15 May 2019 02:49:30 GMT
Content-Length: 0

```


### 更新用户账户密码

```
Path:    /v1/user/update
Method:  POST
Request: {
			"user": "user name",	[required]
			"password": "password",	[required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例:`

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"user": "test", "password": "test"}' http://127.0.0.1:8080/v1/user/update

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:39:31 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### 删除账户

```
Path:    /v1/user/remove
Method:  POST
Request: {
			"user": "user name",	[required]
         }
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例:`

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"user": "test"}' http://127.0.0.1:8080/v1/user/remove
---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:41:14 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### 显示当前用户账户信息(get users)
```
Path:    /v1/user/userz
Method:  GET
```

`返回状态:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`示例:`

```
$ curl http://127.0.0.1:8080/v1/user/userz
---Response---
[{"User":"root","Host":"%"},{"User":"test","Host":"%"},{"User":"mysql.session","Host":"localhost"},{"User":"mysql.sys","Host":"localhost"},{"User":"root","Host":"localhost"},{"User":"test","Host":"localhost"}]%
```
