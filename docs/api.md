Contents
=================

* [API](#api)
   * [Background](#background)
   * [radon](#radon)
      * [config](#config)
      * [readonly](#readonly)
      * [throttle](#throttle)
      * [status](#status)
   * [shard](#shard)
      * [shardz](#shardz)
      * [globals](#globals)
      * [balanceadvice](#balanceadvice)
      * [shift](#shift)
      * [reload](#reload)
      * [migrate](#migrate)
   * [backend](#backend)
      * [health](#health)
   * [backends](#backends)
      * [add](#add)
      * [remove](#remove)
   * [meta](#meta)
      * [versions](#versions)
      * [versioncheck](#versioncheck)
      * [metas](#metas)
   * [debug](#debug)
      * [processlist](#processlist)
      * [txnz](#txnz)
      * [queryz](#queryz)
      * [configz](#configz)
      * [backendz](#backendz)
      * [schemaz](#schemaz)
   * [peers](#peers)
      * [add peer](#add-peer)
      * [peerz](#peerz)
      * [remove peer](#remove-peer)
   * [users](#users)
      * [create user](#create-user)
      * [update user password](#update-user-password)
      * [drop user](#drop-user)
      * [get users](#get-users)

# API

## Background

This document describes the RadonDB REST API, which allows users to achieve most tasks on WebUI.

## radon

### config

```
Path:    /v1/radon/config
Method:  PUT
Request: {
			"max-connections":        The maximum permitted number of simultaneous client connections,
			"max-result-size":        The maximum result size(in bytes) of a query,
			"max-join-rows":          The maximum number of rows that will be held in memory for join's intermediate results,
			"ddl-timeout":            The execution timeout(in millisecond) for DDL statements,
			"query-timeout":          The execution timeout(in millisecond) for DML statements,
			"twopc-enable":           Enables(true or false) radon two phase commit, for distrubuted transaction,
			"allowip":                ["allow-ip-1", "allow-ip-2", "allow-ip-regexp"],
			"audit-mode":             The audit log mode, "N": disabled, "R": read enabled, "W": write enabled, "A": read/write enabled,
			"blocks-readonly":        The size of a block when create hash tables,
			"load-balance":           Enables(0 or 1) load balance, for read-write separation,
			"lower-case-table-names": If set false, table names are stored as specified and comparisons are case-sensitive, else not case-sensitive.
         }
         
```

`allowip:`
```
The specified ip and regexp ip are both supported. We use the regexp to define specified LAN IP segment(10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
case 1: wildcard "*", means all ip are allowed to connect to radon
case 2: we want to specify LAN IP segment(e.g. 10.0.0.0/8) start with `10.12`, the regexp ip will be `10.12.*` or `10.12.[0-9]+.[0-9]+`
case 3: we want to specify LAN IP segment(e.g. 192.168.0.0/16) like  `192.168.%.3`, the regexp ip will be `192.168.[0-9]+.3`
case 4: also you can just list the ip you want, like: "192.168.1.1", "192.168.1.2", "192.168.1.3" ... 
```

`Status:`
```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```
`Example: `
```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "max-join-rows":32768, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":true, "load-balance" 1, "allowip": ["192.168.1.1", "192.168.1.2", "172.10.[0-9]+.[0-9]+"]}' http://127.0.0.1:8080/v1/radon/config

---Response---
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 16:19:44 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### readonly

```
Path:    /v1/radon/readonly
Method:  PUT
Request: {
			"readonly": The value of the read-only(true) or not(false),	                                [required]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"readonly":true}' http://127.0.0.1:8080/v1/radon/readonly
		
---Response---
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 16:28:40 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### throttle

```
Path:    /v1/radon/throttle
Method:  PUT
Request: {
			"limits": The max number of requests in a second, defaults 0, means no limits,  [required]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example:`

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"limits":4800}' http://127.0.0.1:8080/v1/radon/throttle

---Response---
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 16:32:43 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### status

```
Path:    /v1/radon/status
Method:  GET
Response:{
			readonly:true/false
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/radon/status

---Response---
{"readonly":true}
```

## shard

### shardz

This api used to get all shard tables from router.

```
Path:    /v1/shard/shardz
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down.
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/shard/shardz

---Response---
{"Schemas":[{"DB":"db_test1","Tables":[{"Name":"t2","ShardKey":"id","Partition":{"Segments":[{"Table":"t2_0000","Backend":"backend1","Range":{"Start":0,"End":128}},{"Table":"t2_0001","Backend":"backend1","Range":{"Start":128,"End":256}},{"Table":"t2_0002","Backend":"backend1","Range":{"Start":256,"End":384}},

......

{"Start":3584,"End":3712}},{"Table":"t1_0029","Backend":"backend1","Range":{"Start":3712,"End":3840}},{"Table":"t1_0030","Backend":"backend1","Range":{"Start":3840,"End":3968}},{"Table":"t1_0031","Backend":"backend1","Range":{"Start":3968,"End":4096}}]}}]}]}
```

### globals

This api used to get all global tables from router.

```
Path:    /v1/shard/globals
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down.
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/shard/globals

---Response---
{"schemas":[{"database":"db1","tables":["tb2","tb5"]},{"database":"zzq","tables":["tb2"]}]}
```

### balanceadvice

This api used to get the best table(only one) which should be transferred from the max-backend to min-backend.

```
Path:    /v1/shard/balanceadvice
Method:  GET

Response: [{
			"from-address":     The from end address(host:port).
			"from-datasize":    The from end data size in MB.
			"from-user":        The from backend MySQL user.
			"from-password":    The from backend MySQL password.
			"to-address":       The to end address(host:port).
			"to-datasize":      The to end data size in MB.
			"from-user":        The to backend MySQL user.
			"from-password":    The to backend MySQL password.
			"database":         The transfered table database.
			"table":            The transfered table name.
			"tablesize":        The transfered table size.
         }]

```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, radon has no advice.

Notes:
If response is NULL, means there is no advice.
```

`Example:`

```
$ curl http://127.0.0.1:8080/v1/shard/balanceadvice

---Response---
null
```


### shift

This api used to change the partition backend from one to another.

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

`Status:1`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, radon has no advice.
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"database": "db_test1", "table": "t1", "from-address": "127.0.0.1:3306", "to-address": "127.0.0.1:3306"} http://127.0.0.1:8080/v1/shard/shift
```


### reload

This api used to re-load the router info from metadir.

```
Path:    /v1/shard/reload
Method:  POST
Request: NIL
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, radon has no advice.
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X POST http://127.0.0.1:8080/v1/shard/reload

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 02:07:15 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```


### migrate

This api is used to migrate the data from one backend to another.

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

`Status:`

```
	200: StatusOK
	204: StatusNoContent
	500: StatusInternalServerError
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"from": "127.0.0.1:3000","from-user":"usr","from-password":"123456","from-table":"t1","from-database":"test", "to":"127.0.0.1:4000","to-user":"usr","to-password":"123456","to-database":"test","to-table":"t1","cleanup":true}' http://127.0.0.1:8080/v1/shard/migrate

---Response---
HTTP/1.1 200 OK
Date: Fri, 10 Jan 2020 10:56:31 GMT
Content-Length: 0
```

## backend

### health

This api can perform a backend health check by sending the PING(select 1) command to backends.

```
Path:    /v1/radon/ping
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down.
```

`Example:`

```
$ curl http://127.0.0.1:8080/v1/radon/ping
```

## backends

This api used to add/delete a backend config.

### add

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

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend1", "address": "127.0.0.1:3306", "replica-address": "127.0.0.1:3306", "user": "root", "password": "318831", "max-connections":1024}' http://127.0.0.1:8080/v1/radon/backend

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 06:13:59 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### remove

```
Path:    /v1/radon/backend/{backend-name}
Method:  DELETE
```
`Status:`
```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```
`Example: `
```
$ curl -X DELETE http://127.0.0.1:8080/v1/radon/backend/backend1
```

## meta

The API used to do multi-proxy meta synchronization.

### versions

```
Path:    /v1/meta/versions
Method:  GET
Response:{
			Ts int64 `json:"version"`
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/meta/versions

---Response---
{"version":1523328058632112022}
```


### versioncheck

```
Path:    /v1/meta/versioncheck
Method:  GET
Response:{
			"latest":true,
			"peers":["127.0.0.1:8080"]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`Example:`

```
$ curl http://127.0.0.1:8080/v1/meta/versioncheck

---Response---
{"latest":true,"peers":["127.0.0.1:8080"]}
```

### metas

```
Path:    /v1/meta/metas
Method:  GET
Response:{
			Metas map[string]string `json:"metas"`
         }

```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/meta/metas

---Response---
{"metas":{"backend.json":"{\n\t\"backends\": null\n}","db_test1/t1.json":"{\n\t\"name\": \"t1\",\n\t\"shardtype\": \"HASH\",\n\t\"shardkey\": \"id\",\n\t\"partitions\": [\n\t\t{\n
.....
.....
t\t{\n\t\t\t\"table\": \"t2_0029\",\n\t\t\t\"segment\": \"3712-3840\",\n\t\t\t\"backend\": \"backend1\"\n\t\t},\n\t\t{\n\t\t\t\
```

## debug

### processlist
This api shows which threads are running.

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

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

`Example:`

```
$ curl http://127.0.0.1:8080/v1/debug/processlist
---Response---
[{"id":1,"user":"root","host":"127.0.0.1:40742","db":"","command":"Sleep","time":41263,"state":"","info":""}]
```

### txnz
This api shows which transactions are running.

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

`Example: `

```
$ curl http://127.0.0.1:8080/v1/debug/txnz/10
---Response(now backend does nothing, return null)--
null
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

### queryz
This api shows which queries are running.

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

`Example:`

``` 
$ curl http://127.0.0.1:8080/v1/debug/queryz/10
---Response---
null
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```

### configz
This api shows the config of RadonDB.

```
Path:    /v1/debug/configz
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example:`

```
$ curl http://127.0.0.1:8080/v1/debug/configz

---Response---
{"proxy":{"allowip":["127.0.0.1","127.0.0.2"],"meta-dir":"bin/radon-meta","endpoint":":3306","twopc-enable":true,"max-connections":1024,"max-result-size":1073741824,"ddl-timeout":3600,"query-timeout":600,"peer-address":"127.0.0.1:8080"},"audit":{"mode":"N","audit-dir":"bin/radon-audit","max-size":268435456,"expire-hours":1},"router":{"slots-readonly":4096,"blocks-readonly":128},"binlog":{"binlog-dir":"bin/radon-binlog","max-size":134217728},"log":{"level":"INFO"}}
```

### backendz
This api shows all the backends of RadonDB.

```
Path:    /v1/debug/backendz
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/debug/backendz

---Response---
[]
```

### schemaz
This api shows all the schemas of RadonDB.

```
Path:    /v1/debug/schemaz
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl http://127.0.0.1:8080/v1/debug/schemaz

---Response---
{"db_test1":{"DB":"db_test1","Tables":{"t1":{"Name":"t1","ShardKey":"id","Partition":{"Segments":[{"Table":"t1_0000","Backend":"backend1","Range":{"Start":0,"End":128}},{"Table":"t1_0001","Backend":"backend1","Range":{"Start":128,"End":256}},{"Table":"t1_0002","Backend":"backend1","Range":{"Start":256,"End":384}},
....
....
:"backend1","Range":{"Start":3712,"End":3840}},{"Table":"t2_0030","Backend":"backend1","Range":{"Start":3840,"End":3968}},{"Table":"t2_0031","Backend":"backend1","Range":{"Start":3968,"End":4096}}]}}}}}
```

## peers

### add peer

This api used to add a peer.

```
Path:    /v1/peer/add
Method:  POST
Request: {
			"address":         "The REST address of this peer",													[required]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "127.0.0.1:8080"}' http://127.0.0.1:8080/v1/peer/add

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:17:30 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```


### peerz

This api used to show the peers of a group.

```
Path:    /v1/peer/peerz
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
```
`Example:`
```
$ curl http://127.0.0.1:8080/v1/peer/peerz

---Response---
["127.0.0.1:8080"]
```

### remove peer

This api used to removea peer.

```
Path:    /v1/peer/remove
Method:  POST
Request: {
		"address":  "The REST address of this peer",     [required]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example: `

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "127.0.0.1:8080"}' http://127.0.0.1:8080/v1/peer/remove

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:21:09 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```




## users

The normal users that can connect to radon with password.

### create user

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

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
	503: StatusServiceUnavailable, backend(s) MySQL seems to be down
```

`Example: `

"databases" is array about database, if it is empty, we will set it to * .
"privilege" is composed of [select | insert | update | delete], separated by ",". If it is empty, we will set it to all priv.

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


### update user password

```
Path:    /v1/user/update
Method:  POST
Request: {
			"user": "user name",	[required]
			"password": "password",	[required]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example:`

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"user": "test", "password": "test"}' http://127.0.0.1:8080/v1/user/update

---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:39:31 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### drop user

```
Path:    /v1/user/remove
Method:  POST
Request: {
			"user": "user name",	[required]
         }
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example:`

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"user": "test"}' http://127.0.0.1:8080/v1/user/remove
---Response---
HTTP/1.1 200 OK
Date: Tue, 10 Apr 2018 03:41:14 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

### get users
```
Path:    /v1/user/userz
Method:  GET
```

`Status:`

```
	200: StatusOK
	405: StatusMethodNotAllowed
	500: StatusInternalServerError
```

`Example:`

```
$ curl http://127.0.0.1:8080/v1/user/userz
---Response---
[{"User":"root","Host":"%"},{"User":"test","Host":"%"},{"User":"mysql.session","Host":"localhost"},{"User":"mysql.sys","Host":"localhost"},{"User":"root","Host":"localhost"},{"User":"test","Host":"localhost"}]%
```
