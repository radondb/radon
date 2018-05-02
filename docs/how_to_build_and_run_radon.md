`RadonDB` is fairly simple to deploy, without installing external dependencies.

--------------------------------------------------------------------------------------------------
[TOC]

# How to build and run randon

## Requirements
1. [Go](http://golang.org) version 1.8 or newer is required.("sudo apt install golang" for ubuntu or "yum install golang" for centOS/redhat)
2. A 64-bit system is strongly recommended. Building or running radon on 32-bit systems has not been tested

## Step1. Download src code from github
```
$ git clone https://github.com/radondb/radon
```

## Step2. Build
After download radon src code from github, it will generate a directory named "radon", execute the following commands:
```
$ cd radon
$ make build
```
The binary executable file is in the "bin" directory, execute  command "ls bin/":
```
$ ls bin/

---Response---
$ radon radoncli
```

## Step3. Run
Copy the default configure file conf/radon.default.json into bin first:
```
$ cp conf/radon.default.json bin/
```
 
Then Run RadonDB server:
```
$ bin/radon -c bin/radon.default.json
``` 
If start successfully, you will see infos next:
```
    radon:[{Tag:rc-20180126-16-gf448be1 Time:2018/04/04 03:31:39 Git:f448be1
    GoVersion:go1.8.3 Platform:linux amd64}]
    2018/04/04 15:20:17.136839 proxy.go:79:
     ....
     .... 
    2018/04/04 15:20:17.151499 admin.go:54:      [INFO]     http.server.start[:8080]...
```
When randon started, it will use three ports:
`3306: External service port for MySQL client link`
`8080: Management port, external RESTFUL interface`
`6060: debug port, golang debug port`

## Step4. Add a backend(mysql server) to randon
This is an admin instruction of randon api, for more admin instructions, see  [randon admin API](API.md) ).
Here we suppose  mysql has being installed and mysql service has beeing started on your machine and the user and password logined to mysql are all root.
`user: the user to login mysql`
`password: the password to login mysql`
```
$ curl -i -H 'Content-Type: application/json' -X POST -d \
> '{"name": "backend1", "address": "127.0.0.1:3306", "user":\
>  "root", "password": "318831", "max-connections":1024}' \
> http://127.0.0.1:8080/v1/radon/backend
```
`Response: `
```
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 03:23:02 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```
## Step5. Connect mysql client to radon
Radon supports client connections to the MySQL protocol
```
$ mysql -uroot -h127.0.0.1 -P3306
```
If connected success, you will see:
```
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.7-Radon-1.0

Copyright (c) 2000, 2018, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```
Now you can send sql from mysql client, for more sql surpported by randon sql protocol, see *  [Radon_SQL_surported](Radon_SQL_surported.md)
`Example: `
```
mysql> SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| db_gry_test        |
| db_test1           |
| mysql              |
| performance_schema |
| sys                |
+--------------------+
6 rows in set (0.01 sec)
```
