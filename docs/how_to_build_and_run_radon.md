`RadonDB` is fairly simple to deploy, without installing external dependencies.

--------------------------------------------------------------------------------------------------
Contents
=================

* [How to build and run radon](#how-to-build-and-run-radon)
   * [Requirements](#requirements)
   * [Step1. Download src code from github](#step1-download-src-code-from-github)
   * [Step2. Build](#step2-build)
   * [Step3. Run radon](#step3-run-radon)
   * [Step4. Add a backend(mysql server) to radon](#step4-add-a-backendmysql-server-to-radon)
   * [Step5. Connect mysql client to radon](#step5-connect-mysql-client-to-radon)

# How to build and run radon

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

## Step3. Run radon
Copy the default configure file conf/radon.default.json into bin first:
```
$ cp conf/radon.default.json bin/
```
 
Then run `radon` server:
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
When radon started, it will use three ports:
`3308: External service port for MySQL client link`
`8080: Management port, external RESTFUL interface`
`6060: debug port, golang debug port`

## Step4. Add a backend(mysql server) to radon
This is an admin instruction of radon api, for more admin instructions, see  [radon admin API](api.md).
Here we suppose  mysql has being installed and the mysql service has beeing started on your machine, the user and password logged in to mysql are all root.
`user`: the user to login mysql
`password`: the password to login mysql
```
$ curl -i -H 'Content-Type: application/json' -X POST -d \
> '{"name": "backend1", "address": "127.0.0.1:3306", "user":\
>  "root", "password": "root", "max-connections":1024}' \
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
Radon supports client connections to the MySQL protocol, like: mysql -uroot -h127.0.0.1 -P3308
`root`:account login to radon, we provide default account 'root' with no password to login
`3308`:radon default port
```
$ mysql -uroot -h127.0.0.1 -P3308
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
Now you can send sql from mysql client, for more sql supported by radon sql protocol, see *  [Radon_SQL_support](radon_sql_support.md)
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
