Table of Contents
=================

   * [如何构建和运行radon](#如何构建和运行radon)
      * [环境要求](#环境要求)
      * [第一步 从github下载源码](#第一步-从github下载源码)
      * [第二步 构建](#第二步-构建)
      * [第三步 运行radon](#第三步-运行radon)
      * [第四步. 增加一个后端backend(mysql server)到radon](#第四步-增加一个后端backendmysql-server到radon)
      * [第五步 使用MySQL客户端连接到radon](#第五步-使用mysql客户端连接到radon)

`RadonDB` 很容易部署，不用安装额外的依赖包

# 如何构建和运行radon

## 环境要求
1. [Go](http://golang.org) 版本1.8 或者更新.(ubuntu下使用"sudo apt install golang" ,CentOS/Redhat使用"yum install golang")
2. 强烈推荐64位系统,32位系统没有测试验证过.

## 第一步 从github下载源码
```
$ git clone https://github.com/radondb/radon
```

## 第二步 构建 
After download radon src code from github, it will generate a directory named "radon", execute the following commands:
```
$ cd radon
$ make build
```
可执行文件在"bin"目录下,执行命令"ls bin/"查看
```
$ ls bin/

---Response---
$ radon radoncli
```

##  第三步 运行radon
拷贝默认配置文件 "conf/radon.default.json" 到"bin"目录下:
```
$ cp conf/radon.default.json bin/
```
 
运行`radon`:
```
$ bin/radon -c bin/radon.default.json
``` 
如果启动成功，你将会看到如下说信息:
```
    radon:[{Tag:rc-20180126-16-gf448be1 Time:2018/04/04 03:31:39 Git:f448be1
    GoVersion:go1.8.3 Platform:linux amd64}]
    2018/04/04 15:20:17.136839 proxy.go:79:
     ....
     .... 
    2018/04/04 15:20:17.151499 admin.go:54:      [INFO]     http.server.start[:8080]...
```
当radon启动时，会使用如下3个端口
`3308: 用于MySQL客户端连接`
`8080: 管理端口, 额外的RESTFUL接口`
`6060: 调试端口, golang 调试端口`

## 第四步. 增加一个后端backend(mysql server)到radon
这是radon api中的一条管理指令，更多的管理指令使用说明，参见[管理指令](管理指令.md)

首先，创建一个MySQL server的账户，然后使用该账户将MySQL server作为一个后端挂载到radon，Radon使用这个账户访问挂载之后到后端。

这里我们假设MySQL已经在你的机器上安装并且MySQL服务已经启动，默认登入到用户名和密码都是root。

`user`: 登入到MySQL的用户
`password`: 登入到MySQL的密码
```
$ curl -i -H 'Content-Type: application/json' -X POST -d \
> '{"name": "backend1", "address": "127.0.0.1:3306", "user":\
>  "root", "password": "root", "max-connections":1024}' \
> http://127.0.0.1:8080/v1/radon/backend
```
`返回: `
```
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 03:23:02 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

The backends information is recorded in the JSON file `$meta-dir\backend.json`. 
后端信息记录在JSON文件`$meta-dir\backend.json`。
```
{
        "backends": [
                {
                        "name": "backend1",
                        "address": "127.0.0.1:3306",
                        "user": "root",
                        "password": "root",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                }
        ]
}
```

## 第五步 使用MySQL客户端连接到radon
Radon支持MySQL客户端连接协议，可以像连接到MySQL一样连接到Radon，例如：mysql -uroot -h127.0.0.1 -P3308
`root`:登入到Radon的账户，我们默认提供`root`账户，密码为空
`3308`: Radon默认端口
```
$ mysql -uroot -h127.0.0.1 -P3308
```
如果连接成功，你将会看到：
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
Now you can send sql from mysql client, for more sql supported by radon sql protocol, see [radon_sql_statements_manual](radon_sql_statements_manual.md)
现在你可以从客户端发送sql，更多的sql支持，参见[radon_sql_语句手册](radon_sql_语句手册.md)
`示例: `
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
