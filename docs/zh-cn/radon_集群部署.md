Table of Contents
=================

   * [Radon 集群部署](#radon-集群部署)
      * [第一步 环境准备](#第一步-环境准备)
      * [第二步 启动radon](#第二步-启动radon)
         * [2.1 启动radon主节点(IP: 192.168.0.16)](#21-启动radon主节点ip-192168016)
         * [2.2 启动radon从节点](#22-启动radon从节点)
         * [2.3 检查bin/radon-meta目录下的元数据](#23-检查binradon-meta目录下的元数据)
      * [第三步 执行add peer指令来部署radon集群](#第三步-执行add-peer指令来部署radon集群)
         * [3.1 主节点(IP: 192.168.0.16): 执行add peer操作](#31-主节点ip-192168016-执行add-peer操作)
         * [3.2 从节点(IP: 192.168.0.17): 执行add peer操作](#32-从节点ip-192168017-执行add-peer操作)
         * [3.3 再次检查bin/radon-meta目录下的元数据](#33-再次检查binradon-meta目录下的元数据)
      * [第四步 添加后端节点到radon主节点](#第四步-添加后端节点到radon主节点)
         * [4.1 添加后端backend1节点(IP: 192.168.0.14)](#41-添加后端backend1节点ip-192168014)
         * [4.2 添加后端backend2节点(IP: 192.168.0.28)](#42-添加后端backend2节点ip-192168028)
      * [第五步  使用MySQL client连接到radon主节点](#第五步--使用mysql-client连接到radon主节点)

# Radon 集群部署

这部分是介绍如何部署radon集群，我们假设您已经熟悉通过`standalone模式`启动和部署radon。如果您不熟悉它，请先根据文档[如何编译运行radon](如何编译运行radon.md)进行参考。

## 第一步 环境准备
这里，我们通过两个节点（一个主节点和一个从节点，当然可以添加更多的从节点，仅使用两个节点来展示如何部署群集）来部署radon群集。而且我们需要两个后端节点（mysql-server）进行存储。mysql服务器需要4个主机（或虚拟机）。部署架构和每个节点的IP地址如下：

                            +-----------------------------------+
                            |SQL层(radon集群):两个节点            |
                            +-----------------------------------+
                            |存储和计算层:两个后端节点              |
                            +-----------------------------------+

`radon主节点`: 192.168.0.16

`radon从节点`: 192.168.0.17

`后端节点backend1`: 192.168.0.14

`后端节点backend2`: 192.168.0.28

默认情况下，我们假设每台机器之间的mysql帐户和的密码都相同（例如，帐户：`mysql`，密码：`123456`）。当然，mysql-server分别部署在backend1，backend2上。确认每个mysql服务器都授予从其它机器登录的权限，如果没有，请登录mysql服务器并在每台机器上执行以下命令：

```
mysql> GRANT ALL PRIVILEGES ON *.* TO mysql@"%" IDENTIFIED BY '123456'  WITH GRANT OPTION;
```

## 第二步 启动radon

### 2.1 启动radon主节点(IP: 192.168.0.16)

进入radon/bin目录并且执行一下命令：

```
$ ./radon -c radon.default.json > radon.log 2>&1 &
```

执行命令后，将生成一个名为`bin`的新目录，它包含元数据信息。另外，`radon.log`用于记录radon执行的日志。如果要停止radon，请执行linux命令`lsof`，找到与radon相对应的pid，然后将其杀死。

`示例：`
```
$ lsof -i :3308
COMMAND   PID   USER   FD   TYPE   DEVICE SIZE/OFF NODE NAME
radon   35572 ubuntu    7u  IPv6 11618866      0t0  TCP *:3308 (LISTEN)
$ kill 35572
```

### 2.2 启动radon从节点

启动的方式和主节点类似，参考上一步主节点的启动方式。


### 2.3 检查`bin/radon-meta`目录下的元数据

启动主节点和从节点之后，执行命令`ls bin / radon-meta`，您将看到一个名为`backend.json`的文件。后端信息此时为空。当前这两个节点是独立的节点，需要执行指令以形成关联的集群。请参阅步骤3。

```
$ ls bin/radon-meta/
backend.json
```


## 第三步 执行`add peer`指令来部署radon集群

### 3.1 主节点(IP: 192.168.0.16): 执行`add peer`操作

加入主节点（如果成功，你会看到返回状态：`OK`）：
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.16:8080/v1/peer/add
```

加入从节点：
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.16:8080/v1/peer/add
```

### 3.2 从节点(IP: 192.168.0.17): 执行`add peer`操作

加入主节点：
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.17:8080/v1/peer/add
```

加入从节点自身
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.17:8080/v1/peer/add
```

### 3.3 再次检查`bin/radon-meta`目录下的元数据

在`add peer`操作之后，执行命令`ls`以查看`bin/radaon-meta`目录中的json文件。您将看到三个文件：backend.json，peers.json，version.json。存储节点和计算节点的信息存储在backend.json主中。Version.json记录此节点的版本信息，该信息用于确定节点是否需要同步。

```
$ ls bin/radon-meta/
backend.json  peers.json  version.json
```

## 第四步 添加后端节点到radon主节点

切换到radon主节点(`IP: 192.168.0.16`)并且执行以下指令：

### 4.1 添加后端backend1节点(IP: 192.168.0.14)

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend2", "address": "192.168.0.14:3306", "user":"mysql", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backend
```

### 4.2 添加后端backend2节点(IP: 192.168.0.28)

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend1", "address": "192.168.0.28:3306", "user":"mysql", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backend
```

现在，radon集群已经部署完成。使用vim在主节点的`bin/radon-meta`目录中查看backend.json文件。您将看到已添加了后端节点信息。
```
$ vim bin/radon-meta/backend.json 
```

```
{
        "backends": [
                {
                        "name": "backend2",
                        "address": "192.168.0.14:3306",
                        "user": "mysql",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                },
                {
                        "name": "backend1",
                        "address": "192.168.0.28:3306",
                        "user": "mysql",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                }
        ]
}

```

切换到从节点并执行相同的操作，您将看到，尽管从节点不执行后端或备份操作，但元数据与主节点同步。

```
$ vim bin/radon-meta/backend.json 
```

```
{
        "backends": [
                {
                        "name": "backend2",
                        "address": "192.168.0.14:3306",
                        "user": "mysql",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                },
                {
                        "name": "backend1",
                        "address": "192.168.0.28:3306",
                        "user": "mysql",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                }
        ]
}
```

## 第五步  使用MySQL client连接到radon主节点

```
$ mysql -h192.168.0.16 -umysql -p123456 -P3308

mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1038
Server version: 5.7-Radon-1.0 XeLabs TokuDB build 20180118.100653.39b1969

Copyright (c) 2009-2017 Percona LLC and/or its affiliates
Copyright (c) 2000, 2017, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

执行一条sql:
```
mysql> show databases;
+---------------------------+
| Database                  |
+---------------------------+
| information_schema        |
| mysql                     |
| performance_schema        |
| sbtest                    |
| sys                       |
+---------------------------+
5 rows in set (0.13 sec)

mysql>
```
