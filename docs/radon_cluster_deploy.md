`radon` cluster deploy

--------------------------------------------------------------------------------------------------

Contents
=================

* [Radon cluster deploy](#radon-cluster-deploy)
   * [Step1 Environment preparation](#step1-environment-preparation)
   * [Step2 Startup radon](#step2-startup-radon)
      * [2.1 startup master node of radon(IP: 192.168.0.16):](#21-startup-master-node-of-radonip-192168016)
      * [2.2 startup slave node of radon](#22-startup-slave-node-of-radon)
      * [2.3 Check the meta data in bin/radon-meta directory](#23-check-the-meta-data-in-binradon-meta-directory)
   * [Step3 Execute command of add peer to build cluster of radon](#step3-execute-command-of-add-peer-to-build-cluster-of-radon)
      * [3.1 master node (IP: 192.168.0.16): add peer operation](#31-master-node-ip-192168016-add-peer-operation)
      * [3.2 slave node (IP: 192.168.0.17): add peer operation](#32-slave-node-ip-192168017-add-peer-operation)
      * [3.3 Check the meta data again in bin/radon-meta directory](#33-check-the-meta-data-again-in-binradon-meta-directory)
   * [Step4 add backend and backup nodes to master node](#step4-add-backend-and-backup-nodes-to-master-node)
      * [4.1 add backend1 node(IP: 192.168.0.14)](#41-add-backend1-nodeip-192168014)
      * [4.2 add backend2 node(IP: 192.168.0.28)](#42-add-backend2-nodeip-192168028)
      * [4.3 add backup node(IP: 192.168.0.15)](#43-add-backup-nodeip-192168015)
   * [Step5 Connect to master via mysql-cli](#step5-connect-to-master-via-mysql-cli)

# Radon cluster deploy

This part is about how to deploy `radon cluster`. By default,  we suppose you are already familiar with the startup and deployment of radon by `standalone mode`. If you are not familiar with it, please refer to it first according to the doc [how_to_build_and_run_radon](how_to_build_and_run_radon.md)。

## Step1 Environment preparation
Here we deploy radon cluster by two nodes (a master and a slave, of course you can add more slaves, we use only two nodes just to show how to deploy the cluster). And we need two backend nodes (mysql-server) to storage,  one backup node to compute. The mysql-server requires five hosts (or virtual machines). The architecture of deployment and the IP address of each node are as follows:

                       +----------------------------+     
                       |  SQL layer（radon cluster: | 
                       |  two nodes）               |  
                       +----------------------------+     
                       |  storage and compute layer:|
                       |  tow backend nodes and 1   |
                       |  backup node            |  
                       |----------------------------|  



`master node of radon` : 192.168.0.16

`slave node of radon`  : 192.168.0.17

`node of backend1` : 192.168.0.14

`node of backend2` : 192.168.0.28

`node of backup`     :   192.168.0.15

By default, we suppose the mysql account and password of mysql-server are all the same between each machine(e.g. account: `mysql`, password: `123455`). Of course,  mysql-server is deployed on backend1、backend2 and backup. Confirm that each mysql-server has granted all privileges to login from another machine, if not, please login mysql-server and execute the following command on each machine: 

```
mysql> GRANT ALL PRIVILEGES ON *.* TO mysql@"%" IDENTIFIED BY '123456'  WITH GRANT OPTION;
```

## Step2 Startup radon

### 2.1 startup master node of radon(IP: 192.168.0.16):

Enter radon/bin director and execute the following command:

```
$ ./radon -c radon.default.json > radon.log 2>&1 &
```

After executing the command, a new directory named `bin` will be generated. It contains metadata information.  In addition, the `radon.log` is used to record the info of radon's execution. If you want to stop radon,  execute linux command`lsof`, find the pid corresponding with radon and then kill it.

`e.g.`
```
$ lsof -i :3308
COMMAND   PID   USER   FD   TYPE   DEVICE SIZE/OFF NODE NAME
radon   35572 ubuntu    7u  IPv6 11618866      0t0  TCP *:3308 (LISTEN)
$ kill 35572
```

### 2.2 startup slave node of radon

The way to startup slave node is the same with startup master.


### 2.3 Check the meta data in bin/radon-meta directory

After startup master node and slave node,  execute command `ls bin/radon-meta` and you will see a file named `backend.json`. The backend information is empty at this time. The two nodes are currently independent nodes and need to execute instructions to form an associated cluster. See Step 3.

```
$ ls bin/radon-meta/
backend.json
```


## Step3 Execute command of `add peer` to build cluster of radon

### 3.1 master node (IP: 192.168.0.16): add peer operation

add  master node self (If add success, you will see status of `OK` )
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.16:8080/v1/peer/add
```

add slave node
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.16:8080/v1/peer/add
```

### 3.2 slave node (IP: 192.168.0.17): add peer operation

add master node
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.17:8080/v1/peer/add
```

add slave self
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.17:8080/v1/peer/add
```

### 3.3 Check the meta data again in bin/radon-meta directory

After `add peer` operation, execute command `ls` to see the json file in the bin/radaon-meta directory.  You will see three files: backend.json、peers.json、version.json. The information of storage nodes  and computation node are stored in peers.json. Version.json records the version information of this node, which is used to determine whether the nodes are needing to synchronize or not.

```
$ ls bin/radon-meta/
backend.json  peers.json  version.json
```

## Step4 add backend and backup nodes to master node

Switch to master node (`IP: 192.168.0.16`) and execute commands as follows:

### 4.1 add backend1 node(IP: 192.168.0.14)

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend2", "address": "192.168.0.14:3306", "user":"mysql", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backend
```

### 4.2 add backend2 node(IP: 192.168.0.28)

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend1", "address": "192.168.0.28:3306", "user":"mysql", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backend
```

### 4.3 add backup node(IP: 192.168.0.15)

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backupnode", "address": "192.168.0.15:3306", "user":"mysql", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backup
```

From now on，radon cluster has being build. Use vim to view the backend.json file in the bin/radon-meta directory of the master node. You will see that the background node information has been added.

```
$ vim bin/radon-meta/backend.json 
```

```
{
        "backup": {
                "name": "backupnode",
                "address": "192.168.0.15:3306",
                "user": "mysql",
                "password": "123456",
                "database": "",
                "charset": "utf8",
                "max-connections": 1024
        },
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
Switch to slave node and do the same action, you will see that although the slave node does not perform a backend or backup operation, the data is synchronized with the master node.

```
$ vim bin/radon-meta/backend.json 
```

```
{
        "backup": {
                "name": "backupnode",
                "address": "192.168.0.15:3306",
                "user": "mysql",
                "password": "123456",
                "database": "",
                "charset": "utf8",
                "max-connections": 1024
        },
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

## Step5 Connect to master via mysql-cli

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

Execute a sql:
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
