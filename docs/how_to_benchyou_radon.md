Contents
=================

* [how to benchyou radon](#how-to-benchyou-radon)
   * [Step1 Make build](#step1-make-build)
      * [1.1 Download source code](#11-download-source-code)
      * [1.2 Modify source code](#12-modify-source-code)
      * [1.3 Make](#13-make)
   * [Step2 Build radon cluster](#step2-build-radon-cluster)
   * [Step3  Create database: sbtest](#step3--create-database-sbtest)
   * [Step4 Use bencyou to create test tables](#step4-use-bencyou-to-create-test-tables)
   * [Step5  Let`s begin to make a benchmark](#step5--lets-begin-to-make-a-benchmark)
      * [5.1 Close distributed transaction and audit，testing with random-write](#51-close-distributed-transaction-and-audittesting-with-random-write)
      * [5.2 Close distributed transaction and  audit，testing with random-read](#52-close-distributed-transaction-and--audittesting-with-random-read)
      * [5.3 Close distributed transaction and  audit，testing with random-read-write](#53-close-distributed-transaction-and--audittesting-with-random-read-write)
      * [5.4  Open distributed transaction,  close  audit，testing with random-read-write](#54--open-distributed-transaction--close--audittesting-with-random-read-write)
      * [5.5  Open both distributed transaction and audit，testing with random-read-write](#55--open-both-distributed-transaction-and-audittesting-with-random-read-write)

# how to benchyou radon

## Step1 Make build

### 1.1 Download source code

```
$ git clone https://github.com/xelabs/benchyou
$ cd benchyou
$ ls
LICENSE   README.md bin       makefile  pkg       src
```

### 1.2 Modify source code

`Note:` `benchyou` is a tool used for `mysql` and `radon` performance benchmark, now we use it to make a benchmark on `radon`.  Since radon builds the table by specifying the partition by hash (key),  part of the benchyou code needs to be adjusted. The source code needs to adjust is  located in line 41 of the src/sysbench/table.go src file.

the src code needed to adjust is showed next:
```
) engine=%s`, i, engine)
```

Modify the code as shown below：

```
) engine=%s partition by hash(id)`, i, engine)
```

### 1.3 Make

```
$ make build
--> go get...
go get github.com/xelabs/go-mysqlstack/driver
--> Building...
go build -v -o bin/benchyou src/bench/benchyou.go
vendor/golang.org/x/crypto/ed25519/internal/edwards25519
vendor/golang.org/x/crypto/curve25519
xcommon
vendor/github.com/spf13/pflag
xworker
vendor/golang.org/x/crypto/ed25519
sysbench
vendor/golang.org/x/crypto/ssh
vendor/github.com/spf13/cobra
xstat
xcmd
```

## Step2 Build radon cluster

For detailed construction，see [radon_cluster_deploy.md](radon_cluster_deploy.md).

## Step3  Create database: sbtest

Benchyou default uses database `sbtest`  when we use it to make a benchmark, for this reason, we should login mysql-server via mysql-cli to create a database named `sbtest` after we build radon cluster.

```
mysql> CREATE DATABASE SBTEST;
```

## Step4 Use bencyou to create test tables

The command to create tables is follows
parameter description:

`--mysql-host=192.168.0.16` : master node of radon cluster

`--mysql-port` : As we make a benchmark on radon cluster，this parameter is the master node port of radon  cluster.

`--mysql-user=root`: account used to login mysql-server

`--mysql-password=123456 `: password used to login mysql-server

`-oltp-tables-count` ： quantity to create tables

`--mysql-table-engine=innod`: specify innodb engine

`--max-request` : The maximum number of requests, such as execute insert sql, means that 10 million rows is written

```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb   --max-request=1000000 prepare
```

```
2018/05/23 14:19:29 create table benchyou0(engine=innodb) finished...
2018/05/23 14:19:31 create table benchyou1(engine=innodb) finished...
2018/05/23 14:19:32 create table benchyou2(engine=innodb) finished...
...
...

2018/05/23 14:20:09 create table benchyou29(engine=innodb) finished...
2018/05/23 14:20:10 create table benchyou30(engine=innodb) finished...
2018/05/23 14:20:11 create table benchyou31(engine=innodb) finished...
```

## Step5  Let`s begin to make a benchmark

### 5.1 Close distributed transaction and audit，testing with random-write

Execute command on master node of radon cluster.
parameter description:

 `twopc-enable` : distributed transaction switch，set value  `false`

 `audit-mode`: audit switch, set value `N`:

 `allowip` : allows login radon(master node) from specified IP
 
```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":false, "allowip": ["192.168.0.28", "192.168.0.14", "192.168.0.15"], "audit-mode": "N"}' http://192.168.0.16:8080/v1/radon/config
```

Execute commands as follows:
`Note`: **The following test next only shows how to perform benchmark. Here only 64 concurrent threads are set. The specific number of threads can be set according to your own environment, such as set 512 or higher.**

```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=0 --max-request=10000000 random
```

Execution process (takes a little time and finally outputs the average):

```
time            thds              tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:0,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
....
....
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3602s]      1893     1893    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.01,min:0.00,max:1385.86]  [avg:NaN,min:0.00,max:0.00]      6818994
```

### 5.2 Close distributed transaction and  audit，testing with random-read


As `Step 5.1` has closed distributed transaction and audit, we execute commands directly here(set 128 read treads here, as said above, the specific number of threads can be set according to your own environment ):
 
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=0 --read-threads=128 --max-request=10000000 random
```

Execution process (takes a little time and finally outputs the average):
```
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:128,w:0,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
...
...
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                    r-rsp(ms)              total-number
[2934s]      3408     0       3408    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:NaN,min:0.00,max:0.00]  [avg:0.01,min:0.00,max:1813.36]      10000138
```

### 5.3 Close distributed transaction and  audit，testing with random-read-write

As `Step 5.1` has closed distributed transaction and audit, we execute commands directly here:

```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=64 --max-request=10000000 random
```

Execution process (takes a little time and finally outputs the average):
```	
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:64,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
....
....
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3604s]      1739     508     1230    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.03,min:0.00,max:6936.69]  [avg:0.01,min:0.00,max:6200.80]      6269848		
```


### 5.4  Open distributed transaction,  close  audit，testing with random-read-write

On master node of radon cluster，open distributed transaction switch : 

`twopc-enable` : set value`true`

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":true, "allowip": ["192.168.0.28", "192.168.0.14", "192.168.0.15"], "audit-mode": "N"}' http://192.168.0.16:8080/v1/radon/config
```

Execute benchyou command：
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=64 --max-request=10000000 random
``` 

Execution process (takes a little time and finally outputs the average):
```
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:64,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
...
...
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3526s]      2836     967     1869    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.02,min:0.00,max:3808.16]  [avg:0.01,min:0.00,max:3783.29]      10001004
```

### 5.5  Open both distributed transaction and audit，testing with random-read-write

On master node of radon: 

`twopc-enable` : set value `true`,

`audit-mode`: set value `A`

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":true, "allowip": ["192.168.0.28", "192.168.0.14", "192.168.0.15"], "audit-mode": "A"}' http://192.168.0.16:8080/v1/radon/config
```

Execute benchyou command：
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=64 --max-request=10000000 random
```

Execution process (takes a little time and finally outputs the average):
```
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:64,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
....
....
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3602s]      2691     920     1770    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.02,min:0.00,max:4281.83]  [avg:0.01,min:0.00,max:3794.18]      9693881
```


