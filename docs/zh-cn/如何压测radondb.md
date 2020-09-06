## 压测工具

```
$git clone https://github.com/XeLabs/benchyou
$cd benchyou
$make build
$./bin/benchyou -h
```

## 压测说明

* 随机写测试
* 随机读测试
* 随机读写混合测试
* 开启审计日志测试

## 环境
* radon部署在独享主机: 192.168.0.24
* 32个表, 每个表拆分成32个小表, 总共32*32表
* 4个后端分区(一主2从 MySQL Plus集群), 引擎为: innodb
* 后端MySQL配置为最严格模式,不会丢失数据和事务


## 建表

32个sysbench表:

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb   --max-request=1000000 prepare
```

## 随机写

* 512线程写入1000w条数据

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=512 --read-threads=0 --max-request=10000000 random

... ...

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[570s]       [r:0,w:512]  17353    17353   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         10.26      NaN          9981619

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[571s]       [r:0,w:512]  17397    17397   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         11.38      NaN          9999016

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[571s]       17514    17514   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.02,min:0.00,max:1032.05]  [avg:NaN,min:0.00,max:0.00]      10000613
```

## 随机读

* 512线程随机读取1000w条记录

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=0 --read-threads=512 --max-request=10000000 random
... ...

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[161s]       [r:512,w:0]  66088    0       66088   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         NaN        7.73         9872001

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[162s]       [r:512,w:0]  66187    0       66187   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         NaN        7.73         9938188

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                    r-rsp(ms)              total-number
[162s]       61734    0       61734   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:NaN,min:0.00,max:0.00]  [avg:0.05,min:0.00,max:189.10]      10001006
```

* 1024线程随机读取1000w条记录

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=0 --read-threads=1024 --max-request=10000000 random

... ...

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[155s]       [r:1024,w:0]  65962    0       65962   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         NaN        15.47        9900393

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[156s]       [r:1024,w:0]  68140    0       68140   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         NaN        15.02        9968533

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                    r-rsp(ms)              total-number
[156s]       64111    0       64111   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:NaN,min:0.00,max:0.00]  [avg:0.10,min:0.00,max:888.40]      10001413
```

## 读写(4:1)混合压测

* 64个写线程, 256个读线程混合, 100w次随机读写操作

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=256 --max-request=10000000 random

... ...

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[19s]        [r:256,w:64]  46633    5443    41190   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         11.26      6.18         934800

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[20s]        [r:256,w:64]  51542    5563    45979   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         10.94      5.56         986342

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                       r-rsp(ms)              total-number
[20s]        50075    5575    44500   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.52,min:0.00,max:112.90]  [avg:0.27,min:0.00,max:99.22]      1001519
```


* 开启分布式事务压测

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=256 --max-request=1000000 random

... ...

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[21s]        [r:256,w:64]  48514    2597    45917   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         24.54      5.57         925522

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[22s]        [r:256,w:64]  48833    2664    46169   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         23.99      5.54         974355

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                       r-rsp(ms)              total-number
[22s]        45563    2378    43185   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:1.15,min:0.00,max:110.99]  [avg:0.25,min:0.00,max:52.14]      1002403
```

## 审计日志性能评测

* `关闭审计日志`: 64个写线程, 256个读线程混合, 100w次随机读写操作

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=256 --max-request=1000000 random

... ...

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[16s]        [r:256,w:64]  56038    6700    49338   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         9.29       5.19         899295

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[17s]        [r:256,w:64]  69976    7389    62587   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         8.43       4.09         969271

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                       r-rsp(ms)              total-number
[17s]        58896    6518    52377   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.53,min:0.00,max:121.42]  [avg:0.27,min:0.00,max:120.68]      1001237
```

* `开启审计日志`: 64个写线程, 256个读线程混合, 100w次随机读写操作

```
./bin/benchyou  --mysql-host=192.168.0.24 --mysql-port=3306 --mysql-user=mock --mysql-password=mock  --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=256 --max-request=1000000 random

... ...

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[17s]        [r:256,w:64]  52864    6301    46563   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         9.79       5.47         944336

time            thds        tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[18s]        [r:256,w:64]  53764    6270    47494   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         9.72       5.35         998100

---------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                      r-rsp(ms)              total-number
[18s]        55617    6280    49336   0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.51,min:0.00,max:58.91]  [avg:0.27,min:0.00,max:39.45]      1001107
```