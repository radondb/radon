# shift 性能测试

## 测试目的

测试shift迁移工具是否满足Radon集群在AppCenter线上数据迁移性能需求。

## 集群配置

- radon 16核64GB   120GB * 1
- xenon  16核64GB   100GB   *  4

## 测试准备

###  benchyou 加载数据

- 创建4个innodb 存储引擎表

```
../benchyou  --mysql-host="192.168.0.21" --mysql-user="radon" --mysql-password="radon"  --oltp-tables-count=4 --mysql-table-engine=innodb prepare
```

-  加载数据

```
../benchyou  --mysql-host="192.168.0.21" --mysql-user="radon" --mysql-password="radon" --ssh-user=radon --ssh-password="zhu1241jie" --oltp-tables-count=4 --write-threads=128 --read-threads=0 --max-time=3600 --mysql-enable-xa=0 random
```

```
time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[715s]       [r:0,w:128]  15430    15430   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         7.17       NaN          11410673

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[716s]       [r:0,w:128]  15739    15739   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         6.94       NaN          11426412

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[717s]       [r:0,w:128]  15761    15761   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         6.98       NaN          11442173

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[718s]       [r:0,w:128]  14672    14672   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         7.59       NaN          11456845

```

- 数据加载说明：

通过多次使用benchyou做数据加载

- 1个小时benchyou日志：

```
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3602s]      11280    11280   0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.00,min:0.00,max:1056.83]  [avg:NaN,min:0.00,max:0.00]      40633480

```

- 4个小时benchyou日志：

```
time           tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[14401s]      8642     8642    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.00,min:0.00,max:1625.19]  [avg:NaN,min:0.00,max:0.00]      124456855

```
- 4个小时benchyou日志：

```
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time           tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[14401s]      7350     7350    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.00,min:0.00,max:3039.51]  [avg:NaN,min:0.00,max:0.00]      105860194
```

- radon 节点（数据加载I/O和网络使用情况）

```
avg-cpu:  %user   %nice %system %iowait  %steal   %idle
          21.71    0.00   10.02    0.19    1.05   67.04

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     2.00    0.00    2.00     0.00     0.02    16.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdd               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
          22.93    0.00   12.22    0.25    1.25   63.35

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdd               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
          26.41    0.00   14.12    0.13    1.24   58.10
```

```
Average:        IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s   %ifutil
Average:           lo  26073.61  26073.61   4994.69   4994.69      0.00      0.00      0.00      0.00
Average:         eth0  13124.54  16025.21   1085.80   4678.61      0.00      0.00      0.00      0.00
```

- xenon节点资源（数据加载I/O和网络使用情况，xenon节点tps）

```
Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    52.00    0.00 3373.00     0.00    76.57    46.49     1.72    0.51    0.00    0.51   0.25  83.60

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           5.15    0.00    3.18    2.42    0.32   88.93

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    59.00    0.00 3438.00     0.00    77.44    46.13     1.69    0.50    0.00    0.50   0.25  86.40

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           4.90    0.00    2.93    2.93    0.25   88.99

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    62.00    0.00 3482.00     0.00    77.23    45.42     1.71    0.49    0.00    0.49   0.26  91.20

```

```
Average:        IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s   %ifutil
Average:           lo      6.71      6.71      3.14      3.14      0.00      0.00      0.00      0.00
Average:         eth0   6644.52   5405.48   1462.47   3841.58      0.00      0.00      0.00      0.00
```

```
mysql> show engine innodb status\G

2834.58 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
2739.84 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
2805.52 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
2605.30 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
```

## 147万条元组全量迁移测试

### 测试执行

- 在radon服务器执行shift命令

```
nohup ./shift --from="192.168.0.251:3306" --from-user="radon" --from-password="radon" --from-database="sbtest" --from-table="benchyou1_0008" --to="192.168.0.252:3306"  --to-password="radon" --to-table="benchyou1_0008" --to-user="radon" --to-database="sbtest" --threads=64 > shift_test.log 2>&1 &
```

- to backend （tps）

```
mysql> show engine innodb status\G

7526.10 inserts/s, 0.00 updates/s, 0.00 deletes/s, 10021.30 reads/s
7641.96 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
8153.88 inserts/s, 0.10 updates/s, 0.10 deletes/s, 0.40 reads/s
7911.91 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
8434.90 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
7940.17 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
7945.76 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
```

- to backend （I/O系统资源）

```
avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           7.11    0.00    4.72    0.97    1.42   85.78

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00     1.00    0.00 2362.00     0.00    26.38    22.87     0.78    0.33    0.00    0.33   0.28  66.00

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           8.85    0.00    4.58    1.08    1.27   84.21

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00     0.00    0.00 2405.00     0.00    22.43    19.10     0.84    0.35    0.00    0.35   0.29  70.00

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           8.48    0.00    4.78    1.08    1.28   84.38

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00     1.00    0.00 2457.00     0.00    27.09    22.58     0.92    0.38    0.00    0.38   0.30  73.20
```

### 测试结果

- shift 执行日志

```
 2017/07/26 20:24:47.672318 shift.go:252:         [INFO]        shift.dumping...
mysqldump: [Warning] Using a password on the command line interface can be insecure.
Warning: A partial dump from a server that has GTIDs will by default include the GTIDs of all transactions, even those that changed suppressed parts of the database. If you don't want to restore GTIDs, pass --set-gtid-purged=OFF. To make a complete dump, pass --all-databases --triggers --routines --events.
2017/07/26 20:27:57 dump.go:116: [info] dump MySQL and parse OK, use 189.81 seconds, start binlog replication at (mysql-bin.000023, 792216804)
 2017/07/26 20:27:57.482166 shift.go:255:         [INFO]        shift.wait.dumper.background.worker...
 2017/07/26 20:27:57.487524 shift.go:257:         [INFO]        shift.wait.dumper.background.worker.done...
 2017/07/26 20:27:57.487540 shift.go:258:         [INFO]        shift.set.dumper.background.worker.done...
 2017/07/26 20:27:57.487550 shift.go:260:         [INFO]        shift.dumping.done...
.......
 2017/07/26 20:27:59.006377 radon.go:186:         [INFO]        shift.set.radon.throttle.to.unlimits.done...
 2017/07/26 20:27:59.006390 radon.go:193:         [INFO]        shift.all.done...
shift.completed.OK!
```

-  迁移数据检查

```
mysql> select count(*) from benchyou1_0008;
+----------+
| count(*) |
+----------+
|  1474702 |
+----------+
1 row in set (0.24 sec)

ubuntu@i-q0azuc6s:~$ du -sh /data/mysql/sbtest/
20G /data/mysql/sbtest/
ubuntu@i-q0azuc6s:~$ du -sh /data/mysql/sbtest/benchyou1_0008.ibd
536M    /data/mysql/sbtest/benchyou1_0008.ibd
```

- 迁移速率

     单个分区表benchyou1_0008，147万（1474702）条元组，536MB，迁移耗时189秒

     平均每秒插入 7802条元组

## 230万条元组迁移（全量和增量-INSERT ONLY）

### 迁移说明

对benchyou1中的分区表benchyou1_0008在线进行迁移，

```
mysql> select count(*) from benchyou1;
+----------+
| count(*) |
+----------+
| 73673218 |
+----------+
1 row in set (3.53 sec)

mysql> select count(*) from benchyou1_0008;
+----------+
| count(*) |
+----------+
|  2301385 |
+----------+
1 row in set (0.40 sec)

mysql> select 73673218/32;
+--------------+
| 73673218/32  |
+--------------+
| 2302288.0625 |
+--------------+
1 row in set (0.00 sec)

ubuntu@i-q0azuc6s:~$ du -sh /data/mysql/sbtest/
30G	/data/mysql/sbtest/
ubuntu@i-q0azuc6s:~$ du -sh /data/mysql/sbtest/benchyou1_0008.ibd 
925M	/data/mysql/sbtest/benchyou1_0008.ibd

```

分区表benchyou1_0008的元组基本是benchyou1大表元组数量的三十二分之一，分区表数据量为925MB，推测大表数据大概在32GB左右。

###  测试步骤

- 启用 shift 做迁移

```
nohup ./shift --from="192.168.0.252:3306" --from-user="radon" --from-password="radon" --from-database="sbtest" --from-table="benchyou1_0008" --to="192.168.0.251:3306"  \
--to-password="radon" --to-table="benchyou1_0008" --to-user="radon" --to-database="sbtest" --threads=64 >shift_test.log 2>&1 &
```

- 启用 benchyou

```
ubuntu@i-81i76mko:~/tool$ cat random.sh 
../benchyou  --mysql-host="192.168.0.21" --mysql-user="radon" --mysql-password="radon" --ssh-user=radon --ssh-password="zhu1241jie" --oltp-tables-count=4 --write-threads=128 --read-threads=0 --max-time=14400 --mysql-enable-xa=0 random

nohup ./random.sh > test.log 2>&1  &

```

### 系统资源监测

- to backend 系统资源监测（I/O 、网络、tps ）

```
avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           6.73    0.00    4.25    5.59    1.52   81.90

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    18.00    0.00 4239.00     0.00   115.82    55.95     4.91    1.16    0.00    1.16   0.22  94.80

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           1.62    0.00    1.19    6.56    1.00   89.62

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    19.00    0.00 3662.00     0.00   113.55    63.50     5.14    1.40    0.00    1.40   0.26  96.00

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           7.51    0.00    4.13    7.63    2.38   78.35

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    26.00    0.00 4478.00     0.00   113.13    51.74     5.78    1.28    0.00    1.28   0.22  98.80

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           8.11    0.00    4.74    4.99    2.37   79.78

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    14.00    0.00 4099.00     0.00    77.69    38.81     3.55    0.88    0.00    0.88   0.22  88.80

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
           5.48    0.00    2.37    7.10    1.43   83.61

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    24.00    0.00 4424.00     0.00   127.92    59.22     5.12    1.16    0.00    1.16   0.22  98.40

```

```
Average:        IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s   %ifutil
Average:           lo     15.20     15.20      4.30      4.30      0.00      0.00      0.00      0.00
Average:         eth0   7477.30   7024.50   1980.40   5812.86      0.00      0.00      0.00      0.00
```

```
mysql> show engine innodb status\G

6760.12 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
6274.62 inserts/s, 0.05 updates/s, 0.05 deletes/s, 0.18 reads/s
3031.99 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
4049.10 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
5081.27 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
4944.94 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
5126.29 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
5389.30 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
6393.80 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s

```

- radon CPU

```
Tasks: 249 total,   1 running, 248 sleeping,   0 stopped,   0 zombie
%Cpu(s): 17.3 us,  6.8 sy,  0.0 ni, 72.5 id,  0.1 wa,  0.0 hi,  1.8 si,  1.5 st
KiB Mem : 65975832 total, 62885276 free,   347720 used,  2742836 buff/cache
KiB Swap:  2097148 total,  2097148 free,        0 used. 65062032 avail Mem

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND
 2003 ubuntu    20   0 1508676 117972   7880 S 222.3  0.2   1330:48 /opt/radon/radon -c /etc/radon3306.json
60421 ubuntu    20   0 1095024  16724   5076 S 155.1  0.0  13:01.83 ../benchyou --mysql-host=192.168.0.21 --mysql-user=radon --mysql-+
60363 ubuntu    20   0 1444256  11260   6124 S  51.8  0.0   5:03.01 ./shift --from=192.168.0.252:3306 --from-user=radon --from-passwo+
```

- benchyou tps

```
time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[274s]       [r:0,w:128]  9578     9578    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         20.54      NaN          1800031

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[275s]       [r:0,w:128]  2830     2830    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         23.84      NaN          1802861

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[276s]       [r:0,w:128]  4962     4962    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         23.45      NaN          1807823

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[277s]       [r:0,w:128]  5864     5864    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         32.77      NaN          1813687

time            thds       tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[278s]       [r:0,w:128]  9607     9607    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         10.49      NaN          1823294
```

### 测试结果

- shift 日志

```
 2017/07/27 10:03:28.672306 shift.go:252:         [INFO]        shift.dumping...
mysqldump: [Warning] Using a password on the command line interface can be insecure.
Warning: A partial dump from a server that has GTIDs will by default include the GTIDs of all transactions, even those that changed suppressed parts of the database. If you don't want to restore GTIDs, pass --set-gtid-purged=OFF. To make a complete dump, pass --all-databases --triggers --routines --events.
2017/07/27 10:14:40 dump.go:116: [info] dump MySQL and parse OK, use 672.10 seconds, start binlog replication at (mysql-bin.000036, 240755998)
 2017/07/27 10:14:40.770332 shift.go:255:         [INFO]        shift.wait.dumper.background.worker...
 2017/07/27 10:14:40.797569 shift.go:257:         [INFO]        shift.wait.dumper.background.worker.done...
 2017/07/27 10:14:40.797609 shift.go:258:         [INFO]        shift.set.dumper.background.worker.done...
 2017/07/27 10:14:40.797620 shift.go:260:         [INFO]        shift.dumping.done...
2017/07/27 10:14:40 sync.go:21: [info] start sync binlog at (mysql-bin.000036, 240755998)
2017/07/27 10:14:40 binlogsyncer.go:277: [info] begin to sync binlog from position (mysql-bin.000036, 240755998)

 2017/07/27 10:18:03.611366 radon.go:146:         [INFO]        shift.wait.until.pos.done...
 2017/07/27 10:18:03.611374 radon.go:151:         [INFO]        shift.checksum.table...
 2017/07/27 10:18:06.174056 radon.go:156:         [INFO]        shift.checksum.table.done...
 2017/07/27 10:18:06.174113 radon.go:161:         [INFO]        shift.set.radon.rule...
 2017/07/27 10:18:06.174153 radon.go:65:          [INFO]        shift.set.radon[http://127.0.0.1:8080/v1/shard/shift].rule.req[&{Database:sbtest Table:benchyou1_0008 FromAddress:192.168.0.252:3306 ToAddress:192.168.0.251:3306}]
 2017/07/27 10:18:06.176915 radon.go:166:         [INFO]        shift.set.radon.rule.done...
 2017/07/27 10:18:06.176939 radon.go:171:         [INFO]        shift.set.radon.to.write...
 2017/07/27 10:18:06.176952 radon.go:29:          [INFO]        shift.set.radon[http://127.0.0.1:8080/v1/radon/readonly].readonlly.req[&{Readonly:false}]
 2017/07/27 10:18:06.177341 radon.go:176:         [INFO]        shift.set.radon.to.write.done...
 2017/07/27 10:18:06.177360 radon.go:181:         [INFO]        shift.set.radon.throttle.to.unlimits...
 2017/07/27 10:18:06.177376 radon.go:100:         [INFO]        shift.set.radon[http://127.0.0.1:8080/v1/radon/throttle].throttle.to.req[&{Limits:0}].by.factor[0]
 2017/07/27 10:18:06.177715 radon.go:186:         [INFO]        shift.set.radon.throttle.to.unlimits.done...
 2017/07/27 10:18:06.177739 radon.go:193:         [INFO]        shift.all.done...
shift.completed.OK!
```

- 迁移数据检查

```
mysql> select count(*) from benchyou1_0008;
+----------+
| count(*) |
+----------+
|  2345243 |
+----------+
1 row in set (0.41 sec)

ubuntu@i-y8mtaivp:~$ du -sh /data/mysql/sbtest
30G	/data/mysql/sbtest
ubuntu@i-y8mtaivp:~$ du -sh /data/mysql/sbtest/benchyou1_0008.ibd 
881M	/data/mysql/sbtest/benchyou1_0008.ibd

mysql> select count(*) from benchyou1;
+----------+
| count(*) |
+----------+
| 75071043 |
+----------+
1 row in set (3.59 sec)


mysql> select 75071043-73673218;
+-------------------+
| 75071043-73673218 |
+-------------------+
|           1397825 |
+-------------------+
1 row in set (0.00 sec)

```

### 数据迁移结果

- 全量迁移：230万（2301385 ）条元组，迁移用时672秒，  平均每秒迁移3490条元组

- 增量迁移：43858（2345243-2301385）条元组，用时200秒左右，平均每秒增量219条元组

- 在线迁移总用时900秒


## 230万条元组迁移（全量和增量-READ，INSERT，UPDATE，DELETE）

### 迁移说明

基于上面的做的测试结果继续进行迁移测试，（benchyou 启用READ，UPDATE，DELETE）

### 测试步骤

- 启用 shift 做迁移

```
nohup ./shift --from="192.168.0.251:3306" --from-user="radon" --from-password="radon" --from-database="sbtest" --from-table="benchyou1_0008" --to="192.168.0.252:3306" --to-password="radon" --to-table="benchyou1_0008" --to-user="radon" --to-database="sbtest" --threads=64 > shift_test.log 2>&1 &
```

- 启用benchyou

```
ubuntu@i-81i76mko:~/tool$ cat random.sh
../benchyou --mysql-host="192.168.0.21" --mysql-user="radon" --mysql-password="radon" --ssh-user=radon --ssh-password="zhu1241jie" --oltp-tables-count=4 --write-threads=32 --update-threads=32 --read-threads=32 --delete-threads=32 --max-time=14400 --mysql-enable-xa=0 random

nohup ./random.sh > test.log 2>&1  &

```

### 系统资源监测

- to backend 系统资源监测（I/O、网络、tps）

```
avg-cpu:  %user   %nice %system %iowait  %steal   %idle
          12.73    0.00    6.07    3.29    3.75   74.16

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00     9.00    0.00 3809.00     0.00    69.88    37.57     2.89    0.76    0.00    0.76   0.23  88.00

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
          12.26    0.00    6.35    2.77    4.40   74.23

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    21.00    0.00 3732.00     0.00    73.06    40.09     3.09    0.83    0.00    0.83   0.21  79.60

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
          12.30    0.00    5.57    2.69    2.63   76.81

Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
vda               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
vdc               0.00    19.00    0.00 3832.00     0.00    77.30    41.31     2.63    0.69    0.00    0.69   0.21  79.20

```

```
Average:        IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s   %ifutil
Average:           lo     10.98     10.98      3.55      3.55      0.00      0.00      0.00      0.00
Average:         eth0  14179.57  13916.43   3295.03   7825.51      0.00      0.00      0.00      0.00
```

```
mysql> show engine innodb status\G

9336.66 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
7290.30 inserts/s, 0.05 updates/s, 0.05 deletes/s, 0.18 reads/s
6192.85 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
5918.79 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
6237.97 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
6454.28 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
6194.49 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s

```

- radon CPU

```
Tasks: 265 total,   1 running, 264 sleeping,   0 stopped,   0 zombie
%Cpu(s): 35.8 us, 15.6 sy,  0.0 ni, 44.5 id,  0.1 wa,  0.0 hi,  3.3 si,  0.8 st
KiB Mem : 65975832 total, 63478824 free,   358396 used,  2138612 buff/cache
KiB Swap:  2097148 total,  2097148 free,        0 used. 65048968 avail Mem 

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND
 2003 ubuntu    20   0 1508676 117632   7888 S 653.2  0.2   1350:33 /opt/radon/radon -c /etc/radon3306.json
 8937 ubuntu    20   0  931156  16204   5064 S 161.1  0.0   3:18.65 ../benchyou --mysql-host=192.168.0.21 --mysql-user=radon --mysql-+
 8877 ubuntu    20   0 1435004  11204   6108 S  67.8  0.0   1:56.83 ./shift --from=192.168.0.251:3306 --from-user=radon --from-passwo+

```

- benchyou tps

```
time            thds                 tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[261s]       [r:32,w:32,u:32,d:32]  26962    18942   8020    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         5.01       3.97         6993889

time            thds                 tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[262s]       [r:32,w:32,u:32,d:32]  28349    19617   8732    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         4.87       3.67         7022238

time            thds                 tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[263s]       [r:32,w:32,u:32,d:32]  27228    19046   8182    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         5.00       3.91         7049466

time            thds                 tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[264s]       [r:32,w:32,u:32,d:32]  24594    17189   7405    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         5.56       4.33         7074060

time            thds                 tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[265s]       [r:32,w:32,u:32,d:32]  25712    17940   7772    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    0       0         5.30       4.10         7099772

```

### 测试结果

- shift 日志

```
 2017/07/27 12:13:57.078281 shift.go:252:         [INFO]        shift.dumping...
mysqldump: [Warning] Using a password on the command line interface can be insecure.
Warning: A partial dump from a server that has GTIDs will by default include the GTIDs of all transactions, even those that changed suppressed parts of the database. If you don't want to restore GTIDs, pass --set-gtid-purged=OFF. To make a complete dump, pass --all-databases --triggers --routines --events.
2017/07/27 12:20:48 dump.go:116: [info] dump MySQL and parse OK, use 411.60 seconds, start binlog replication at (mysql-bin.000036, 524897490)
 2017/07/27 12:20:48.675361 shift.go:255:         [INFO]        shift.wait.dumper.background.worker...
 2017/07/27 12:20:48.687889 shift.go:257:         [INFO]        shift.wait.dumper.background.worker.done...
 2017/07/27 12:20:48.687925 shift.go:258:         [INFO]        shift.set.dumper.background.worker.done...
 2017/07/27 12:20:48.687940 shift.go:260:         [INFO]        shift.dumping.done...
2017/07/27 12:20:48 sync.go:21: [info] start sync binlog at (mysql-bin.000036, 524897490)
2017/07/27 12:20:48 binlogsyncer.go:277: [info] begin to sync binlog from position (mysql-bin.000036, 524897490)


 2017/07/27 12:21:22.019568 radon.go:146:         [INFO]        shift.wait.until.pos.done...
 2017/07/27 12:21:22.019575 radon.go:151:         [INFO]        shift.checksum.table...
 2017/07/27 12:21:24.475496 radon.go:156:         [INFO]        shift.checksum.table.done...
 2017/07/27 12:21:24.475541 radon.go:161:         [INFO]        shift.set.radon.rule...
 2017/07/27 12:21:24.475566 radon.go:65:          [INFO]        shift.set.radon[http://127.0.0.1:8080/v1/shard/shift].rule.req[&{Database:sbtest Table:benchyou1_0008 FromAddress:192.168.0.251:3306 ToAddress:192.168.0.252:3306}]
 2017/07/27 12:21:24.478204 radon.go:166:         [INFO]        shift.set.radon.rule.done...
 2017/07/27 12:21:24.478232 radon.go:171:         [INFO]        shift.set.radon.to.write...
 2017/07/27 12:21:24.478244 radon.go:29:          [INFO]        shift.set.radon[http://127.0.0.1:8080/v1/radon/readonly].readonlly.req[&{Readonly:false}]
 2017/07/27 12:21:24.478621 radon.go:176:         [INFO]        shift.set.radon.to.write.done...
 2017/07/27 12:21:24.478638 radon.go:181:         [INFO]        shift.set.radon.throttle.to.unlimits...
 2017/07/27 12:21:24.478653 radon.go:100:         [INFO]        shift.set.radon[http://127.0.0.1:8080/v1/radon/throttle].throttle.to.req[&{Limits:0}].by.factor[0]
 2017/07/27 12:21:24.478968 radon.go:186:         [INFO]        shift.set.radon.throttle.to.unlimits.done...
 2017/07/27 12:21:24.478990 radon.go:193:         [INFO]        shift.all.done...
shift.completed.OK!

```

- 迁移数据检查

```
mysql> select count(*) from benchyou1_0008;
+----------+
| count(*) |
+----------+
|  2355425 |
+----------+
1 row in set (0.42 sec)


ubuntu@i-q0azuc6s:~$ du -sh /data/mysql/sbtest/
31G	/data/mysql/sbtest/
ubuntu@i-q0azuc6s:~$ du -sh /data/mysql/sbtest/benchyou1_0008.ibd 
853M	/data/mysql/sbtest/benchyou1_0008.ibd

mysql> select count(*) from benchyou1;
+----------+
| count(*) |
+----------+
| 75393611 |
+----------+
1 row in set (3.84 sec)

```

### 数据迁移结果

- 全量迁移：234万（2345243 ）条元组，迁移用时411秒，  平均每秒迁移5706条元组

- 增量迁移：耗时34秒

- 在线迁移总用时450秒

## 测试结果分析

对大表benchyou1，32GB左右，（7300～7500万条元组）的一个分区表benchyou1_0008，1GB（230万条元组）进行在线迁移。 

- benchyou只做INSERT操作，集群负载较高，在线迁移相对较慢（900秒)，迁移速度：

```
	3490 rows/sec

    0.97 MB/sec
```

- benchyou做READ，INSERT，UPDATE，DELETE操作，集群负载较低，在线迁移相对较快（450秒），迁移速度：

```
	5706 rows/sec

    1.89 MB/sec
```

总体分析，shift迁移工具基本可以满足AppCenter线上迁移需求。
