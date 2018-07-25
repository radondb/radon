Contents
=================

* [How to Import and Export Data](#how-to-import-and-export-data)
   * [1. Background](#1-background)
   * [2. Make build](#2-make-build)
   * [3. How to import data](#3-how-to-import-data)
      * [3.1 export data from data source](#31-export-data-from-data-source)
      * [3.2 modify schema](#32-modify-schema)
      * [3.3 import data to radon](#33-import-data-to-radon)
      * [3.4 test](#34-test)
   * [4. How to export data](#4-how-to-export-data)

# How to Import and Export Data

## 1. Background

Currently, the only way to import and export data used by `radon` is `go-mydumper`.

[XeLabs/go-mydumper](https://github.com/XeLabs/go-mydumper) is developed by golang. It is compatible with [maxbube/mydumper](https://github.com/maxbube/mydumper) in the layout, but `go-mydumper` is optimized for parallelism, and performance is more remarkable.

Importing data to `radon`, `go-mydumper` will batch import data in parallel, very fast. 

Exporting data from `radon`, `go-mydumper` will export data in batch parallel streaming mode, the resource occupancy rate is low.

## 2. Make build

```plain
$git clone https://github.com/XeLabs/go-mydumper
$cd go-mydumper
$make

$./bin/mydumper --help
Usage: ./bin/mydumper -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR]
  -F int
        Split tables into chunks of this output file size. This value is in MB (default 128)
  -P int
        TCP/IP port to connect to (default 3306)
  -db string
        Database to dump
  -h string
        The host to connect to
  -o string
        Directory to output files to
  -p string
        User password
  -s int
        Attempted size of INSERT statement in bytes (default 1000000)
  -t int
        Number of threads to use (default 16)
  -table string
        Table to dump
  -u string
        Username with privileges to run the dump

$./bin/myloader --help
Usage: ./bin/myloader -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -d  [DIR]
  -P int
        TCP/IP port to connect to (default 3306)
  -d string
        Directory of the dump to import
  -h string
        The host to connect to
  -p string
        User password
  -t int
        Number of threads to use (default 16)
  -u string
        Username with privileges to run the loader
```

## 3. How to import data

### 3.1 export data from data source

Firstly, use `mydumper` to export data from the other `MySQL` data source. For example:
```plain
$./bin/mydumper -h 192.168.0.2 -P 3306 -u test -p test -db sbtest  -o sbtest.sql
 2017/10/25 13:12:52.933391 dumper.go:35:         [INFO]        dumping.database[sbtest].schema...
 2017/10/25 13:12:52.937743 dumper.go:45:         [INFO]        dumping.table[sbtest.benchyou0].schema...
 2017/10/25 13:12:52.937791 dumper.go:168:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1]...
 2017/10/25 13:12:52.939008 dumper.go:45:         [INFO]        dumping.table[sbtest.benchyou1].schema...
 2017/10/25 13:12:52.939055 dumper.go:168:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2]...
 2017/10/25 13:12:55.611905 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[633987].bytes[128MB].part[1].thread[1]
 2017/10/25 13:12:55.765127 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[633987].bytes[128MB].part[1].thread[2]
 2017/10/25 13:12:58.146093 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[1266050].bytes[256MB].part[2].thread[1]

 ...snip...

 2017/10/25 13:13:37.627178 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[11974624].bytes[2432MB].part[19].thread[1]
 2017/10/25 13:13:37.753966 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[11974630].bytes[2432MB].part[19].thread[2]
 2017/10/25 13:13:39.453430 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou0].done.allrows[12486842].allbytes[2536MB].thread[1]...
 2017/10/25 13:13:39.453462 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1].done...
 2017/10/25 13:13:39.622390 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou1].done.allrows[12484135].allbytes[2535MB].thread[2]...
 2017/10/25 13:13:39.622423 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2].done...
 2017/10/25 13:13:39.622454 dumper.go:188:        [INFO]        dumping.all.done.cost[46.69sec].allrows[24970977].allbytes[5318557708].rate[108.63MB/s]
```
### 3.2 modify schema

In the export directory(such as `sbtest.sql`), find `*-schema.sql`(such as `sbtest.benchyou0-scehma.sql`) :

At the end of the original sentence, adding ‘`PARTITON BY HASH(ShardKey)`’ syntax:

sbtest.benchyou0-schema.sql:

```sql
CREATE TABLE `benchyou0` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `k` bigint(20) unsigned NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `k_1` (`k`)
) ENGINE=InnoDB;
```

Modified (`id` is ShardKey):

```sql
CREATE TABLE `benchyou0` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `k` bigint(20) unsigned NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `k_1` (`k`)
) ENGINE=InnoDB PARTITION BY HASH(id);
```

### 3.3 import data to radon

```plain
$./bin/myloader -h 192.168.0.2 -P 3306 -u radondb -p radondb -d sbtest.sql
 2017/10/25 13:04:17.396002 loader.go:75:         [INFO]        restoring.database[sbtest]
 2017/10/25 13:04:17.458076 loader.go:99:         [INFO]        restoring.schema[sbtest.benchyou0]
 2017/10/25 13:04:17.516236 loader.go:99:         [INFO]        restoring.schema[sbtest.benchyou1]
 2017/10/25 13:04:17.516389 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00015].thread[1]
 2017/10/25 13:04:17.516456 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00005].thread[2]
 2017/10/25 13:04:17.516486 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00020].thread[3]
 2017/10/25 13:04:17.516523 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00009].thread[4]
 2017/10/25 13:04:17.516550 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00018].thread[5]
 2017/10/25 13:04:17.516572 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00020].thread[6]
 2017/10/25 13:04:17.516606 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00019].thread[7]
 2017/10/25 13:04:17.516655 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00002].thread[8]
 2017/10/25 13:04:17.516692 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00011].thread[9]
 2017/10/25 13:04:17.516718 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00009].thread[10]
 2017/10/25 13:04:17.516739 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00017].thread[11]
 2017/10/25 13:04:17.516772 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00010].thread[12]
 2017/10/25 13:04:17.516797 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00008].thread[13]
 2017/10/25 13:04:17.516818 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00002].thread[14]
 2017/10/25 13:04:50.476413 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00013].thread[0].done...

 ...snip...

 2017/10/25 13:04:50.476499 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00001].thread[0]
 2017/10/25 13:04:50.667836 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00003].thread[15].done...
 2017/10/25 13:04:50.667916 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00013].thread[15]
 2017/10/25 13:04:50.702259 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00011].thread[9].done...
 2017/10/25 13:04:50.702397 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00005].thread[9]
 2017/10/25 13:05:52.286931 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00006].thread[11].done...
 2017/10/25 13:05:52.602444 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00019].thread[8].done...
 2017/10/25 13:05:52.602573 loader.go:187:        [INFO]        restoring.all.done.cost[95.09sec].allbytes[5120.00MB].rate[53.85MB/s]
```

### 3.4 test

```plain
1) before export data from data source
mysql> show tables;
+------------------+
| Tables_in_sbtest |
+------------------+
| benchyou0        |
| benchyou1        |
+------------------+
2 rows in set (0.00 sec)

mysql> select count(*) from benchyou0;
+----------+
| count(*) |
+----------+
| 12486842 |
+----------+
1 row in set (6.43 sec)

mysql> select count(*) from benchyou1;
+----------+
| count(*) |
+----------+
| 12484135 |
+----------+

2) after import data to `radon`

mysql> show tables;
+------------------+
| Tables_in_sbtest |
+------------------+
| benchyou0        |
| benchyou1        |
+------------------+
2 rows in set (0.00 sec)

mysql> select count(*) from benchyou0;
+----------+
| count(*) |
+----------+
| 12486842 |
+----------+
1 row in set (1.30 sec)

mysql> select count(*) from benchyou1;
+----------+
| count(*) |
+----------+
| 12484135 |
+----------+
1 row in set (1.25 sec)
```

## 4. How to export data

Use `mydumper` to export data from `radon`, this process is stream acquire (select statement with '/*backup*/'hint) and export, basically does not occupy system memory.

```plain
$./bin/mydumper -h 192.168.0.2 -P 3306 -u radondb -p radondb -db sbtest  -o sbtest.sql
 2017/10/25 13:12:52.933391 dumper.go:35:         [INFO]        dumping.database[sbtest].schema...
 2017/10/25 13:12:52.937743 dumper.go:45:         [INFO]        dumping.table[sbtest.benchyou0].schema...
 2017/10/25 13:12:52.937791 dumper.go:168:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1]...
 2017/10/25 13:12:52.939008 dumper.go:45:         [INFO]        dumping.table[sbtest.benchyou1].schema...
 2017/10/25 13:12:52.939055 dumper.go:168:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2]...
 2017/10/25 13:12:55.611905 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[633987].bytes[128MB].part[1].thread[1]
 2017/10/25 13:12:55.765127 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[633987].bytes[128MB].part[1].thread[2]
 2017/10/25 13:12:58.146093 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[1266050].bytes[256MB].part[2].thread[1]
 2017/10/25 13:12:58.253219 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[1266054].bytes[256MB].part[2].thread[2]
 2017/10/25 13:13:00.545536 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[1896681].bytes[384MB].part[3].thread[1]
 2017/10/25 13:13:00.669499 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[1896682].bytes[384MB].part[3].thread[2]
 2017/10/25 13:13:02.939278 dumper.go:182:        [INFO]        dumping.allbytes[1024MB].allrows[5054337].time[10.01sec].rates[102.34MB/sec]...
 2017/10/25 13:13:03.012645 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[2527168].bytes[512MB].part[4].thread[1]

 ... ...

 2017/10/25 13:13:37.627178 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[11974624].bytes[2432MB].part[19].thread[1]
 2017/10/25 13:13:37.753966 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[11974630].bytes[2432MB].part[19].thread[2]
 2017/10/25 13:13:39.453430 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou0].done.allrows[12486842].allbytes[2536MB].thread[1]...
 2017/10/25 13:13:39.453462 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1].done...
 2017/10/25 13:13:39.622390 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou1].done.allrows[12484135].allbytes[2535MB].thread[2]...
 2017/10/25 13:13:39.622423 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2].done...
 2017/10/25 13:13:39.622454 dumper.go:188:        [INFO]        dumping.all.done.cost[46.69sec].allrows[24970977].allbytes[5318557708].rate[108.63MB/s]
```