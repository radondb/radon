[![Build Status](https://travis-ci.org/XeLabs/go-mydumper.png)](https://travis-ci.org/XeLabs/go-mydumper) [![Go Report Card](https://goreportcard.com/badge/github.com/XeLabs/go-mydumper)](https://goreportcard.com/report/github.com/XeLabs/go-mydumper) [![codecov.io](https://codecov.io/gh/XeLabs/go-mydumper/graphs/badge.svg)](https://codecov.io/gh/XeLabs/go-mydumper/branch/master)

# go-mydumper

***go-mydumper*** is a multi-threaded MySQL backup and restore tool, and it is compatible with [maxbube/mydumper](https://github.com/maxbube/mydumper) in the layout.


## Build

```
$git clone https://github.com/XeLabs/go-mydumper
$cd go-mydumper
$make
$./bin/mydumper   --help
$./bin/myloader   --help
$./bin/mystreamer --help
```

## Test

```
$make test
```

## Usage

### mydumper

```
./bin/mydumper --help
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

Examples:
$./bin/mydumper -h 192.168.0.1 -P 3306 -u mock -p mock -db sbtest  -o sbtest.sql
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
 2017/10/25 13:13:03.098937 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[2527169].bytes[512MB].part[4].thread[2]
 2017/10/25 13:13:05.344051 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[3157657].bytes[640MB].part[5].thread[1]
 2017/10/25 13:13:05.429411 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[3157655].bytes[640MB].part[5].thread[2]
 2017/10/25 13:13:07.649915 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[3788144].bytes[768MB].part[6].thread[2]
 2017/10/25 13:13:07.709657 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[3788144].bytes[768MB].part[6].thread[1]
 2017/10/25 13:13:09.839178 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[4418631].bytes[896MB].part[7].thread[2]
 2017/10/25 13:13:09.899665 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[4418633].bytes[896MB].part[7].thread[1]
 2017/10/25 13:13:12.046545 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[5049118].bytes[1024MB].part[8].thread[2]
 2017/10/25 13:13:12.109284 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[5049121].bytes[1024MB].part[8].thread[1]
 2017/10/25 13:13:12.939307 dumper.go:182:        [INFO]        dumping.allbytes[2150MB].allrows[10604068].time[20.01sec].rates[107.47MB/sec]...
 2017/10/25 13:13:14.404759 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[5679606].bytes[1152MB].part[9].thread[2]
 2017/10/25 13:13:14.467313 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[5679607].bytes[1152MB].part[9].thread[1]
 2017/10/25 13:13:16.788106 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[6310093].bytes[1280MB].part[10].thread[2]
 2017/10/25 13:13:16.894142 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[6310093].bytes[1280MB].part[10].thread[1]
 2017/10/25 13:13:19.125115 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[6940580].bytes[1408MB].part[11].thread[2]
 2017/10/25 13:13:19.196091 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[6940582].bytes[1408MB].part[11].thread[1]
 2017/10/25 13:13:21.401179 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[7571067].bytes[1536MB].part[12].thread[2]
 2017/10/25 13:13:21.489994 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[7571070].bytes[1536MB].part[12].thread[1]
 2017/10/25 13:13:22.939262 dumper.go:182:        [INFO]        dumping.allbytes[3256MB].allrows[16050014].time[30.01sec].rates[108.51MB/sec]...
 2017/10/25 13:13:23.705833 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[8201555].bytes[1664MB].part[13].thread[2]
 2017/10/25 13:13:23.796722 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[8201556].bytes[1664MB].part[13].thread[1]
 2017/10/25 13:13:26.088441 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[8832041].bytes[1792MB].part[14].thread[2]
 2017/10/25 13:13:26.185251 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[8832044].bytes[1792MB].part[14].thread[1]
 2017/10/25 13:13:28.338164 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[9462159].bytes[1920MB].part[15].thread[2]
 2017/10/25 13:13:28.392105 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[9462164].bytes[1920MB].part[15].thread[1]
 2017/10/25 13:13:30.658287 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[10089992].bytes[2048MB].part[16].thread[2]
 2017/10/25 13:13:30.708556 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[10089998].bytes[2048MB].part[16].thread[1]
 2017/10/25 13:13:32.939424 dumper.go:182:        [INFO]        dumping.allbytes[4352MB].allrows[21435655].time[40.01sec].rates[108.78MB/sec]...
 2017/10/25 13:13:32.964288 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[10717825].bytes[2176MB].part[17].thread[2]
 2017/10/25 13:13:33.110688 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[10717830].bytes[2176MB].part[17].thread[1]
 2017/10/25 13:13:35.408958 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[11345661].bytes[2304MB].part[18].thread[1]
 2017/10/25 13:13:35.496439 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[11345659].bytes[2304MB].part[18].thread[2]
 2017/10/25 13:13:37.627178 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[11974624].bytes[2432MB].part[19].thread[1]
 2017/10/25 13:13:37.753966 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[11974630].bytes[2432MB].part[19].thread[2]
 2017/10/25 13:13:39.453430 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou0].done.allrows[12486842].allbytes[2536MB].thread[1]...
 2017/10/25 13:13:39.453462 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1].done...
 2017/10/25 13:13:39.622390 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou1].done.allrows[12484135].allbytes[2535MB].thread[2]...
 2017/10/25 13:13:39.622423 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2].done...
 2017/10/25 13:13:39.622454 dumper.go:188:        [INFO]        dumping.all.done.cost[46.69sec].allrows[24970977].allbytes[5318557708].rate[108.63MB/s]
```

The dump files:
```
$ ls sbtest.sql/
metadata                    sbtest.benchyou0.00009.sql  sbtest.benchyou0.00018.sql   sbtest.benchyou1.00006.sql  sbtest.benchyou1.00015.sql
sbtest.benchyou0.00001.sql  sbtest.benchyou0.00010.sql  sbtest.benchyou0.00019.sql   sbtest.benchyou1.00007.sql  sbtest.benchyou1.00016.sql
sbtest.benchyou0.00002.sql  sbtest.benchyou0.00011.sql  sbtest.benchyou0.00020.sql   sbtest.benchyou1.00008.sql  sbtest.benchyou1.00017.sql
sbtest.benchyou0.00003.sql  sbtest.benchyou0.00012.sql  sbtest.benchyou0-schema.sql  sbtest.benchyou1.00009.sql  sbtest.benchyou1.00018.sql
sbtest.benchyou0.00004.sql  sbtest.benchyou0.00013.sql  sbtest.benchyou1.00001.sql   sbtest.benchyou1.00010.sql  sbtest.benchyou1.00019.sql
sbtest.benchyou0.00005.sql  sbtest.benchyou0.00014.sql  sbtest.benchyou1.00002.sql   sbtest.benchyou1.00011.sql  sbtest.benchyou1.00020.sql
sbtest.benchyou0.00006.sql  sbtest.benchyou0.00015.sql  sbtest.benchyou1.00003.sql   sbtest.benchyou1.00012.sql  sbtest.benchyou1-schema.sql
sbtest.benchyou0.00007.sql  sbtest.benchyou0.00016.sql  sbtest.benchyou1.00004.sql   sbtest.benchyou1.00013.sql  sbtest-schema-create.sql
sbtest.benchyou0.00008.sql  sbtest.benchyou0.00017.sql  sbtest.benchyou1.00005.sql   sbtest.benchyou1.00014.sql
```

### myloader

```
$ ./bin/myloader --help
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

Examples:
$./bin/myloader -h 192.168.0.2 -P 3306 -u mock -p mock -d sbtest.sql
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
 2017/10/25 13:04:17.516854 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00003].thread[15]
 2017/10/25 13:04:17.516893 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00013].thread[0]
 2017/10/25 13:04:39.820177 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00020].thread[3].done...
 2017/10/25 13:04:39.820255 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00012].thread[3]
 2017/10/25 13:04:41.483128 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00020].thread[6].done...
 2017/10/25 13:04:41.483198 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00010].thread[6]
 2017/10/25 13:04:46.865340 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00015].thread[1].done...
 2017/10/25 13:04:46.865421 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00014].thread[1]
 2017/10/25 13:04:47.211336 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00005].thread[2].done...
 2017/10/25 13:04:47.211415 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00007].thread[2]
 2017/10/25 13:04:47.399552 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00009].thread[10].done...
 2017/10/25 13:04:47.399638 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00007].thread[10]
 2017/10/25 13:04:47.835980 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00002].thread[8].done...
 2017/10/25 13:04:47.836068 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00003].thread[8]
 2017/10/25 13:04:49.079171 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00009].thread[4].done...
 2017/10/25 13:04:49.079249 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00015].thread[4]
 2017/10/25 13:04:49.279380 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00017].thread[11].done...
 2017/10/25 13:04:49.279457 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00018].thread[11]
 2017/10/25 13:04:49.539250 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00008].thread[13].done...
 2017/10/25 13:04:49.539329 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00012].thread[13]
 2017/10/25 13:04:49.574229 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00018].thread[5].done...
 2017/10/25 13:04:49.574314 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00016].thread[5]
 2017/10/25 13:04:49.834601 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00010].thread[12].done...
 2017/10/25 13:04:49.834682 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00006].thread[12]
2017/10/25 13:04:49.834682 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00006].thread[12]
 2017/10/25 13:04:49.851738 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00002].thread[14].done...
 2017/10/25 13:04:49.851815 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00017].thread[14]
 2017/10/25 13:04:50.101755 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00019].thread[7].done...
 2017/10/25 13:04:50.101831 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00004].thread[7]
 2017/10/25 13:04:50.476413 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00013].thread[0].done...
 2017/10/25 13:04:50.476499 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00001].thread[0]
 2017/10/25 13:04:50.667836 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00003].thread[15].done...
 2017/10/25 13:04:50.667916 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00013].thread[15]
 2017/10/25 13:04:50.702259 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00011].thread[9].done...
 2017/10/25 13:04:50.702397 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00005].thread[9]
 2017/10/25 13:05:17.111718 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00012].thread[3].done...
 2017/10/25 13:05:17.111804 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00001].thread[3]
 2017/10/25 13:05:17.374860 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00010].thread[6].done...
 2017/10/25 13:05:17.374944 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00016].thread[6]
 2017/10/25 13:05:21.331044 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00007].thread[2].done...
 2017/10/25 13:05:21.331130 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00014].thread[2]
 2017/10/25 13:05:23.707516 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00014].thread[1].done...
 2017/10/25 13:05:23.707602 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00011].thread[1]
 2017/10/25 13:05:23.857451 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00007].thread[10].done...
 2017/10/25 13:05:23.857561 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00008].thread[10]
 2017/10/25 13:05:24.975366 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00001].thread[0].done...
 2017/10/25 13:05:24.975446 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00004].thread[0]
 2017/10/25 13:05:25.026926 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00003].thread[8].done...
 2017/10/25 13:05:25.027012 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00019].thread[8]
 2017/10/25 13:05:26.133469 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00018].thread[11].done...
 2017/10/25 13:05:26.133612 loader.go:115:        [INFO]        restoring.tables[benchyou1].parts[00006].thread[11]
 2017/10/25 13:05:26.696523 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00015].thread[4].done...
 2017/10/25 13:05:26.855668 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00012].thread[13].done...
 2017/10/25 13:05:26.878910 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00006].thread[12].done...
 2017/10/25 13:05:26.979248 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00016].thread[5].done...
 2017/10/25 13:05:26.995089 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00017].thread[14].done...
 2017/10/25 13:05:27.107230 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00004].thread[7].done...
 2017/10/25 13:05:27.160083 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00013].thread[15].done...
 2017/10/25 13:05:27.783560 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00005].thread[9].done...
 2017/10/25 13:05:36.133758 loader.go:181:        [INFO]        restoring.allbytes[4087MB].time[78.62sec].rates[51.99MB/sec]...
 2017/10/25 13:05:44.759183 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00001].thread[3].done...
 2017/10/25 13:05:46.133728 loader.go:181:        [INFO]        restoring.allbytes[4216MB].time[88.62sec].rates[47.58MB/sec]...
 2017/10/25 13:05:46.567156 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00016].thread[6].done...
 2017/10/25 13:05:50.612200 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00008].thread[10].done...
 2017/10/25 13:05:51.131155 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00014].thread[2].done...
 2017/10/25 13:05:51.185629 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00011].thread[1].done...
 2017/10/25 13:05:51.836354 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00004].thread[0].done...
 2017/10/25 13:05:52.286931 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00006].thread[11].done...
 2017/10/25 13:05:52.602444 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00019].thread[8].done...
 2017/10/25 13:05:52.602573 loader.go:187:        [INFO]        restoring.all.done.cost[95.09sec].allbytes[5120.00MB].rate[53.85MB/s]
```

### mystreamer

Streaming mode, dumps datas from upstream to downstream in parallel instead of dumping to the out directory.
```
$./bin/mystreamer
Usage: ./bin/mystreamer -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -2h [DOWNSTREAM-HOST] -2P [DOWNSTREAM-PORT] -2u [DOWNSTREAM-USER] -2p [DOWNSTREAM-PASSWORD] [-2db DOWNSTREAM-DATABASE] [-o]
  -2P int
    	Downstream TCP/IP port to connect to (default 3306)
  -2db string
    	Downstream database, default is same as upstream db
  -2h string
    	The downstream host to connect to
  -2p string
    	Downstream user password
  -2u string
    	Downstream username with privileges to run the streamer
  -P int
    	Upstream TCP/IP port to connect to (default 3306)
  -db string
    	Database to stream
  -h string
    	The upstream host to connect to
  -o	Drop tables if they already exist
  -p string
    	Upstream user password
  -s int
    	Attempted size of INSERT statement in bytes (default 1000000)
  -t int
    	Number of threads to use (default 16)
  -table string
    	Table to stream
  -u string
    	Upstream username with privileges to run the streamer

Examples:
$./bin/mystreamer -h 192.168.0.2 -P 3306 -u mock -p mock -2h 192.168.0.3 -2P 3306 -2u mock -2p mock -db sbtest
 2017/11/17 23:52:46.424107 streamer.go:30:       [INFO]        streaming.database[sbtest].schema...
 2017/11/17 23:52:46.458807 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou0].schema...
 2017/11/17 23:52:46.458892 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou0].datas.thread[1]...
 2017/11/17 23:52:46.509911 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou1].schema...
 2017/11/17 23:52:46.510008 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou1].datas.thread[2]...
 2017/11/17 23:52:46.557443 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou2].schema...
 2017/11/17 23:52:46.557499 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou2].datas.thread[3]...
 2017/11/17 23:52:46.608464 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou3].schema...
 2017/11/17 23:52:46.608526 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou3].datas.thread[4]...
 2017/11/17 23:52:46.667519 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou4].schema...
 2017/11/17 23:52:46.667584 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou4].datas.thread[5]...
 2017/11/17 23:52:46.807810 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou5].schema...
 2017/11/17 23:52:46.808157 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou5].datas.thread[6]...
 2017/11/17 23:52:46.946838 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou6].schema...
 2017/11/17 23:52:46.946902 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou6].datas.thread[7]...
 2017/11/17 23:52:47.114519 streamer.go:50:       [INFO]        streaming.table[sbtest.benchyou7].schema...
 2017/11/17 23:52:47.114609 streamer.go:165:      [INFO]        streaming.table[sbtest.benchyou7].datas.thread[8]...
 2017/11/17 23:52:52.198910 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou0].done.allrows[133342].allbytes[28MB].thread[1]...
 2017/11/17 23:52:52.198965 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou0].datas.thread[1].done...
 2017/11/17 23:52:52.728184 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou1].done.allrows[133875].allbytes[28MB].thread[2]...
 2017/11/17 23:52:52.728221 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou1].datas.thread[2].done...
 2017/11/17 23:52:52.774523 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou3].done.allrows[133459].allbytes[28MB].thread[4]...
 2017/11/17 23:52:52.774792 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou3].datas.thread[4].done...
 2017/11/17 23:52:52.887324 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou2].done.allrows[133849].allbytes[28MB].thread[3]...
 2017/11/17 23:52:52.887564 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou2].datas.thread[3].done...
 2017/11/17 23:52:53.056884 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou4].done.allrows[133995].allbytes[28MB].thread[5]...
 2017/11/17 23:52:53.056923 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou4].datas.thread[5].done...
 2017/11/17 23:52:53.319846 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou5].done.allrows[134490].allbytes[28MB].thread[6]...
 2017/11/17 23:52:53.319881 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou5].datas.thread[6].done...
 2017/11/17 23:52:53.338505 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou6].done.allrows[134035].allbytes[28MB].thread[7]...
 2017/11/17 23:52:53.338553 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou6].datas.thread[7].done...
 2017/11/17 23:52:53.465916 streamer.go:121:      [INFO]        streaming.table[sbtest.benchyou7].done.allrows[133240].allbytes[28MB].thread[8]...
 2017/11/17 23:52:53.465958 streamer.go:167:      [INFO]        streaming.table[sbtest.benchyou7].datas.thread[8].done...
 2017/11/17 23:52:53.465999 streamer.go:185:      [INFO]        streaming.all.done.cost[7.04sec].allrows[1070285].allbytes[240555976].rate[32.52MB/s]
```
