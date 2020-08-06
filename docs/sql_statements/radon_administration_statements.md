Table of Contents
=================

   * [Radon](#radon)
      * [RADON ATTACH](#radon-attach)
      * [RADON ATTACHLIST](#radon-attachlist)
      * [RADON DETACH](#radon-detach)
      * [RADON RESHARD](#radon-reshard)
      * [RADON CLEANUP](#radon-cleanup)
      * [RADON REBALANCE](#radon-rebalance)

# Radon
## RADON ATTACH

`Syntax`
```
RADON ATTACH($address,$user,$password)
```

`Instructions`
* Attch one mysql as Radon's backend. 
* The type is 'attach' in `backend.json`.

`Example: `

```
mysql> radon attach('127.0.0.1:3306','root','123456');
Query OK, 0 rows affected (0.94 sec)
```

## RADON ATTACHLIST

`Instructions`
* List the backend of type `attach`. 

`Example: `
```
mysql> radon attachlist;
+----------------+----------------+------+
| Name           | Address        | User |
+----------------+----------------+------+
| 127.0.0.1:3306 | 127.0.0.1:3306 | root |
+----------------+----------------+------+
1 row in set (0.00 sec)
```

## RADON DETACH

`Syntax`
```
RADON DETACH($address)
```

`Instructions`
* Detach the backend of type `attach`. 

```
mysql> radon detach('127.0.0.1:3306');
Query OK, 0 rows affected (0.22 sec)

mysql> radon attachlist;
Empty set (0.00 sec)
```

## RADON RESHARD

`Syntax`
```
RADON RESHARD tbl_name TO new_tbl_name
```

`Instructions`
* RADON RESHARD can shift data from one SINGLE table to another PARTITION table. 
* The cmd execute the shift cmd and will return immediately, the shift will run in background on other goroutine.
* The SINGLE table with the primary key can be partitioned.

```
mysql> show tables;
Empty set (0.10 sec)

mysql> create table t1(a int primary key, b varchar(255)) single;
Query OK, 0 rows affected (0.13 sec)

mysql> insert into t1(a,b) values(1,'a'),(2,'b');
Query OK, 2 rows affected (0.10 sec)

mysql> radon reshard t1 to new_tb;
Query OK, 0 rows affected (0.00 sec)

mysql> show tables;
+---------------+
| Tables_in_zzq |
+---------------+
| t1            |
| new_tb        |
+---------------+
2 rows in set (0.10 sec)

mysql> show create table new_tb;
+--------+----------------------------------------------------------------+
| Table  | Create Table                                                   |
+--------+----------------------------------------------------------------+
| new_tb | CREATE TABLE `new_tb` (
  `a` int(11) NOT NULL,
  `b` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH(a) */ |
+--------+----------------------------------------------------------------+
1 row in set (0.05 sec)

mysql> select * from new_tb;
+---+------+
| a | b    |
+---+------+
| 1 | a    |
| 2 | b    |
+---+------+
2 rows in set (1.09 sec)
```

## RADON CLEANUP

`Syntax`
```
RADON CLEANUP
```

`Instructions`
* RADON CLEANUP can clean up the old data after shifted.

```
mysql> radon cleanup;
Query OK, 0 rows affected (0.13 sec)
```

## RADON REBALANCE

`Syntax`
```
RADON REBALANCE
```

`Instructions`
* If the radon running for a long time, the user find the data are imbalance among the backends
* This admin command aims for re-balance the data(partition tables) among the backends, migrate only one partition table per operation.
The internal operation is mainly divided into two steps:
    1. get the advice about the rebalance on the shards.
    2. migrate data on the partition table from source backend to target backend
     according to the above advice.
```
mysql> radon rebalance;
Query OK, 0 rows affected (39.09 sec)
```
* Shown below is the  Data rebalance before and after `RADON REBALANCE`
```
mysql> show status;
...
| radon_backend     | {
	"Backends": [
		"{'name': 'node1', 'tables': '1045', 'datasize':'169MB'}",
		"{'name': 'node2', 'tables': '1047', 'datasize':'28MB'}"
	]
}
```
```
mysql> radon rebalance;
Query OK, 0 rows affected (39.09 sec)
```
```
mysql> show status;
...
| radon_backend     | {
	"Backends": [
		"{'name': 'node1', 'tables': '1044', 'datasize':'135MB'}",
		"{'name': 'node2', 'tables': '1048', 'datasize':'62MB'}"
	]
}
```

* NOTICE: If execute the cmd: RADON REBALANCE, the client interface will always stop there. During the period, if the user execute `ctrl+c` or exit the client, the rebalance will keep going on, the user has to find the status in the log file, it is successful that the following line exists in the file, if not find the line, need find the cause based on the error log.
```
  [WARNING]      rebalance.migrate.done...
```

