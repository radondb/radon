Table of Contents
=================

   * [Radon](#radon)
      * [RADON ATTACH](#radon-attach)
      * [RADON ATTACHLIST](#radon-attachlist)
      * [RADON DETACH](#radon-detach)
      * [RADON RESHARD](#radon-reshard)
      * [RADON CLEANUP](#radon-cleanup)

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
