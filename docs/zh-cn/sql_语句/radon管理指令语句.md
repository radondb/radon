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

`语法`
```
RADON ATTACH($address,$user,$password)
```

`说明`
* 挂载一个MySQL作为Radon的后端。
* 在`backend.json`中类型为`attach`

`示例: `

```
mysql> radon attach('127.0.0.1:3306','root','123456');
Query OK, 0 rows affected (0.94 sec)
```

## RADON ATTACHLIST

`说明`
* 列出`backend.json`中类型为`attach`的后端。

`示例: `
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

`语法`
```
RADON DETACH($address)
```

`说明`
* 移除`backend.json`中类型为`attach`的后端。

```
mysql> radon detach('127.0.0.1:3306');
Query OK, 0 rows affected (0.22 sec)

mysql> radon attachlist;
Empty set (0.00 sec)
```

## RADON RESHARD

`语法`
```
RADON RESHARD tbl_name TO new_tbl_name
```

`说明`
* RADON RESHARD可以将一个SINGLE表迁移为另一张分区表。
* 这条指令将会立即返回，迁移操作会在启动线程在后台执行。
* 带主键的SINGLE表可以被分区。

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

`语法`
```
RADON CLEANUP
```

`说明`
* RADON CLEANUP指令将会清理迁移完之后的旧数据。

```
mysql> radon cleanup;
Query OK, 0 rows affected (0.13 sec)
```

## RADON REBALANCE

`语法`
```
RADON REBALANCE
```

`说明`
* 如果radon运行了很长一段时间，用户会发现数据在后端节点之间分布不均衡。
* 这条指令旨让节点之间的数据重新均衡分布，每次从一个后端节点只迁移一张分区表到另一个后端节点，
内部机制主要分为两步：
    1. 从分片中获取重分布建议。
    2. 根据建议从源（后端）迁移分区表到目标（后端）。
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

* 注意: 如果执行了这条指令： `RADON REBALANCE`，客户端将会阻塞。在这期间，如果用户执行`ctrl+c` 或者退出客户端，数据重分布操作将会在后台继续进行，用户得在radon log日志中找到迁移日志信息，查到是否迁移完成，如果迁移完成，会展示以下信息（如下图），否则会发现失败的log信息。
```
  [WARNING]      rebalance.migrate.done...
```
