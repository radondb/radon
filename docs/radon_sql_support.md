Contents
=================

* [Radon SQL support](#radon-sql-support)
   * [Background](#background)
   * [DDL](#ddl)
      * [DATABASE](#database)
         * [CREATE DATABASE](#create-database)
         * [DROP DATABASE](#drop-database)
      * [TABLE](#table)
         * [CREATE TABLE](#create-table)
         * [DROP TABLE](#drop-table)
         * [Change Table Engine](#change-table-engine)
         * [Change the table character set](#change-the-table-character-set)
         * [TRUNCATE TABLE](#truncate-table)
      * [COLUMN OPERATION](#column-operation)
         * [Add  Column](#add--column)
         * [Drop Column](#drop-column)
         * [Modify Column](#modify-column)
      * [INDEX](#index)
         * [ADD INDEX](#add-index)
         * [DROP INDEX](#drop-index)
   * [DML](#dml)
      * [SELECT](#select)
      * [INSERT](#insert)
      * [DELETE](#delete)
      * [UPDATE](#update)
      * [REPLACE](#replace)
   * [SHOW](#show)
      * [SHOW ENGINES](#show-engines)
      * [SHOW DATABASES](#show-databases)
      * [SHOW TABLES](#show-tables)
      * [SHOW CREATE TABLE](#show-create-table)
      * [SHOW PROCESSLIST](#show-processlist)
      * [SHOW VARIABLES](#show-variables)
   * [USE](#use)
      * [USE DATABASE](#use-database)
   * [KILL](#kill)
      * [KILL processlist_id](#kill-processlist_id)
   * [SET](#set)

# Radon SQL support

## Background

On SQL syntax level, RadonDB Fully compatible with MySQL.

In most scenarios, the SQL implementation of RadonDB is a subset of MySQL, for better use and standardization.

## DDL

### DATABASE

Based on database, RadonDB now  only supports `CREATE` and `DELETE` operation.

#### CREATE DATABASE

`Syntax`
```
 CREATE DATABASE [IF NOT EXISTS] db_name
```
`Instructions`

* RadonDB will sends this statement directly to all backends to execute and return results.
* *Cross-partition non-atomic operations*

`Example:`
```
mysql> CREATE DATABASE db_test1;
Query OK, 1 row affected (0.00 sec)
```

#### DROP DATABASE

`Syntax`
```
 DROP DATABASE [IF EXISTS] db_name
```

`Instructions`

* RadonDB will sends this statement directly to all backends to execute and return results.
* *Cross-partition non-atomic operations*

`Example `
```
mysql> DROP DATABASE db_test1;
Query OK, 0 rows affected (0.01 sec)
```
---------------------------------------------------------------------------------------------------

### TABLE

#### CREATE TABLE

`Syntax`
```
 CREATE TABLE [IF NOT EXISTS] table_name
    (create_definition,...)
    [ENGINE={InnoDB|TokuDB}]
    [DEFAULT CHARSET=(charset)]
    PARTITION BY HASH(shard-key)
```

`Instructions`
* Create partition information and generate partition tables on each partition
* Partition table syntax should include`PARTITION BY HASH(partition key)`
* The partitioning key only supports specifying one column, the data type of this column is not limited(
  except for TYPE `BINARY/NULL`)
* The partition mode is HASH, which is evenly distributed across the partitions according to the partition key
 `HASH value`
* table_options only support `ENGINE` and `CHARSET`，Others are automatically ignored
* The default engine for partition table is `InnoDB`
* The default character set for partition table `UTF-8`
* Does not support PRIMARY/UNIQUE constraints for non-partitioned keys, returning errors directly
* *Cross-partition non-atomic operations*

`Example:`
```
mysql> CREATE DATABASE db_test1;
Query OK, 1 row affected (0.00 sec)

mysql> USE db_test1;

Database changed
mysql> CREATE TABLE t1(id int, age int) PARTITION BY HASH(id);
Query OK, 0 rows affected (1.80 sec)
```

#### DROP TABLE

`Syntax`
```
DROP TABLE [IF EXISTS] table_name
```

`Instructions`

* Delete partition information and backend`s partition table
* *Cross-partition non-atomic operations*

`Example: `
```
mysql> DROP TABLE t1;
Query OK, 0 rows affected (0.05 sec)
```

#### Change Table Engine

`Syntax`
```
ALTER TABLE ... ENGINE={InnoDB|TokuDB...}
```

`Instructions`
* RadonDB sends the corresponding backend execution engine changes based on the routing information
* *Cross-partition non-atomic operations*

`Example: `

```
mysql> CREATE TABLE t1(id int, age int) PARTITION BY HASH(id);
Query OK, 0 rows affected (1.76 sec)

mysql> SHOW CREATE TABLE t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
1 row in set (0.00 sec)

mysql> ALTER TABLE t1 ENGINE=TokuDB;
Query OK, 0 rows affected (0.15 sec)

mysql> SHOW CREATE TABLE t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=TokuDB DEFAULT CHARSET=utf8
1 row in set (0.00 sec)
```

#### Change the table character set

In RadonDB, the default character set is `UTF-8`.

`Syntax`
```
ALTER TABLE table_name CONVERT TO CHARACTER SET {charset}
```

`Instructions`
* RadonDB sends the corresponding backend execution engine changes based on the routing information
* *Cross-partition non-atomic operations*

`Example: `

```
mysql> create table t1(id int, b int) partition by hash(id);
Query OK, 0 rows affected (0.15 sec)

mysql> show create table t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
1 row in set (0.00 sec)

mysql> alter table t1 convert to character set utf8mb4;
Query OK, 0 rows affected (0.07 sec)

mysql> show create table t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
1 row in set (0.00 sec)
```

#### TRUNCATE TABLE
`Syntax`
```
TRUNCATE TABLE table_name
```

`Instructions`

* *Cross-partition non-atomic operations*

`Example: `
```
mysql> insert into t1(id) values(1);
Query OK, 1 row affected (0.01 sec)

mysql> select * from t1;
+------+------+
| id   | age  |
+------+------+
|    1 | NULL |
+------+------+
1 row in set (0.01 sec)

mysql> truncate table t1;
Query OK, 0 rows affected (1.21 sec)

mysql> select * from t1;
Empty set (0.01 sec)
```
---------------------------------------------------------------------------------------------------

### COLUMN OPERATION

#### Add  Column
`Syntax`
```
ALTER TABLE table_name ADD COLUMN (col_name column_definition,...)
```

`Instructions`
* Add new columns to the table
* *Cross-partition non-atomic operations*

`Example: `

```
mysql> ALTER TABLE t1 ADD COLUMN (b int, c varchar(100));
Query OK, 0 rows affected (2.94 sec)

mysql> SHOW CREATE TABLE t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL,
  `c` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
1 row in set (0.01 sec)
```

#### Drop Column

`Syntax`
```
ALTER TABLE table_name DROP COLUMN col_name
```

`Instructions`
*  drop column from  table
* *Cannot delete the column where the partition key is located*
* *Cross-partition non-atomic operations*

`Example: `

```
mysql>  ALTER TABLE t1 DROP COLUMN c;
Query OK, 0 rows affected (2.92 sec)

mysql> SHOW CREATE TABLE t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
1 row in set (0.00 sec)

mysql>  ALTER TABLE t1 DROP COLUMN id;
ERROR 1105 (HY000): unsupported: cannot.drop.the.column.on.shard.key
```

#### Modify Column

`Syntax`
```
ALTER TABLE table_name MODIFY COLUMN col_name column_definition
```

`Instructions`
* Modify the column definition from table 
* *Cannot modify the column where the partition key is located*
* *Cross-partition non-atomic operations*

`Example: `

```
mysql> ALTER TABLE t1 MODIFY COLUMN b bigint;
Query OK, 0 rows affected (4.09 sec)

mysql> SHOW CREATE TABLE t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
1 row in set (0.00 sec)
mysql>  ALTER TABLE t1 MODIFY COLUMN id bigint;
ERROR 1105 (HY000): unsupported: cannot.modify.the.column.on.shard.key

```
---------------------------------------------------------------------------------------------------

### INDEX

RadonDB only supports the `CREATE/DROP INDEX` syntax in order to simplify the index operation.

#### ADD INDEX

`Syntax`
```
CREATE INDEX index_name ON table_name (index_col_name,...)
```

`Instructions`
* RadonDB sends the index to the corresponding backend based on the routing information.
* *Cross-partition non-atomic operations*

`Example: `
```
mysql> CREATE INDEX idx_id_age ON t1(id, age);
Query OK, 0 rows affected (0.17 sec)
```

#### DROP INDEX

`Syntax`
```
  DROP INDEX index_name ON table_name
```

`Instructions`
* RadonDB sends an drop index  operation to the appropriate backend based on routing information
* *Cross-partition non-atomic operations*

`Example: `
```
mysql> DROP INDEX idx_id_age ON t1;
Query OK, 0 rows affected (0.09 sec)
```

## DML
### SELECT

`Syntax`
```
SELECT
    select_expr [, select_expr ...]
    [FROM table_references
    [WHERE where_condition]
    [GROUP BY {col_name}
    [ORDER BY {col_name | expr | position}
      [ASC | DESC], ...]
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]]
```

`Instructions`

 * Support cross-partition count, sum, avg, max, min and other aggregate functions, *avg field must be in select_expr*, Aggregate  functions only support for numeric values
 * Support cross-partition order by, group by, limit and other operations, *field must be in select_expr*
 * Support complex queries such as joins, automatic routing to AP-Nodes to execute and return
 * Support retrieving rows computed without reference to any table or specify `DUAL` as a dummy table name in situations where no tables are referenced. 
 * Support alias_name for column like `SELECT columna [[AS] alias] FROM mytable;`.
 * Support alias_name for table like `SELECT columna FROM tbl_name [[AS] alias];`.
 

`Example: `
```
mysql> CREATE TABLE t2(id int, age int) partition by HASH(id);
Query OK, 0 rows affected (1.78 sec)

mysql> INSERT INTO t2(id, age) values(1, 25);
Query OK, 1 row affected (0.01 sec)

mysql> INSERT INTO t2(id, age) values(3, 22);
Query OK, 1 row affected (0.01 sec)

mysql> INSERT INTO t2(id, age) values(13, 22);
Query OK, 1 row affected (0.02 sec)

mysql> INSERT INTO t2(id, age) values(23, 22);
Query OK, 1 row affected (0.00 sec)

mysql> select id, sum(id) from t2 group by id order by id desc limit 10;
+------+---------+
| id   | sum(id) |
+------+---------+
|    1 |       1 |
|    3 |       3 |
|   13 |      13 |
|   23 |      23 |
+------+---------+
4 rows in set (0.01 sec)
```


SELECT can be used to retrieve rows computed without reference to any table:

```
mysql> select 1 + 1;
+-------+
| 1 + 1 |
+-------+
|     2 |
+-------+
1 row in set (0.00 sec)
```

Specify `DUAL` as a dummy table name in situations where no tables are referenced:

```
mysql> select date_format(now(),'%y-%m-%d') FROM DUAL;
+-------------------------------+
| date_format(now(),'%y-%m-%d') |
+-------------------------------+
| 18-06-18                      |
+-------------------------------+
1 row in set (0.00 sec)
```

SELECT with alias, `AS` is optional:

```
mysql> select id ID from t2 testTbl;
+------+
| ID   |
+------+
|    3 |
|   23 |
|    1 |
|   13 |
+------+
4 rows in set (0.02 sec)
```

```
mysql> select testTbl.id as ID from t2 as testTbl;
+------+
| ID   |
+------+
|    3 |
|   23 |
|    1 |
|   13 |
+------+
4 rows in set (0.02 sec)
```

### INSERT

`Syntax`
``` 
INSERT INTO tbl_name
    (col_name,...)
    {VALUES | VALUE}
```

`Instructions`
 * Support distributed transactions to ensure cross-partition write atomicity
 * Support insert multiple values, these values can be in different partitions
 * Must specify the write column
 *  *Does not support clauses*

`Example: `
```
mysql> INSERT INTO t2(id, age) VALUES(1, 24), (2, 28), (3, 29);
Query OK, 3 rows affected (0.01 sec)
```

### DELETE

`Syntax`
```
DELETE  FROM tbl_name
    [WHERE where_condition]
```

``Instructions``
 * Support distributed transactions to ensure that atomicity is removed across partitions
 *  *Does not support delete without WHERE condition*
 *  *Does not support clauses*

`Example: `
```
mysql> DELETE FROM t1 WHERE id=1;
Query OK, 2 rows affected (0.01 sec)
```

### UPDATE

`Syntax`
```
UPDATE table_reference
    SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
    [WHERE where_condition]
```

`Instructions`
 * Supports distributed transactions to ensure atomicity across partitions
 * *Does not support WHERE-less condition updates*
 * *Does not support updating partition key*
 *  *Does not support clauses*

`Example: `
```
mysql> UPDATE t1 set age=age+1 WHERE id=1;
Query OK, 1 row affected (0.00 sec)
```
### REPLACE

`Syntax`
```
REPLACE INTO tbl_name
    [(col_name,...)]
    {VALUES | VALUE} ({expr | DEFAULT},...),(...),...
```

`Instructions`
 * Support distributed transactions to ensure cross-partition write atomicity
 * Support replace multiple values, these values can be in different partitions
 * Must specify write column

`Example: `
```
mysql> REPLACE INTO t2 (id, age) VALUES(3,34),(5, 55);
Query OK, 2 rows affected (0.01 sec)
```

## SHOW

### SHOW ENGINES

`Syntax`
```
SHOW ENGINES
```

`Instructions`
* Backend partitioned supported engine list by MySQL

`Example: `
```

mysql> SHOW ENGINES;
+--------------------+---------+----------------------------------------------------------------------------+--------------+------+------------+
| Engine             | Support | Comment                                                                    | Transactions | XA   | Savepoints |
+--------------------+---------+----------------------------------------------------------------------------+--------------+------+------------+
| MyISAM             | YES     | MyISAM storage engine                                                      | NO           | NO   | NO         |
| MRG_MYISAM         | YES     | Collection of identical MyISAM tables                                      | NO           | NO   | NO         |
| InnoDB             | DEFAULT | Percona-XtraDB, Supports transactions, row-level locking, and foreign keys | YES          | YES  | YES        |
| BLACKHOLE          | YES     | /dev/null storage engine (anything you write to it disappears)             | NO           | NO   | NO         |
| CSV                | YES     | CSV storage engine                                                         | NO           | NO   | NO         |
| PERFORMANCE_SCHEMA | YES     | Performance Schema                                                         | NO           | NO   | NO         |
| ARCHIVE            | YES     | Archive storage engine                                                     | NO           | NO   | NO         |
| TokuDB             | YES     | Percona TokuDB Storage Engine with Fractal Tree(tm) Technology             | YES          | YES  | YES        |
| FEDERATED          | NO      | Federated MySQL storage engine                                             | NULL         | NULL | NULL       |
| MEMORY             | YES     | Hash based, stored in memory, useful for temporary tables                  | NO           | NO   | NO         |
+--------------------+---------+----------------------------------------------------------------------------+--------------+------+------------+
10 rows in set (0.00 sec)
```

### SHOW DATABASES

`Syntax`
```
SHOW DATABASES
```

`Instructions`
* Including system DB, such as mysql, information_schema

`Example: `
```
mysql> SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| db_gry_test        |
| db_test1           |
| mysql              |
| performance_schema |
| sys                |
+--------------------+
6 rows in set (0.01 sec)
```

### SHOW TABLES

`Syntax`
```
SHOW TABLES
[FROM db_name]
```

`Instructions`
* If db_name is not specified, the table under the current DB is returned

`Example: `
```
mysql> SHOW TABLES;
+--------------------+
| Tables_in_db_test1 |
+--------------------+
| t1                 |
| t2                 |
+--------------------+
2 rows in set (0.01 sec)
```

### SHOW CREATE TABLE

`Syntax`
```
SHOW CREATE TABLE table_name
```

`Instructions`
* N/A

`Example: `
```
mysql> SHOW CREATE TABLE t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` bigint(20) DEFAULT NULL,
  KEY `idx_id_age` (`id`,`age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
1 row in set (0.01 sec)
```

### SHOW PROCESSLIST

`Syntax`
```
SHOW PROCESSLIST
```

`Instructions`
* Shows the connection from client to RadonDB, not the backend partition MySQL

`Example: `
```
mysql> SHOW PROCESSLIST;
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
| Id   | User | Host            | db       | Command | Time | State | Info | Rows_sent | Rows_examined |
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
|    1 | root | 127.0.0.1:56984 | db_test1 | Sleep   |  794 |       |      |         0 |             0 |
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
1 row in set (0.00 sec)
```

### SHOW VARIABLES

`Syntax`
```
SHOW VARIABLES
    [LIKE 'pattern' | WHERE expr]
```

`Instructions`
* For compatibility JDBC/mydumper
* The SHOW VARIABLES command is sent to the backend partition MySQL (random partition) to get and return

## USE

### USE DATABASE

`Syntax`
```
USE db_name
```

`Instructions`
* Switch the database of the current session

`Example: `
```
mysql> use db_test1;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
```

## KILL

### KILL processlist_id

`Syntax`
```
KILL processlist_id
```

`Instructions`
* Kill a link (including terminating the executing statement)

`Example: `

```
mysql> show processlist;
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
| Id   | User | Host            | db       | Command | Time | State | Info | Rows_sent | Rows_examined |
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
|    2 | root | 127.0.0.1:38382 | db_test1 | Sleep   |  197 |       |      |         0 |             0 |
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
1 row in set (0.00 sec)

mysql> kill 2;
ERROR 2013 (HY000): Lost connection to MySQL server during query

mysql> show processlist;
ERROR 2006 (HY000): MySQL server has gone away
No connection. Trying to reconnect...
Connection id:    3
Current database: db_test1

+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
| Id   | User | Host            | db       | Command | Time | State | Info | Rows_sent | Rows_examined |
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
|    3 | root | 127.0.0.1:38516 | db_test1 | Sleep   |    0 |       |      |         0 |             0 |
+------+------+-----------------+----------+---------+------+-------+------+-----------+---------------+
1 row in set (0.00 sec)

```

## SET

`Instructions`
* For compatibility JDBC/mydumper
* SET is an empty operation, *all operations will not take effect*, do not use it directly。

