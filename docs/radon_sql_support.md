Contents
=================

* [Radon SQL support](#radon-sql-support)
   * [Background](#background)
   * [Data Definition Statements](#data-definition-statements)
      * [DATABASE](#database)
         * [CREATE DATABASE](#create-database)
         * [DROP DATABASE](#drop-database)
      * [TABLE](#table)
         * [CREATE TABLE](#create-table)
         * [DROP TABLE](#drop-table)
         * [Change Table Engine](#change-table-engine)
         * [Change The Table Character Set](#change-the-table-character-set)
         * [TRUNCATE TABLE](#truncate-table)
      * [COLUMN OPERATION](#column-operation)
         * [Add  Column](#add--column)
         * [Drop Column](#drop-column)
         * [Modify Column](#modify-column)
      * [INDEX](#index)
         * [CREATE INDEX](#create-index)
         * [DROP INDEX](#drop-index)
   * [Data Manipulation Statements](#data-manipulation-statements)
      * [SELECT](#select)
      * [INSERT](#insert)
      * [DELETE](#delete)
      * [UPDATE](#update)
      * [REPLACE](#replace)
   * [Transactional and Locking Statements](#transactional-and-locking-statements)
      * [TRANSACTION](#transaction)
   * [Database Administration Statements](#database-administration-statements)
      * [SHOW](#show)
         * [SHOW ENGINES](#show-engines)
         * [SHOW DATABASES](#show-databases)
         * [SHOW TABLES](#show-tables)
         * [SHOW TABLE STATUS](#show-table-status)
         * [SHOW COLUMNS](#show-columns)
         * [SHOW CREATE TABLE](#show-create-table)
         * [SHOW PROCESSLIST](#show-processlist)
         * [SHOW VARIABLES](#show-variables)
      * [USE](#use)
         * [USE DATABASE](#use-database)
      * [KILL](#kill)
         * [KILL processlist_id](#kill-processlist_id)
      * [CHECKSUM](#checksum)
         * [CHECKSUM TABLE](#checksum-table)
      * [SET](#set)
    * [Full Text Search](#full-text-search)
      * [ngram Full Text Parser](#ngram-full-text-parser)
    * [Radon](#radon)
      * [RADON ATTACH](#radon-attach)
      * [RADON ATTACHLIST](#radon-attachlist)
      * [RADON DETACH](#radon-detach)
      * [RADON RESHARD](#radon-reshard) 
      * [RADON CLEANUP](#radon-cleanup)
    * [Others](#others)
      * [Using AUTO_INCREMENT](#using-auto-increment)

# Radon SQL support

## Background

On SQL syntax level, RadonDB Fully compatible with MySQL.

In most scenarios, the SQL implementation of RadonDB is a subset of MySQL, for better use and standardization.

RadonDB runs SQL statements in parallel, multiple processes work together simultaneously to run a single SQL statement. 

## Data Definition Statements

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
      ENGINE [=] {InnoDB|TokuDB}
    | AUTO_INCREMENT [=] value
    | [DEFAULT] {CHARSET | CHARACTER SET} [=] charset_name
    | COMMENT [=] 'string'
    | {PARTITION BY HASH(shard-key)|SINGLE|GLOBAL|DISTRIBUTED BY (backend-name)}
    | PARTITION BY LIST(shard-key)(PARTITION backend VALUES IN (value_list),...)
```

`Instructions`
* Create partition information and generate partition tables on each partition
* AUTO_INCREMENT table_option currently only supported at the grammatical level, the value will not take effect.
* With `GLOBAL` will create a global table. The global table has full data at every backend. The global tables are generally used for tables with fewer changes and smaller capacity, requiring frequent association with other tables.
* With `SINGLE` will create a single table. The single table only on the first backend.
* With `DISTRIBUTED BY (backend-name)` will create a single table. The single table is distributed on the specified backend `backend-name`.
* With `PARTITION BY HASH(shard-key)` will create a hash partition table. The partition mode is HASH, which is evenly distributed across the partitions according to the partition key `HASH value`
* Without `PARTITION BY HASH(shard-key)|LIST(shard-key)|SINGLE|GLOBAL` will create a hash partition table. The table's `PRIMARY|UNIQUE KEY` is the partition key, only support one primary|unique key.
* With `PARTITION BY LIST(shard-key)` will create a list partition table. `PARTITION backend VALUES IN (value_list)` is one partition, The variable backend is one backend name, The variable value_list is values with `,`.
	* all expected values for the partitioning expression should be covered in `PARTITION ... VALUES IN (...)` clauses. An INSERT statement containing an unmatched partitioning column value fails with an error, as shown in this example:
	```
	mysql> CREATE TABLE h2 (
	    ->   c1 INT,
	    ->   c2 INT
	    -> )
	    -> PARTITION BY LIST(c1) (
	    ->   PARTITION p0 VALUES IN (1, 4, 7),
	    ->   PARTITION p1 VALUES IN (2, 5, 8)
	    -> );
	Query OK, 0 rows affected (0.11 sec)
		
	mysql> INSERT INTO h2 VALUES (3, 5);
	ERROR 1525 (HY000): Table has no partition for value 3

  mysql> CREATE TABLE t5(id int, age int) DISTRIBUTED BY (backend1);
  Query OK, 0 rows affected (0.11 sec)
	```

* The partitioning key only supports specifying one column, the data type of this column is not limited(
  except for TYPE `BINARY/NULL`)
* table_options only support `ENGINE` `CHARSET` and `COMMENT`，Others are automatically ignored
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

mysql> show create table t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.051 sec)

mysql> CREATE TABLE t2(id int, age int) GLOBAL;
Query OK, 0 rows affected (1.80 sec)

mysql> show create table t2\G
*************************** 1. row ***************************
       Table: t2
Create Table: CREATE TABLE `t2` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!GLOBAL*/
1 row in set (0.047 sec)

mysql> CREATE TABLE t3(id int, age int) SINGLE COMMENT 'HELLO RADON';
Query OK, 0 rows affected (1.80 sec)

mysql> show create table t3\G
*************************** 1. row ***************************
       Table: t3
Create Table: CREATE TABLE `t3` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'HELLO RADON'
/*!SINGLE*/
1 row in set (0.093 sec)

mysql> CREATE TABLE t4(id int, age int);
ERROR 1105 (HY000): The unique/primary constraint shoule be defined or add 'PARTITION BY HASH' to mandatory indication
mysql> CREATE TABLE t4(id int, age int,primary key(id));
Query OK, 0 rows affected (1.110 sec)

mysql> show create table t4\G
*************************** 1. row ***************************
       Table: t4
Create Table: CREATE TABLE `t4` (
  `id` int(11) NOT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.094 sec)
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

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.046 sec)

mysql> ALTER TABLE t1 ENGINE=TokuDB;
Query OK, 0 rows affected (0.15 sec)

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=TokuDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.095 sec)
```

#### Change The Table Character Set

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

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.097 sec)

mysql> alter table t1 convert to character set utf8mb4;
Query OK, 0 rows affected (0.07 sec)

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.045 sec)
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

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL,
  `c` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.048 sec)
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

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.092 sec)

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

mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `b` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.049 sec)
mysql>  ALTER TABLE t1 MODIFY COLUMN id bigint;
ERROR 1105 (HY000): unsupported: cannot.modify.the.column.on.shard.key

```
---------------------------------------------------------------------------------------------------

### INDEX

RadonDB only supports the `CREATE/DROP INDEX` syntax in order to simplify the index operation.

#### CREATE INDEX

`Syntax`
```
CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX index_name
    ON tbl_name (key_part,...)
    [index_option]
    [algorithm_option | lock_option] ...
	
key_part:
    col_name [(length)]

index_option:
    KEY_BLOCK_SIZE [=] value
  | index_type
  | WITH PARSER NGRAM
  | COMMENT 'string'

index_type:
    USING {BTREE | HASH}

algorithm_option:
    ALGORITHM [=] {DEFAULT | INPLACE | COPY}

lock_option:
    LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
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

## Data Manipulation Statements
### SELECT

`Syntax`
```
SELECT
    [DISTINCT]
    select_expr [, select_expr ...]
    [FROM table_references
    [WHERE where_condition]
    [GROUP BY {col_name}
    [HAVING where_condition]
    [ORDER BY {col_name}
      [ASC | DESC], ...]
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]
```

`JOIN`
```
table_references:
    escaped_table_reference [, escaped_table_reference] ...
escaped_table_reference:
    table_reference
  | { OJ table_reference }
table_reference:
    table_factor
  | join_table
table_factor:
    [schema_name.]tbl_name [[AS] alias]
  | ( table_references )
join_table:
    table_reference [INNER | CROSS] JOIN table_factor [join_condition]
  | table_reference {LEFT|RIGHT} [OUTER] JOIN table_reference join_condition
join_condition:
    ON conditional_expr
```

`UNION`
``` 
SELECT ...
UNION [ALL | DISTINCT]
SELECT ...
[UNION [ALL | DISTINCT]
SELECT ...]
```

`Instructions`

 * Support cross-partition count, sum, avg, max, min and other aggregate functions, Aggregate functions only support for numeric values
 * Support cross-partition order by, group by, limit and other operations, *group by field must be in select_expr*
 * Group by suggest to be used with aggregation function, avoid using group by alone when returning non-`group by` fields.
 * Support complex queries such as joins.
 * Support where and having clause, having doesn't support aggregate function temporarily.
 * Support retrieving rows computed without reference to any table or specify `DUAL` as a dummy table name in situations where no tables are referenced. 
 * Support alias_name for column like `SELECT columna [[AS] alias] FROM mytable;`.
 * Support alias_name for table like `SELECT columna FROM tbl_name [[AS] alias];`.
 * Support LEFT|RIGHT OUTER and INNER|CROSS join.
 * `select *` is not recommended, especially in join statements.
 * Support UNION [ALL | DISTINCT].
 

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
|   23 |      23 |
|   13 |      13 |
|    3 |       3 |
|    1 |       1 |
+------+---------+
4 rows in set (1.048 sec)
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

SELECT with `JOIN`, the join statement that cannot be pushed down cannot have `*` in the `selectexpr`:
```
mysql> CREATE TABLE t1(id int, age int) partition by HASH(id);
Query OK, 0 rows affected (1.127 sec)

mysql> INSERT INTO t1(id, age) values(1, 22),(2,25),(3,22),(4,25);
Query OK, 4 row affected (0.197 sec)

mysql> select id, sum(id) from t2 group by id order by id desc limit 10;
+------+---------+
| id   | sum(id) |
+------+---------+
|   23 |      23 |
|   13 |      13 |
|    3 |       3 |
|    1 |       1 |
+------+---------+
4 rows in set (1.048 sec)

mysql> select * from t1 join t2 on t1.id=t2.id where t2.age=22;
+------+------+------+------+
| id   | age  | id   | age  |
+------+------+------+------+
|    3 |   22 |    3 |   22 |
+------+------+------+------+
1 row in set (1.082 sec)

mysql> select t1.id, t1.age,t2.id from t1 join t2 on t1.age=t2.age where t2.id > 10 order by t1.id;
+------+------+------+
| id   | age  | id   |
+------+------+------+
|    1 |   22 |   23 |
|    1 |   22 |   13 |
|    3 |   22 |   23 |
|    3 |   22 |   13 |
+------+------+------+
4 rows in set (1.056 sec)

mysql> select * from t1 join t2 on t1.age=t2.age where t2.id > 10 order by t1.id;
ERROR 1105 (HY000): unsupported: '*'.expression.in.cross-shard.query

mysql> select * from t1 union select * from t2 order by id limit 1;
+------+------+
| id   | age  |
+------+------+
|    1 |   25 |
+------+------+
1 row in set (1.012 sec)
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
 * *Does not support clauses*

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
## Transactional and Locking Statements
### Transaction
`Syntax`
```
BEGIN
COMMIT
ROLLBACK
```

``Instructions``
 * Multi-Statement Transaction
 * RadonDB twopc-enable must be enabled
 * RadonDB supports autocommit transaction for Single-Statement (twopc-enable ON)

`Example: `
```
mysql> create table txntbl(a int);
Query OK, 0 rows affected (0.01 sec)

mysql> begin;
Query OK, 0 rows affected (0.00 sec)

mysql> insert into txntbl(a) values(1),(2);
Query OK, 4 rows affected (0.00 sec)

mysql> select * from txntbl;
+------+
| a    |
+------+
|    1 |
|    2 |
+------+
2 rows in set (0.01 sec)

mysql> rollback;
Query OK, 0 rows affected (0.00 sec)

mysql> select * from txntbl;
Empty set (0.00 sec)

mysql> begin;
Query OK, 0 rows affected (0.00 sec)

mysql> insert into txntbl(a) values(1),(2);
Query OK, 4 rows affected (0.00 sec)

mysql> commit;
Query OK, 0 rows affected (0.00 sec)

mysql> select * from txntbl;
+------+
| a    |
+------+
|    1 |
|    2 |
+------+
2 rows in set (0.00 sec)

```


## Database Administration Statements
### SHOW

#### SHOW ENGINES

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

#### SHOW DATABASES

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

#### SHOW TABLES

`Syntax`
```
SHOW [FULL] TABLES
[FROM db_name]
[LIKE 'pattern' | WHERE expr]
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

#### SHOW TABLE STATUS

`Syntax`
```
SHOW TABLE STATUS
[FROM db_name]
```

`Instructions`
* If db_name is not specified, the table under the current DB is returned

`Example: `
```
mysql> show table status;
+--------+--------+---------+------------+------+----------------+-------------+-----------------+--------------+-----------+----------------+---------------------+---------------------+------------+-----------------+----------+----------------+---------+
| Name   | Engine | Version | Row_format | Rows | Avg_row_length | Data_length | Max_data_length | Index_length | Data_free | Auto_increment | Create_time         | Update_time         | Check_time | Collation       | Checksum | Create_options | Comment |
+--------+--------+---------+------------+------+----------------+-------------+-----------------+--------------+-----------+----------------+---------------------+---------------------+------------+-----------------+----------+----------------+---------+
| b      | InnoDB |      10 | Dynamic    |    6 |          16384 |       16384 |               0 |            0 |         0 |           NULL | 2018-12-24 08:26:24 | 2019-01-22 08:31:47 | NULL       | utf8_general_ci |     NULL |                |         |
| g      | InnoDB |      10 | Dynamic    |    1 |          16384 |       16384 |               0 |            0 |         0 |           NULL | 2018-12-24 08:26:24 | 2019-02-28 03:20:46 | NULL       | utf8_general_ci |     NULL |                          |         |
+--------+--------+---------+------------+------+----------------+-------------+-----------------+--------------+-----------+----------------+---------------------+---------------------+------------+-----------------+----------+----------------+---------+
2 rows in set (0.08 sec)
```

#### SHOW COLUMNS

`Syntax`

```
SHOW [FULL] {COLUMNS | FIELDS} 
FROM [db_name.]table_name
[LIKE 'pattern' | WHERE expr]
```

`Instructions`
* Get the column definitions of a table

`Example: `

```
mysql> CREATE TABLE t1(A INT PRIMARY KEY, B VARCHAR(10)) PARTITION BY HASH(A);
Query OK, 0 rows affected (0.52 sec)

mysql> SHOW COLUMNS FROM t1;
+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null | Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| A     | int(11)     | NO   | PRI | NULL    |       |
| B     | varchar(10) | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+
2 rows in set (0.03 sec)

mysql> SHOW FULL COLUMNS FROM t1 where `Key` = 'PRI';
+-------+---------+-----------+------+-----+---------+-------+---------------------------------+---------+
| Field | Type    | Collation | Null | Key | Default | Extra | Privileges                      | Comment |
+-------+---------+-----------+------+-----+---------+-------+---------------------------------+---------+
| A     | int(11) | NULL      | NO   | PRI | NULL    |       | select,insert,update,references |         |
+-------+---------+-----------+------+-----+---------+-------+---------------------------------+---------+
1 row in set (0.04 sec)
```

#### SHOW CREATE TABLE

`Syntax`
```
SHOW CREATE TABLE table_name
```

`Instructions`
* N/A

`Example: `
```
mysql> SHOW CREATE TABLE t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY HASH (id) */
1 row in set (0.094 sec)
```

#### SHOW PROCESSLIST

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

#### SHOW VARIABLES

`Syntax`
```
SHOW VARIABLES
    [LIKE 'pattern' | WHERE expr]
```

`Instructions`
* For compatibility JDBC/mydumper
* The SHOW VARIABLES command is sent to the backend partition MySQL (random partition) to get and return

### USE

#### USE DATABASE

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

### KILL

#### KILL processlist_id

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
### CHECKSUM

#### CHECKSUM TABLE

`Syntax`
```
CHECKSUM TABLE  [database_name.]table_name
```

`Instructions`
* Reports a checksum for the contents of a table
* RadonDB gives same result as MySQL

`Example: `

```
mysql> checksum table test.t1;
+----------+------------+
| Table    | Checksum   |
+----------+------------+
| test.t1  | 2464930879 |
+----------+------------+
1 row in set (0.00 sec)
```

### SET

`Instructions`
* For compatibility JDBC/mydumper
* SET is an empty operation, *all operations will not take effect*, do not use it directly。

## Full Text Search
###  ngram Full Text Parser

`Instructions`
* RadonDB supports Full-Text Search, provides an ngram full-text parser that supports Chinese, Japanese, and Korean (CJK).
* RadonDB Full-Text tables are partitioned (MySQL Partitioned tables do not support FULLTEXT indexes or searches), and query runs ` in parallel`.

`Example: `

```
mysql>CREATE TABLE `articles` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(200) DEFAULT NULL,
  `body` text,
  PRIMARY KEY (`id`),
  FULLTEXT INDEX `ngram_idx` (`title`,`body`) WITH PARSER ngram
) ENGINE=InnoDB PARTITION BY HASH(id);

mysql>INSERT INTO articles (title,body) VALUES
    ('数据库管理','在本教程中我将向你展示如何管理数据库'),
    ('数据库应用开发','学习开发数据库应用程序');


SELECT title from articles  WHERE MATCH (title, body) AGAINST ('数据库' IN BOOLEAN MODE);
+-----------------------+
| title                 |
+-----------------------+
| 数据库应用开发        |
| 数据库管理            |
+-----------------------+
2 rows in set (0.04 sec)
```

## Radon
### RADON ATTACH

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

### RADON ATTACHLIST

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

### RADON DETACH

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

### RADON RESHARD

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

### RADON CLEANUP

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

## Others
###  Using AUTO INCREMENT

`Instructions`
* RadonDB employs its own unique identity by golang's UnixNano().
* AUTO_INCREMENT field must be BIGINT.

`Example: `

```
mysql> CREATE TABLE animals (
    ->      id BIGINT NOT NULL AUTO_INCREMENT,
    ->      name CHAR(30) NOT NULL,
    ->      PRIMARY KEY (id)
    -> ) PARTITION BY HASH(id);
Query OK, 0 rows affected (0.14 sec)

mysql> INSERT INTO animals (name) VALUES
    ->     ('dog'),('cat'),('penguin'),
    ->     ('lax'),('whale'),('ostrich');
Query OK, 6 rows affected (0.01 sec)

mysql> SELECT * FROM animals;
+---------------------+---------+
| id                  | name    |
+---------------------+---------+
| 1553090617754346084 | lax     |
| 1553090617754346082 | cat     |
| 1553090617754346085 | whale   |
| 1553090617754346081 | dog     |
| 1553090617754346083 | penguin |
| 1553090617754346086 | ostrich |
+---------------------+---------+
6 rows in set (0.02 sec)
```
