Table of Contents
=================

   * [Data Definition Statements](#data-definition-statements)
      * [ALTER Database Statement](#alter-database-statement)
      * [ALTER TABLE Statement](#alter-table-statement)
         * [Add  Column](#add--column)
         * [Change Table Engine](#change-table-engine)
         * [Change The Table Character Set](#change-the-table-character-set)
         * [Drop Column](#drop-column)
         * [Modify Column](#modify-column)
      * [CREATE DATABASE Statement](#create-database-statement)
      * [CREATE INDEX](#create-index)
      * [CREATE TABLE](#create-table)
      * [DROP DATABASE](#drop-database)
      * [DROP INDEX](#drop-index)
      * [DROP TABLE](#drop-table)
      * [TRUNCATE TABLE Statement](#truncate-table-statement)

# Data Definition Statements

## ALTER TABLE Statement
`Syntax`
```
ALTER {DATABASE | SCHEMA} [db_name]
    alter_option ...

alter_option: {
    [DEFAULT] CHARACTER SET [=] charset_name
  | [DEFAULT] COLLATE [=] collation_name
  | [DEFAULT] ENCRYPTION [=] {'Y' | 'N'}
  | READ ONLY [=] {DEFAULT | 0 | 1}
}
```

`Instructions`
* RadonDB sends the corresponding backend execution engine changes based on the routing information
* *Cross-partition non-atomic operations*
* radon completely support syntax with MySQL 8.0 and abandon "alter ... UPGRADE DATA DIRECTORY NAME" feature in 5.7.

`Example: `
1. alter with specify database.
```
mysql> create database testdb DEFAULT CHARSET=utf8 collate utf8_unicode_ci;
Query OK, 2 rows affected (0.02 sec)

mysql> show create database testdb;
+----------+-----------------------------------------------------------------------------------------+
| Database | Create Database                                                                         |
+----------+-----------------------------------------------------------------------------------------+
| testdb   | CREATE DATABASE `testdb` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci */ |
+----------+-----------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> alter database testdb default character set = utf8mb4 collate = utf8mb4_bin;
Query OK, 2 rows affected (0.01 sec)

mysql> show create database testdb;
+----------+----------------------------------------------------------------------------------------+
| Database | Create Database                                                                        |
+----------+----------------------------------------------------------------------------------------+
| testdb   | CREATE DATABASE `testdb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin */ |
+----------+----------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
2. Alter without specify database, use the current default session database.
```
mysql> alter /*current session has no db, no database selected*/ database default character set = utf8 collate = utf8_unicode_ci;
ERROR 1046 (3D000): No database selected
mysql> use testdb;
Database changed
mysql> alter /*use current session testdb*/ database default character set = utf8 collate = utf8_unicode_ci;
Query OK, 2 rows affected (0.02 sec)

mysql> show create database testdb;
+----------+-----------------------------------------------------------------------------------------+
| Database | Create Database                                                                         |
+----------+-----------------------------------------------------------------------------------------+
| testdb   | CREATE DATABASE `testdb` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci */ |
+----------+-----------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## ALTER TABLE Statement
`Syntax`
```
ALTER TABLE tbl_name alter_option

alter_option: {
    table_option
  | ADD COLUMN (col_name column_definition,...)
  | DROP COLUMN col_name
  | MODIFY COLUMN col_name column_definition

}

  table_option: {
    ENGINE [=] engine_name
  | CONVERT TO CHARACTER SET charset_name
}
```

`Instructions`
* RadonDB sends the corresponding backend execution engine changes based on the routing information
* *Cross-partition non-atomic operations*

### Add  Column

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

### Change Table Engine

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

### Change The Table Character Set
In RadonDB, the default character set is `UTF-8`.

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

### Drop Column

`Instructions`
* *Cannot delete the column where the partition key is located*

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

### Modify Column

`Instructions`
* *Cannot modify the column where the partition key is located*

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


## CREATE DATABASE Statement

`Syntax`
```
CREATE {DATABASE | SCHEMA} [IF NOT EXISTS] db_name
    [create_option] ...

create_option: [DEFAULT] {
    CHARACTER SET [=] charset_name
  | COLLATE [=] collation_name
  | ENCRYPTION [=] {'Y' | 'N'}
}
```
`Instructions`

* RadonDB will sends this statement directly to all backends to execute and return results.
* *Cross-partition non-atomic operations*
* The `ENCRYPTION` option, introduced in MySQL 8.0.16, defines the default database encryption. Inherited by tables created in the database.

`Example:`
```
mysql> CREATE DATABASE db_test1;
Query OK, 1 row affected (0.00 sec)
```
---------------------------------------------------------------------------------------------------

## CREATE INDEX

`Syntax`
```
CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX index_name
    ON tbl_name (key_part,...)
    [index_option]
    [index_lock_and_algorithm_opt]
	
key_part:
    col_name [(length)]

index_option:
    KEY_BLOCK_SIZE [=] value
  | index_type
  | WITH PARSER NGRAM
  | COMMENT 'string'

index_type:
    USING {BTREE | HASH}

index_lock_and_algorithm_opt:
    algorithm_option
|   lock_option
|   algorithm_option lock_option
|   lock_option algorithm_option

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
---------------------------------------------------------------------------------------------------

## CREATE TABLE

`Syntax`

```
CREATE TABLE [IF NOT EXISTS] tbl_name
    (create_definition,...)
    [table_options]
    [partition_options]

create_definition: {
    col_name column_definition
  | {INDEX | KEY} [index_name] [index_type] (key_part,...)
      [index_option] ...
  | {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)
      [index_option] ...
  | [CONSTRAINT [symbol]] PRIMARY KEY
      [index_type] (key_part,...)
      [index_option] ...
  | [CONSTRAINT [symbol]] UNIQUE [INDEX | KEY]
      [index_name] [index_type] (key_part,...)
      [index_option] ...
}

column_definition: {
    data_type [NOT NULL | NULL] [DEFAULT {literal | (expr)} ]
      [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY]
      [COMMENT 'string']
      [COLLATE collation_name]
      [COLUMN_FORMAT {FIXED | DYNAMIC | DEFAULT}]
      [STORAGE {DISK | MEMORY}]
}

key_part: {col_name [(length)] | (expr)} [ASC | DESC]

index_type:
    USING {BTREE | HASH}

index_option: {
    KEY_BLOCK_SIZE [=] value
  | index_type
  | WITH PARSER parser_name
  | COMMENT 'string'
}

table_options:
    table_option [[,] table_option] ...

table_option: {
    AUTO_INCREMENT [=] value
  | AVG_ROW_LENGTH [=] value
  | [DEFAULT] CHARACTER SET [=] charset_name
  | CHECKSUM [=] {0 | 1}
  | [DEFAULT] COLLATE [=] collation_name
  | COMMENT [=] 'string'
  | COMPRESSION [=] {'ZLIB' | 'LZ4' | 'NONE'}
  | CONNECTION [=] 'connect_string'
  | {DATA | INDEX} DIRECTORY [=] 'absolute path to directory'
  | DELAY_KEY_WRITE [=] {0 | 1}
  | ENCRYPTION [=] {'Y' | 'N'}
  | ENGINE [=] {InnoDB | TokuDB}
  | INSERT_METHOD [=] { NO | FIRST | LAST }
  | KEY_BLOCK_SIZE [=] value
  | MAX_ROWS [=] value
  | MIN_ROWS [=] value
  | PACK_KEYS [=] {0 | 1 | DEFAULT}
  | PASSWORD [=] 'string'
  | ROW_FORMAT [=] {DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT}
  | STATS_AUTO_RECALC [=] {DEFAULT | 0 | 1}
  | STATS_PERSISTENT [=] {DEFAULT | 0 | 1}
  | STATS_SAMPLE_PAGES [=] value
  | TABLESPACE tablespace_name [STORAGE {DISK | MEMORY}]
}

partition_options:
    PARTITION BY HASH(shard-key) [PARTITIONS num]
    | PARTITION BY LIST(shard-key)(PARTITION backend VALUES IN (value_list),...)
    | SINGLE
    | GLOBAL
    | DISTRIBUTED BY (backend-name)

```
With MySQL compatibility:
* radon not support `CREATE TEMPORARY TABLE` syntax
* not support `CREATE TABLE [AS] query_expression` and `CREATE TABLE LIKE`
* not support `check_constraint_definition` and `reference_definition`
* not support FOREIGN KEY

`Instructions`
* Create partition information and generate partition tables on each partition
* AUTO_INCREMENT table_option currently only supported at the grammatical level, the value will not take effect.
* With `GLOBAL` will create a global table. The global table has full data at every backend. The global tables are generally used for tables with fewer changes and smaller capacity, requiring frequent association with other tables.
* With `SINGLE` will create a single table. The single table only on the first backend.
* With `DISTRIBUTED BY (backend-name)` will create a single table. The single table is distributed on the specified backend `backend-name`.
* With `PARTITION BY HASH(shard-key)` will create a hash partition table. The partition mode is HASH, which is evenly distributed across the partitions according to the partition key `HASH value`, `PARTITIONS num` can specify the partition number.
* Without `PARTITION BY HASH(shard-key)|LIST(shard-key)|SINGLE|GLOBAL` will create a hash partition table. The table's `PRIMARY|UNIQUE KEY` is the partition key, only support one primary|unique key.
* With `PARTITION BY LIST(shard-key)` will create a list partition table. `PARTITION backend VALUES IN (value_list)` is one partition, The variable backend is one backend name, The variable value_list is values with `,`.
	* all expected values for the partitioning expression should be covered in `PARTITION ... VALUES IN (...)` clauses. An INSERT statement containing an unmatched partitioning column value fails with an error, as shown in this example:
```
	mysql> CREATE TABLE h2 (
	    ->   c1 INT,
	    ->   c2 INT
	    -> )
	    -> PARTITION BY LIST(c1) (
	    ->   PARTITION backend_name1 VALUES IN (1, 4, 7),
	    ->   PARTITION backend_name2 VALUES IN (2, 5, 8)
	    -> );
	Query OK, 0 rows affected (0.11 sec)
		
	mysql> INSERT INTO h2 VALUES (3, 5);
	ERROR 1525 (HY000): Table has no partition for value 3

  mysql> CREATE TABLE t5(id int, age int) DISTRIBUTED BY (backend1);
  Query OK, 0 rows affected (0.11 sec)
```

* The partitioning key only supports specifying one column, the data type of this column is not limited(
  except for TYPE `BINARY/NULL`)
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

mysql> CREATE TABLE t3(id int, age int) COMMENT 'HELLO RADON' SINGLE;
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
---------------------------------------------------------------------------------------------------

## DROP DATABASE

`Syntax`
```
 DROP {DATABASE | SCHEMA} [IF EXISTS] db_name
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

## DROP INDEX

`Syntax`
```
DROP INDEX index_name ON tbl_name index_lock_and_algorithm_opt

index_lock_and_algorithm_opt:
    algorithm_option
|   lock_option
|   algorithm_option lock_option
|   lock_option algorithm_option

algorithm_option:
    ALGORITHM [=] {DEFAULT | INPLACE | COPY}

lock_option:
    LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
```

`Instructions`
* RadonDB sends an drop index  operation to the appropriate backend based on routing information
* *Cross-partition non-atomic operations*

`Example: `
```
mysql> DROP INDEX idx_id_age ON t1;
Query OK, 0 rows affected (0.09 sec)
```

## DROP TABLE

`Syntax`
```
DROP {TABLE | TABLES} [IF EXISTS] table_name
```
---------------------------------------------------------------------------------------------------

`Instructions`

* Delete partition information and backend`s partition table
* *Cross-partition non-atomic operations*

`Example: `
```
mysql> DROP TABLE t1;
Query OK, 0 rows affected (0.05 sec)
```

## TRUNCATE TABLE Statement
`Syntax`
```
TRUNCATE TABLE table_name
```

`Instructions`

*Cross-partition non-atomic operations*

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
