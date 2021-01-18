Table of Contents
=================

   * [Database Administration Statements](#database-administration-statements)
      * [SET](#set)
      * [SHOW](#show)
         * [SHOW CHARSET](#show-charset)
         * [SHOW COLLATION](#show-collation)
         * [SHOW ENGINES](#show-engines)
         * [SHOW DATABASES](#show-databases)
         * [SHOW TABLES](#show-tables)
         * [SHOW TABLE STATUS](#show-table-status)
         * [SHOW COLUMNS](#show-columns)
         * [SHOW CREATE TABLE](#show-create-table)
         * [SHOW INDEX](#show-index)
         * [SHOW PROCESSLIST](#show-processlist)
         * [SHOW VARIABLES](#show-variables)
      * [Table Maintenance Statements](#table-maintenance-statements)
         * [CHECKSUM TABLE Statements](#checksum-table-statements)
         * [OPTIMIZE TABLE Statements](#optimize-table-statements)
      * [Other Administrative Statements](#other-administrative-statements)
         * [KILL Statement](#kill-statement)

# Database Administration Statements

## SET

`Instructions`
* For compatibility JDBC/mydumper
* SET is an empty operation, *all operations will not take effect*, do not use it directly。

## SHOW

### SHOW CHARSET

`Syntax`
```
SHOW CHARSET
```

`Instructions`
* This statement lists all available character sets.

`Example: `
```
mysql> show charset;
+----------+---------------------------------+---------------------+--------+
| Charset  | Description                     | Default collation   | Maxlen |
+----------+---------------------------------+---------------------+--------+
| big5     | Big5 Traditional Chinese        | big5_chinese_ci     |      2 |
| dec8     | DEC West European               | dec8_swedish_ci     |      1 |
| cp850    | DOS West European               | cp850_general_ci    |      1 |
| hp8      | HP West European                | hp8_english_ci      |      1 |
| koi8r    | KOI8-R Relcom Russian           | koi8r_general_ci    |      1 |
| latin1   | cp1252 West European            | latin1_swedish_ci   |      1 |
| latin2   | ISO 8859-2 Central European     | latin2_general_ci   |      1 |
| swe7     | 7bit Swedish                    | swe7_swedish_ci     |      1 |
... ...
41 rows in set (0.02 sec)
```

### SHOW COLLATION

`Syntax`
```
SHOW COLLATION
```

`Instructions`
* This statement lists collations supported by the server.

`Example: `
```
mysql> SHOW COLLATION;
+--------------------------+----------+-----+---------+----------+---------+
| Collation                | Charset  | Id  | Default | Compiled | Sortlen |
+--------------------------+----------+-----+---------+----------+---------+
| big5_chinese_ci          | big5     |   1 | Yes     | Yes      |       1 |
| big5_bin                 | big5     |  84 |         | Yes      |       1 |
| dec8_swedish_ci          | dec8     |   3 | Yes     | Yes      |       1 |
| dec8_bin                 | dec8     |  69 |         | Yes      |       1 |
| cp850_general_ci         | cp850    |   4 | Yes     | Yes      |       1 |
| cp850_bin                | cp850    |  80 |         | Yes      |       1 |
| hp8_english_ci           | hp8      |   6 | Yes     | Yes      |       1 |
| hp8_bin                  | hp8      |  72 |         | Yes      |       1 |
| koi8r_general_ci         | koi8r    |   7 | Yes     | Yes      |       1 |
... ...
222 rows in set (0.05 sec)
```

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

### SHOW TABLE STATUS

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

### SHOW COLUMNS

`Syntax`

```
SHOW [FULL] {COLUMNS | FIELDS}
    {FROM | IN} tbl_name
    [{FROM | IN} db_name]
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

### SHOW CREATE TABLE

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

### SHOW INDEX

`Syntax`
```
SHOW {INDEX | INDEXES | KEYS}
    {FROM | IN} tbl_name
    [{FROM | IN} db_name]
    [WHERE expr]
```

`Instructions`
* Get the table index information.

`Example: `
```
mysql> CREATE TABLE t1(A INT PRIMARY KEY, B VARCHAR(10)) PARTITION BY HASH(A);
Query OK, 0 rows affected (2.20 sec)

mysql> show index from t1\G
*************************** 1. row ***************************
        Table: t1
   Non_unique: 0
     Key_name: PRIMARY
 Seq_in_index: 1
  Column_name: A
    Collation: A
  Cardinality: 0
     Sub_part: NULL
       Packed: NULL
         Null: 
   Index_type: BTREE
      Comment: 
Index_comment: 
1 row in set (0.05 sec)
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

## Table Maintenance Statements

### CHECKSUM TABLE Statements

`Syntax`
```
CHECKSUM {TABLE | TABLES} tbl_name [, tbl_name] ... [QUICK | EXTENDED]
```

`Instructions`
* Reports a checksum for the contents of a table
* RadonDB gives same result as MySQL

`Example: `

```
mysql> checksum tables t1, t extended;
+---------+------------+
| Table   | Checksum   |
+---------+------------+
| test.t1 | 1910461541 |
| test.t  | 2643913285 |
+---------+------------+
2 rows in set (0.00 sec)

mysql> checksum tables t1;
+---------+------------+
| Table   | Checksum   |
+---------+------------+
| test.t1 | 1910461541 |
+---------+------------+
1 row in set (0.00 sec)

mysql> checksum /*db not exsit*/ tables t1, db.t;
+---------+------------+
| Table   | Checksum   |
+---------+------------+
| test.t1 | 1910461541 |
| db.t    |       NULL |
+---------+------------+
2 rows in set (0.00 sec)

mysql> create table t2(a int key, b int);
insertQuery OK, 0 rows affected (1.20 sec)

mysql> insert into t2(a,b) values (1,2),(3,4);
Query OK, 2 rows affected (0.01 sec)

mysql> checksum tables t,t1,t2 quick;
+---------+------------+
| Table   | Checksum   |
+---------+------------+
| test.t  | NULL       |
| test.t1 | NULL       |
| test.t2 | NULL       |
+---------+------------+
3 rows in set (0.03 sec)
```

### OPTIMIZE TABLE Statements
`Syntax`
```
OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL]
    {TABLE | TABLES} tbl_name [, tbl_name] ...
```

`Instructions`
* Reports a checksum for the contents of a table
* RadonDB gives same result as MySQL

`Example: `
1. optimize global table
```
mysql> create table t_global(a int, b char) global;
Query OK, 0 rows affected (0.06 sec)

mysql> optimize local tables t_global;
+---------------+----------+----------+-------------------------------------------------------------------+
| Table         | Op       | Msg_type | Msg_text                                                          |
+---------------+----------+----------+-------------------------------------------------------------------+
| test.t_global | optimize | status   | OK                                                                |
| test.t_global | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_global | optimize | status   | OK                                                                |
| test.t_global | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
+---------------+----------+----------+-------------------------------------------------------------------+
4 rows in set (0.04 sec)
```

2. optimize single table
```
mysql> create table t_single(a int, b char) single;
Query OK, 0 rows affected (0.05 sec)

mysql> optimize local tables t_single;
+---------------+----------+----------+-------------------------------------------------------------------+
| Table         | Op       | Msg_type | Msg_text                                                          |
+---------------+----------+----------+-------------------------------------------------------------------+
| test.t_single | optimize | status   | OK                                                                |
| test.t_single | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
+---------------+----------+----------+-------------------------------------------------------------------+
2 rows in set (0.04 sec)
```

3. optimize partition table
```
mysql> create table t_part(a int key, b char);
Query OK, 0 rows affected (0.05 sec)

mysql> optimize local tables t_part;
+------------------+----------+----------+-------------------------------------------------------------------+
| Table            | Op       | Msg_type | Msg_text                                                          |
+------------------+----------+----------+-------------------------------------------------------------------+
| test.t_part_0000 | optimize | status   | OK                                                                |
| test.t_part_0000 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_part_0001 | optimize | status   | OK                                                                |
| test.t_part_0001 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_part_0002 | optimize | status   | OK                                                                |
....
....
| test.t_part_0062 | optimize | status   | OK                                                                |
| test.t_part_0062 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_part_0063 | optimize | status   | OK                                                                |
| test.t_part_0063 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
+------------------+----------+----------+-------------------------------------------------------------------+
128 rows in set (1.65 sec)
```

4. optimize list table
```
mysql> create /*test partition list*/ table t_list(c1 int, c2 int) ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by list(c1) (partition backend1 values in (1,3,7), partition backend2 values in (2,5,8));
Query OK, 0 rows affected (0.14 sec)

mysql> optimize table t_list;
+------------------+----------+----------+-------------------------------------------------------------------+
| Table            | Op       | Msg_type | Msg_text                                                          |
+------------------+----------+----------+-------------------------------------------------------------------+
| test.t_list_0000 | optimize | status   | OK                                                                |
| test.t_list_0000 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_list_0001 | optimize | status   | OK                                                                |
| test.t_list_0001 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_list_0002 | optimize | status   | OK                                                                |
| test.t_list_0002 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_list_0003 | optimize | status   | OK                                                                |
| test.t_list_0003 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_list_0004 | optimize | status   | OK                                                                |
| test.t_list_0004 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
| test.t_list_0005 | optimize | status   | OK                                                                |
| test.t_list_0005 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
+------------------+----------+----------+-------------------------------------------------------------------+
12 rows in set (0.34 sec)
```

## Other Administrative Statements

### KILL Statement

`Syntax`
```
KILL [CONNECTION | QUERY] processlist_id
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
