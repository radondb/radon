Table of Contents
=================

   * [DESCRIBE Statement](#describe-statement)
   * [EXPLAIN Statement](#explain-statement)
   * [USE Statement](#use-statement)

# DESCRIBE Statement
The DESCRIBE and EXPLAIN statements are synonyms, used either to obtain information about table structure or query execution plans. For more information, see [EXPLAIN Statement](#explain-statement).

# EXPLAIN Statement
`Syntax`
```
{EXPLAIN | DESCRIBE | DESC}
    tbl_name [col_name | wild]

{EXPLAIN | DESCRIBE | DESC}
    [explain_type]
    {explainable_stmt | FOR CONNECTION connection_id}

{EXPLAIN | DESCRIBE | DESC} ANALYZE [FORMAT = TREE] select_statement

explain_type: {
    EXTENDED
  | PARTITIONS
  | FORMAT = format_name
}

format_name: {
    TRADITIONAL
  | JSON
  | TREE
}

explainable_stmt: {
    SELECT statement
  | DELETE statement
  | INSERT statement
  | REPLACE statement
  | UPDATE statement
}
```

`Instructions`
* wild: if given, is a pattern string. It can contain the SQL % and _ wildcard characters.
* Now explain explainable_stmt will output radon's execute plans, not from MySQL.
* TABLE statement is supported from 8.0, we'll support it in the future.
* EXTENDED and PARTITIONS are abandoned from 8.0, we'll still parse them but won't use them.

`Example: `
Describe table infos:
```
mysql> create table t(c1 int key, c2 char(10), c3 varchar(100));
Query OK, 0 rows affected (1.31 sec)

mysql> desc t;
+-------+--------------+------+-----+---------+-------+
| Field | Type         | Null | Key | Default | Extra |
+-------+--------------+------+-----+---------+-------+
| c1    | int(11)      | NO   | PRI | NULL    |       |
| c2    | char(10)     | YES  |     | NULL    |       |
| c3    | varchar(100) | YES  |     | NULL    |       |
+-------+--------------+------+-----+---------+-------+
3 rows in set (0.00 sec)

mysql> desc t c1;
+-------+---------+------+-----+---------+-------+
| Field | Type    | Null | Key | Default | Extra |
+-------+---------+------+-----+---------+-------+
| c1    | int(11) | NO   | PRI | NULL    |       |
+-------+---------+------+-----+---------+-------+
1 row in set (0.01 sec)

mysql> desc t "c%";
+-------+--------------+------+-----+---------+-------+
| Field | Type         | Null | Key | Default | Extra |
+-------+--------------+------+-----+---------+-------+
| c1    | int(11)      | NO   | PRI | NULL    |       |
| c2    | char(10)     | YES  |     | NULL    |       |
| c3    | varchar(100) | YES  |     | NULL    |       |
+-------+--------------+------+-----+---------+-------+
3 rows in set (0.00 sec)
```

Get execution plans:
```
mysql> explain select * from t\G
*************************** 1. row ***************************
EXPLAIN: {
	"RawQuery": "explain select * from t",
	"Project": "*",
	"Partitions": [
		{
			"Query": "select * from testdb.t_0000 as t",
			"Backend": "backend1",
			"Range": "[0-64)"
		},
		{
			"Query": "select * from testdb.t_0001 as t",
			"Backend": "backend1",
			"Range": "[64-128)"
		},
		.........
		.........
		.........

mysql> explain delete from t where c=2\G
*************************** 1. row ***************************
EXPLAIN: {
	"RawQuery": "explain delete from t where c=2",
	"Partitions": [
		{
			"Query": "delete from testdb.t_0000 where c = 2",
			"Backend": "backend1",
			"Range": "[0-64)"
		},
		{
			"Query": "delete from testdb.t_0001 where c = 2",
			"Backend": "backend1",
			"Range": "[64-128)"
		},
		.......
		.......
		.......

mysql> explain insert into t(c1) values(1)\G
*************************** 1. row ***************************
EXPLAIN: {
	"RawQuery": "explain insert into t(c1) values(1)",
	"Partitions": [
		{
			"Query": "insert into testdb.t_0036(c1) values (1)",
			"Backend": "backend2",
			"Range": "[2304-2368)"
		}
	]
}
1 row in set (0.00 sec)
```

# USE Statement

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
