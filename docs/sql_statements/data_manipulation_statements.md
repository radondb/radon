Table of Contents
=================

   * [Data Manipulation Statements](#data-manipulation-statements)
      * [DELETE Statement](#delete-statement)
      * [DO Statement](#do-statement)
      * [INSERT](#insert)
      * [REPLACE](#replace)
      * [SELECT](#select)
      * [UPDATE](#update)

# Data Manipulation Statements

## DELETE Statement

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

## DO Statement
`Syntax`
```
DO expr [, expr] ...
```

`Example: `
```
mysql> do 1;
Query OK, 0 rows affected (0.01 sec)

mysql> do 2
    -> ;
Query OK, 0 rows affected (0.00 sec)

mysql> do 1 > 2, 1&2;
Query OK, 0 rows affected (0.00 sec)
```

## INSERT

`Syntax`
```
INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
    [INTO] tbl_name
    [PARTITION (partition_name [, partition_name] ...)]
    [(col_name [, col_name] ...)]
    {VALUES | VALUE} (value_list) [, (value_list)] ...
    [ON DUPLICATE KEY UPDATE assignment_list]

INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
    [INTO] tbl_name
    [PARTITION (partition_name [, partition_name] ...)]
    SET assignment_list
    [ON DUPLICATE KEY UPDATE assignment_list]

value:
    {expr | DEFAULT}

value_list:
    value [, value] ...

assignment:
    col_name = value

assignment_list:
    assignment [, assignment] ...
```

`Instructions`
 * Support distributed transactions to ensure cross-partition write atomicity
 * Support insert multiple values, these values can be in different partitions
 *  *Does not support clauses like INSERT ... SELECT Statement*
 * *Not support PARTITION* we support parser PARTITION, but the function hasn't supported yet.
 * If we write data with specified columns, we'll get a better performance.
 * Not support all default values: "INSERT INTO t VALUES (),(),();"
 * Not support subquery clause: "INSERT INTO t select * from t1 ... "
 * Not support expr in values: "INSERT INTO t values (a+2)"

`Example: `
`Write data with columns(In this way we'll get a better performance.)`
```
mysql> desc t;
+-------+---------+------+-----+---------+-------+
| Field | Type    | Null | Key | Default | Extra |
+-------+---------+------+-----+---------+-------+
| a     | int(11) | NO   | PRI | NULL    |       |
| b     | int(11) | YES  |     | NULL    |       |
| c     | int(11) | YES  |     | NULL    |       |
+-------+---------+------+-----+---------+-------+
3 rows in set (0.00 sec)

mysql> INSERT INTO t(id, age, c) VALUES(1, 24, 2), (2, 28, 3), (3, 29, 4);
Query OK, 3 rows affected (0.03 sec)
```

`Write data without columns(In this way there will be some performance loss.)`
```

mysql> insert into t values (11, 2, 3), (12, 4, 5), (13, 2, 3);
Query OK, 2 rows affected (0.03 sec)
```

## REPLACE

`Syntax`
```
REPLACE [LOW_PRIORITY | DELAYED]
    [INTO] tbl_name
    [PARTITION (partition_name [, partition_name] ...)]
    [(col_name [, col_name] ...)]
    {VALUES | VALUE} (value_list) [, (value_list)] ...

REPLACE [LOW_PRIORITY | DELAYED]
    [INTO] tbl_name
    [PARTITION (partition_name [, partition_name] ...)]
    SET assignment_list

value:
    {expr | DEFAULT}

value_list:
    value [, value] ...

assignment:
    col_name = value

assignment_list:
    assignment [, assignment] ...
```

`Instructions`
 * Support distributed transactions to ensure cross-partition write atomicity
 * Support replace multiple values, these values can be in different partitions
 *  *Does not support clauses like INSERT ... SELECT Statement*
 * *Not support PARTITION* we support parser PARTITION, but the function hasn't supported yet. 
 * If we write data with specified columns, we'll get a better performance.

`Example: `
```
mysql> replace into t values (23, 2, 3), (24, 4, 5), (25, 5, 6);
Query OK, 3 rows affected (0.01 sec)

mysql> replace into t(a,b,c) values (33, 2, 3), (34, 4, 5), (35, 5, 6);
Query OK, 3 rows affected (0.01 sec)
```

## SELECT

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

## UPDATE

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
