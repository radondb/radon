Table of Contents
=================

   * [DESCRIBE Statement](#describe-statement)
   * [EXPLAIN Statement](#explain-statement)

# DESCRIBE Statement
The DESCRIBE and EXPLAIN statements are synonyms, used either to obtain information about table structure or query execution plans. For more information, see [EXPLAIN Statement](#explain-statement).

# EXPLAIN Statement
`Syntax`
```
{EXPLAIN | DESCRIBE | DESC}
    tbl_name [col_name | wild]
```

`Instructions`
* wild: if given, is a pattern string. It can contain the SQL % and _ wildcard characters.

`Example: `
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
