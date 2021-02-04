Table of Contents
=================

   * [Transactional and Locking Statements](#transactional-and-locking-statements)
      * [Transaction](#transaction)
   * [Others](#others)
      * [Using AUTO INCREMENT](#using-auto-increment)
      * [Streaming fetch](#streaming-fetch)
      * [Read-write Separation](#read-write-separation)
   * [Full Text Search](#full-text-search)
      * [ngram Full Text Parser](#ngram-full-text-parser)

# Transactional and Locking Statements

## Transaction
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


# Others
##  Using AUTO INCREMENT

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

## Streaming fetch

`Instructions`
* When the query result set is relatively large, the result set can be fetched by streaming.
* Method 1: Execute `set @@ SESSION.radon_streaming_fetch = 'ON'` to turn on streaming fetch. After the query is executed, `set @@ SESSION.radon_streaming_fetch = 'OFF'` to turn off streaming fetch.
* Method 2: Add hint `/*+ streaming */` to the query statement.
* *Doesnot support complex queries*

`Example: `

```
mysql> select /*+ streaming */ * from t1;
Empty set (0.00 sec)
```

## Read-write Separation

`Instructions`
* If `load-balance` is 1, the query can route to the `replica-address`.
* The query must be read and not in multi-statement txn.
* By using `/*+ loadbalance=0 */`, the query will be forced to execute on normal `address`.
* By using `/*+ loadbalance=1 */`, the query will be forced to execute on `replica-address`.

`Example: `

```
mysql> select /*+ loadbalance=0 */ * from t1;
Empty set (0.00 sec)

mysql> select /*+ loadbalance=1 */ * from t1;
Empty set (0.00 sec)
```

# Full Text Search
##  ngram Full Text Parser

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
