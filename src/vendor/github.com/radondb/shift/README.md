Shift

$ make build

Usage:
```
./bin/shift --from=[host:port] --from-database=[database] --from-table=[table] --from-user=[user] --from-password=[password] --to=[host:port] --to-database=[database] --to-table=[table]  --to-user=[user] --to-password=[password]
```

For example:
```
./bin/shift --from=192.168.0.2:3306 --from-database=sbtest --from-table=benchyou0_0031 --from-user=mock --from-password=mock --to=192.168.0.9:3306 --to-database=sbtest --to-table=benchyou0_0031 --to-user=mock --to-password=mock
```


$ How to test shift

To test shift, first create two mysql instance:

1. $cat my.cnf.3306
```
[mysqld]
port = 3306
socket = /tmp/mysql.sock.3306
datadir = data3306
log_bin=mysql-bin
log_bin_index=mysql-bin.index
binlog_format=ROW
server-id        = 12345

[mysqld_safe]

```

2. $./bin/mysqld --defaults-file=my.cnf.3306 --initialize-insecure

3. $./bin/mysqld --defaults-file=my.cnf.3306

4. $cat my.cnf.3307
```
[mysqld]
port = 3307
socket = /tmp/mysql.sock.3307
datadir = data3307
log_bin=mysql-bin
log_bin_index=mysql-bin.index
binlog_format=ROW
server-id        = 12346

[mysqld_safe]
```

5. $./bin/mysqld --defaults-file=my.cnf.3307 --initialize-insecure

6. $./bin/mysqld --defaults-file=my.cnf.3307

7. $make testshift

$ How to test go-mysql
1. create a database `test` on 127.0.0.1:3306
2. make testmysql
