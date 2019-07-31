# Shift

Table transfer for radon.

## How to work

First, Shift will dump your MySQL data then sync changed data using binlog incrementally.

## Mysqldump

You should specified the absolute path for mysqldump by command line option '--mysqldump' or make sure mysqldump in your PATH.

## Binlog Sync

You must use ROW format for binlog, full binlog row image is preferred.

## How to use shift

Usage:
```
./bin/shift --from=[host:port] --from-database=[database] --from-table=[table] --from-user=[user] --from-password=[password] --to=[host:port] --to-database=[database] --to-table=[table]  --to-user=[user] --to-password=[password]
```

For example:
```
./bin/shift --from=192.168.0.2:3306 --from-database=sbtest --from-table=benchyou0_0031 --from-user=mock --from-password=mock --to=192.168.0.9:3306 --to-database=sbtest --to-table=benchyou0_0031 --to-user=mock --to-password=mock
```

## Shift Successful

IMPORTANT: Please check that the shift run completes successfully.

At the end of a successful shift run prints "shift.completed.OK!".

## Restrictions and Limits

* DDL - not support transfer table with ddl operation

* XA - support xa transaction not well (now we can't fix the xa transaction whict has not commited before binlog sync start)
