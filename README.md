[![Build Status](https://travis-ci.org/radondb/radon.png)](https://travis-ci.org/radondb/radon)
[![Go Report Card](https://goreportcard.com/badge/github.com/radondb/radon)](https://goreportcard.com/report/github.com/radondb/radon)
[![codecov.io](https://codecov.io/gh/radondb/radon/graphs/badge.svg)](https://codecov.io/gh/radondb/radon/branch/master)

# OverView
RadonDB is an open source, Cloud-native MySQL database for unlimited scalability and performance.

## What is RadonDB?

RadonDB is a cloud-native database based on MySQL，and architected in fully distributed cluster that enable unlimited scalability (scale-out), capacity and performance. It supported distributed transaction that ensure high data consistency, and leveraged MySQL as storage engine for trusted data reliability. RadonDB is compatible with MySQL protocol, and sup-porting automatic table sharding as well as batch of automation feature for simplifying the maintenance and operation workflow.

## Features

* **Automatic Sharding**
* **Auditing and Logging**
* **Parallel Execution**: Parallel Query, Parallel DML and Parallel DDL
* **Parallel CHECKSUM TABLE**: Gives same results as MySQL
* **Distributed Transaction**: Snapshot Isolation
* **Distributed Joins**: Sort-Merge Join, Nested-Loop Join
* **Distributed Full Text Search**
* **Multi Tenan by Database**
* **Prepared SQL Statement**
* **JSON**

## Documentation
For guidance on installation, deployment, and administration, see our [Documentation](docs).


## Architecture

## Overview
RadonDB is a new generation of distributed relational database (MyNewSQL) based on MySQL. It was designed to create the open-source database our developers would want to use: one that has features such as financial high availability、
large-capacity database、automatic plane split table、 scalable and strong consistency, this guide sets out to detail the inner-workings of the radon process as a means of explanation.


## SQL Layer

### SQL support
On SQL syntax level, RadonDB Fully compatible with MySQL.You can view all of the SQL features RadonDB supports here  [radon_sql_support](docs/radon_sql_support.md)

###  SQL parser, planner, executor

After your SQL node  receives a SQL request from a mysql client via proxy, RadonDB parses the statement, creates a query plan, and then executes the plan.




                                                                    +---------------+
                                                        x---------->|node1_Executor |
                                +--------------------+  x           +---------------+
                                |      SQL Node      |  x
                                |--------------------|  x
    +-------------+             |     sqlparser      |  x           +---------------+
    |    query    |+----------->|                    |--x---------->|node2_Executor |
    +-------------+             |  Distributed Plan  |  x           +---------------+
                                |                    |  x
                                +--------------------+  x
                                                        x           +---------------+
                                                        x---------->|node3_Executor |
                                                                    +---------------+



``` Parsing ```

Received queries are parsed by sqlparser (which describes the supported syntax by mysql) and generated Abstract Syntax Trees (AST).


``` Planning ```

With the AST, RadonDB begins planning the query's execution by generating a tree of planNodes.
This step also includes steps analyzing the client's SQL statements against the expected AST expressions, which include things like type checking.

You can see the a query plan  generates using `EXPLAIN`(At this stage we only use `EXPLAIN` to  analysis  Table distribution).

``` Excuting ```
Executing an Executor in a storage layer in Parallel with a Distributed Execution Plan.

### SQL with Transaction
The SQL node is stateless, but in order to guarantee transaction `Snapshot Isolation`, it is currently a write-multiple-read mode.


## Transaction Layer

``` Distributed transaction```

RadonDB provides distributed transaction capabilities. If the distributed executor at different storage nodes and one of the nodes failed to execute, then operation of the rest nodes will be rolled back, This guarantees the atomicity of operating across nodes  and makes the database in a consistent state.

```Isolation Levels```

RadonDB achieves the level of SI (Snapshot Isolation) at the level of consistency. As long as a distributed transaction has not commit, or if some of the partitions have committed, the operation is invisible to other transactions.

``` Transaction with SQL Layer```

The SQL node is stateless, but in order to guarantee transaction `Snapshot Isolation`, it is currently a write-multiple-read mode.

## Live Demo
 [https://radonchain.org](https://radonchain.org)

## Issues

The [integrated github issue tracker](https://github.com/radondb/radon/issues)
is used for this project.

## License

RadonDB is released under the GPLv3. See LICENSE
