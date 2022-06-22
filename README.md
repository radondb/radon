[![Build Status](https://travis-ci.org/radondb/radon.svg?branch=master)](https://travis-ci.org/radondb/radon)
[![Go Report Card](https://goreportcard.com/badge/github.com/radondb/radon)](https://goreportcard.com/report/github.com/radondb/radon)
[![codecov.io](https://codecov.io/gh/radondb/radon/graphs/badge.svg)](https://codecov.io/gh/radondb/radon/branch/master)

# Overview
RadonDB is an open-source and cloud-native database based on MySQL, enabling unlimited scalability and high performance.

## What is RadonDB?

RadonDB is a cloud-native database based on MySQL. It adopts the architecture of distributed clusters, enabling unlimited scalability (scaling-out), capacity and performance. It supports distributed transactions and ensures high data consistency, using MySQL as the storage engine for data reliability. RadonDB is compatible with MySQL protocols, and supports automatic table sharding and a batch of automation features for simplifying the O&M workflow.

## Features

⌛ **Automatic sharding**

✍ **Auditing and logging**

🎈 **Parallel execution**: Parallel query, DML and DDL

🧠 **Parallel CHECKSUM TABLE**: Gives the same results as MySQL

💻 **Distributed transaction**: Snapshot isolation

👏 **Distributed Joins**: Sort-merge Join, nested-loop Join

⌨ **Distributed full-text search**

🙂 **Multi-tenancy**

👂 **Prepared SQL statement**

👀 **JSON**

## Documentation
For guidance on installation, deployment, and management of RadonDB, see our [Documentation](docs).


## Architecture

### Overview
RadonDB is a new distributed relational database (MyNewSQL) based on MySQL. It is designed to create the open-source and developer-friendly database, with such features as high availability in financial scenarios, large capacity, automatic horizontal table partitioning, scalability and strong consistency. This guide displays the details of the internals of RadonDB.


### SQL layer

#### SQL support
On SQL syntax level, RadonDB is compatible with MySQL. For all the SQL features supported by RadonDB, see [radon_sql_statements_manual](docs/radon_sql_statements_manual.md)

#### SQL parser, planner and executor

After a node in a RadonDB cluster receives a SQL request from a MySQL client by proxy, RadonDB parses the statement, creates a query plan, and then executes the plan.




                                                                    +---------------+
                                                        x---------->|node1_Executor |
                                +--------------------+  x           +---------------+
                                |      SQL node      |  x
                                |--------------------|  x
    +-------------+             |     SQL parser     |  x           +---------------+
    |    Query    |+----------->|                    |--x---------->|node2_Executor |
    +-------------+             |  Distributed plan  |  x           +---------------+
                                |                    |  x
                                +--------------------+  x
                                                        x           +---------------+
                                                        x---------->|node3_Executor |
                                                                    +---------------+



``` Parsing ```

Received queries are parsed by SQL parser (describing the syntax supported by MySQL) and abstract syntax trees (AST) are generated.


``` Planning ```

With the AST, a query plan is formed by RadonDB by generating a tree of plan nodes.
During the process, RadonDB also performs semantic analysis, including checking whether the query is a valid statement in the SQL language.

As we can see, a query plan is generated with `EXPLAIN` (at this stage, only `EXPLAIN` is used to analyze table distribution).

``` Execution ```

In the storage layer, the executor is called in parallel with the distributed execution plan.

#### SQL and transaction
The SQL node is stateless, but the multiple-reader single-writer mode is currently adopted for the purpose of transaction `snapshot isolation`.


### Transaction layer

``` Distributed transaction```

RadonDB provides distributed transaction capabilities. If one of the nodes fails to execute the transaction in the distributed environment, the rest nodes will be rolled back. 
This ensures the atomicity of operating across nodes and keeps the database in a consistent state.

```Isolation levels```

RadonDB achieves snapshot isolation levels on the basis of consistency levels. As long as the distributed transaction has not committed, or some of the partitions have committed, the operation is invisible to other transactions.

``` Transaction with SQL layer```

The SQL node is stateless, but the multiple-reader single-writer mode is currently adopted for the purpose of transaction `snapshot isolation`.


## Issue

The [integrated GitHub issue tracker](https://github.com/radondb/radon/issues)
is used for this project.

## License

RadonDB is released under the GPLv3. See [LICENSE](LICENSE).