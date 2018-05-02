# OverView
RadonDB is an open source, cloud-native distributed MySQL database for building global, scalable cloud services

## what is RadonDB?

RadonDB is based MySQL,but add more next generation feature such as distributed,unlimted scale out,support distributed transaction, and compatible with the MySQL protocol.

## Feature

* **Automatic shard table**: automatically shards (partitions) tables across nodes, enabling databases to scale horizontally on low cost,Sharding is entirely transparent to the application which is able to connect to any node in the cluster and have queries automatically access the correct shards.
* **support distributed transaction**: support distributed transaction across partitions,implementation Atomicity,Consistency,Isolation,Durability （ACID）for transaction completely.
* **Automatic data compression**: DBA selected TokuDB storage engine when partition MySQL,Will automatically achieve high-rate data compression,The storage space utilization is increased by 50%, which greatly saves storage space and IO overhead, and further optimizes service performance.
* **Intelligent smooth scale out**: Virtually seamless dynamic re-sharding,Vertical and Horizontal sharding support.when scale out finished, all data will auto rebalance between shards,The service is not perceived and will not be interrupted due to capacity scaling.
* **support connection thread pool**: Providing a connected thread pool and presetting a set of connected thread resources. When a distributed SQL cluster needs to establish an access connect with each storage node, these pre-set threads can be used to quickly establish connection and support connection reuse and automatic reconnection. Overall connection efficiency.
* **auditing and logging**:The user can choose to enable the SQL query audit log function to implement auditing of multiple dimensions such as the query event time, operation statement type, and time-consuming query to ensure the security of the user's operations and meet the data compliance requirements. The audit log can be set to read operation auditing, write operation auditing, or simultaneous read/write operation auditing modes,and allow the user to select flexibly based on actual needs.

## Architecture

## Overview
RadonDB is a new generation of distributed relational database (MyNewSQL) based on MySQL. It was designed to create the open-source database our developers would want to use: one that has features such as financial high availability、
large-capacity database、automatic plane split table、 scalable and strong consistency, this guide sets out to detail the inner-workings of the radon process as a means of explanation.


## SQL Layer

### SQL surpported
On SQL syntax level, RadonDB Fully compatible with MySQL.You can view all of the SQL features RadonDB supports here  [radon_sql_surported](radon_SQL_surpported.md)

###  SQL parser, planner, excutor

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

RadonDB provides distributed transaction capabilities. If the distrubuted executor at different storage nodes and one of the nodes failed to execute, then operation of the rest nodes will be rolled back, This guarantees the atomicity of operating across nodes  and makes the database in a consistent state.

```Isolation Levels```

RadonDB achieves the level of SI (Snapshot Isolation) at the level of consistency. As long as a distributed transaction has not commit, or if some of the partitions have committed, the operation is invisible to other transactions.

``` Transaction with SQL Layer```

The SQL node is stateless, but in order to guarantee transaction `Snapshot Isolation`, it is currently a write-multiple-read mode.
