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
