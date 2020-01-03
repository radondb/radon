/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDDL1(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		// Table.
		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id) partitions 6",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") default charset=utf8",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") default charset=utf8",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 PARTITION BY HASH(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=InnoDB default charset=utf8mb4",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8 partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default charset=utf8",
		},
		{
			input: "create table t (\n" +
				"	`status` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(`status`)",
			output: "create table t (\n" +
				"	`status` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	status int primary key comment '/*non_reserved_keyword*/',\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(status)",
			output: "create table t (\n" +
				"	`status` int comment '/*non_reserved_keyword*/' primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	bool int primary key comment '/*non_reserved_keyword*/',\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(bool)",
			output: "create table t (\n" +
				"	`bool` int comment '/*non_reserved_keyword*/' primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	enum int primary key comment '/*non_reserved_keyword*/',\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(enum)",
			output: "create table t (\n" +
				"	`enum` int comment '/*non_reserved_keyword*/' primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	datetime int primary key comment '/*non_reserved_keyword*/',\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(datetime)",
			output: "create table t (\n" +
				"	`datetime` int comment '/*non_reserved_keyword*/' primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// For issue: https://github.com/radondb/radon/issues/486
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb comment='comment option' default charset=utf8 partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset=utf8",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine='tokudb' comment='comment option' default charset='utf8' global",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine='tokudb' default charset='utf8'",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") single engine=tokudb comment='comment option' default charset utf8",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset=utf8",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb comment 'comment option' charset \"utf8\" partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset='utf8'",
		},

		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb comment='comment option' character set 'utf8' partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset='utf8'",
		},
		// GLOBAL.
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") GLOBAL",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// SINGLE.
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") SINGLE",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// partition list.
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") PARTITION BY LIST(c1) (" +
				"PARTITION p0 VALUES IN (1,4,7)," +
				"PARTITION p1 VALUES IN (2,5,8) )",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// SINGLE DISTRIBUTED BY BACKEND
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") distributed by(node3)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// NORMAL.
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") ",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},
		// for issue #190
		{
			input: "create table t (\n" +
				"	`id` int not null,\n" +
				"	`col2` int not null unique,\n" +
				"	`col3` int not null unique key,\n" +
				"	`col4` int not null unique key key,\n" +
				"	`col5` int not null unique key key comment 'RadonDB',\n" +
				"	`col6` int not null unique key key comment 'RadonDB' auto_increment,\n" +
				"	`col7` int not null unique key key comment 'RadonDB' auto_increment primary key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null,\n" +
				"	`col2` int not null unique key,\n" +
				"	`col3` int not null unique key,\n" +
				"	`col4` int not null primary key unique key,\n" +
				"	`col5` int not null comment 'RadonDB' primary key unique key,\n" +
				"	`col6` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int auto_increment,\n" +
				"	`col2` int auto_increment not null,\n" +
				"	`col3` int auto_increment not null unique,\n" +
				"	`col4` int auto_increment not null unique key,\n" +
				"	`col5` int auto_increment not null unique key key,\n" +
				"	`col6` int auto_increment not null unique key key comment 'RadonDB',\n" +
				"	`col7` int auto_increment not null unique key key comment 'RadonDB' primary key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int auto_increment,\n" +
				"	`col2` int not null auto_increment,\n" +
				"	`col3` int not null auto_increment unique key,\n" +
				"	`col4` int not null auto_increment unique key,\n" +
				"	`col5` int not null auto_increment primary key unique key,\n" +
				"	`col6` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int comment 'RadonDB',\n" +
				"	`col2` int comment 'RadonDB' not null,\n" +
				"	`col3` int comment 'RadonDB' not null unique,\n" +
				"	`col4` int comment 'RadonDB' not null unique key,\n" +
				"	`col5` int comment 'RadonDB' not null unique key key,\n" +
				"	`col6` int comment 'RadonDB' not null unique key key comment 'RadonDB',\n" +
				"	`col7` int comment 'RadonDB' not null unique key key comment 'RadonDB' auto_increment,\n" +
				"	`col8` int comment 'RadonDB' not null unique key key comment 'RadonDB' auto_increment primary key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int comment 'RadonDB',\n" +
				"	`col2` int not null comment 'RadonDB',\n" +
				"	`col3` int not null comment 'RadonDB' unique key,\n" +
				"	`col4` int not null comment 'RadonDB' unique key,\n" +
				"	`col5` int not null comment 'RadonDB' primary key unique key,\n" +
				"	`col6` int not null comment 'RadonDB' primary key unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`col8` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int key,\n" +
				"	`col2` int key not null,\n" +
				"	`col3` int key not null unique,\n" +
				"	`col4` int key not null unique key,\n" +
				"	`col5` int key not null unique key comment 'RadonDB',\n" +
				"	`col6` int key not null unique key comment 'RadonDB' auto_increment,\n" +
				"	`col7` int key not null unique key comment 'RadonDB' auto_increment primary key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`col2` int not null primary key,\n" +
				"	`col3` int not null primary key unique key,\n" +
				"	`col4` int not null primary key unique key,\n" +
				"	`col5` int not null comment 'RadonDB' primary key unique key,\n" +
				"	`col6` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`col2` int primary key not null,\n" +
				"	`col3` int primary key not null unique,\n" +
				"	`col4` int primary key not null unique key,\n" +
				"	`col5` int primary key not null unique key comment 'RadonDB',\n" +
				"	`col6` int primary key not null unique key comment 'RadonDB' auto_increment,\n" +
				"	`col7` int primary key not null unique key comment 'RadonDB' auto_increment key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`col2` int not null primary key,\n" +
				"	`col3` int not null primary key unique key,\n" +
				"	`col4` int not null primary key unique key,\n" +
				"	`col5` int not null comment 'RadonDB' primary key unique key,\n" +
				"	`col6` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int unique,\n" +
				"	`col2` int unique not null,\n" +
				"	`col3` int unique not null unique key,\n" +
				"	`col4` int unique not null key unique,\n" +
				"	`col5` int unique not null unique key comment 'RadonDB',\n" +
				"	`col6` int unique not null unique key comment 'RadonDB' auto_increment,\n" +
				"	`col7` int unique not null unique key comment 'RadonDB' auto_increment primary key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int unique key,\n" +
				"	`col2` int not null unique key,\n" +
				"	`col3` int not null unique key,\n" +
				"	`col4` int not null primary key unique key,\n" +
				"	`col5` int not null comment 'RadonDB' unique key,\n" +
				"	`col6` int not null auto_increment comment 'RadonDB' unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int unique key,\n" +
				"	`col2` int unique key not null,\n" +
				"	`col3` int unique key not null unique,\n" +
				"	`col4` int unique key not null key unique,\n" +
				"	`col5` int unique key not null key unique comment 'RadonDB',\n" +
				"	`col6` int unique key not null key unique comment 'RadonDB' auto_increment,\n" +
				"	`col7` int unique key not null key unique comment 'RadonDB' auto_increment primary key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int unique key,\n" +
				"	`col2` int not null unique key,\n" +
				"	`col3` int not null unique key,\n" +
				"	`col4` int not null primary key unique key,\n" +
				"	`col5` int not null comment 'RadonDB' primary key unique key,\n" +
				"	`col6` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`col7` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int not null auto_increment primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null auto_increment primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// current timestamp.
		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`t1` timestamp default current_timestamp,\n" +
				"	`t2`  timestamp ON UPDATE CURRENT_TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'currenttimestamp' DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'currenttimestamp' ON UPDATE CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`t1` timestamp default current_timestamp,\n" +
				"	`t2` timestamp not null default current_timestamp on update current_timestamp comment 'currenttimestamp'\n" +
				")",
		},

		// Fulltext.
		{
			input: "create table t (\n" +
				"	id INT PRIMARY KEY,\n" +
				"	title VARCHAR(200),\n" +
				"   FULLTEXT INDEX ngram_idx(title,body) WITH PARSER ngram\n" +
				")",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`title` varchar(200),\n" +
				"	fulltext index `ngram_idx` (`title`, `body`) WITH PARSER ngram\n" +
				")",
		},

		// Fulltext.
		{
			input: "create table t (\n" +
				"	id INT PRIMARY KEY,\n" +
				"	title VARCHAR(200),\n" +
				"   FULLTEXT KEY ngram_idx(title,body) /*!50100 WITH PARSER `ngram` */ \n" +
				")",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`title` varchar(200),\n" +
				"	fulltext key `ngram_idx` (`title`, `body`) WITH PARSER ngram\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int unique key unique primary key comment 'RadonDB' auto_increment not null\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null auto_increment comment 'RadonDB' primary key unique key\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int key not null auto_increment primary key primary key key key not null auto_increment,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null auto_increment primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int key not null auto_increment primary key primary key key unique unique key unique not null auto_increment,\n" +
				"	`name` varchar(10) comment 'RadonDB'\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null auto_increment primary key unique key,\n" +
				"	`name` varchar(10) comment 'RadonDB'\n" +
				")",
		},

		{
			input: "create table t (\n" +
				"	`id` int comment 'RadonDB' auto_increment not null primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null auto_increment comment 'RadonDB' primary key,\n" +
				"	`name` varchar(10)\n" +
				")",
		},

		// test key field options
		{
			input: "create table t (\n" +
				"	`id` int comment 'RadonDB' auto_increment not null primary key,\n" +
				"	`name` varchar(10) key\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int not null auto_increment comment 'RadonDB' primary key,\n" +
				"	`name` varchar(10) primary key\n" +
				")",
		},
		// Index.
		{
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10),\n" +
				"	KEY `IDX_USER` (`user_id`)\n" +
				") engine=tokudb default charset=utf8 partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10),\n" +
				"	key `IDX_USER` (`user_id`)\n" +
				") engine=tokudb default charset=utf8",
		},

		{
			input:  "create table if not exists t1 (a int)",
			output: "create table if not exists t1 (\n\t`a` int\n)",
		},

		{
			input:  "truncate table t1",
			output: "truncate table t1",
		},

		{
			input:  "drop table t1",
			output: "drop table t1",
		},

		{
			input:  "drop table t1, t2",
			output: "drop table t1, t2",
		},

		{
			input:  "drop table if exists t1",
			output: "drop table if exists t1",
		},

		// Database.
		{
			input:  "drop database test",
			output: "drop database test",
		},

		{
			input:  "create database test",
			output: "create database test",
		},

		{
			input:  "drop database if exists test",
			output: "drop database if exists test",
		},
		{
			input:  "create database if not exists test",
			output: "create database if not exists test",
		},

		// Create database with option issue #478
		{
			input:  "create database test charset utf8mxx",
			output: "create database test charset utf8mxx",
		},
		{
			input:  "create database test character set latin1xxx",
			output: "create database test character set latin1xxx",
		},
		{
			input:  "create database test charset default",
			output: "create database test charset default",
		},
		{
			input:  "create database test character set default",
			output: "create database test character set default",
		},
		{
			input:  "create database test charset utf8mb4",
			output: "create database test charset utf8mb4",
		},
		{
			input:  "create database test character set latin1",
			output: "create database test character set latin1",
		},
		{
			input:  "create database test collate latin1_swedish_ci",
			output: "create database test collate latin1_swedish_ci",
		},
		{
			input:  "create database test collate default charset default",
			output: "create database test collate default charset default",
		},
		{
			input:  "create database test  charset default collate default",
			output: "create database test charset default collate default",
		},
		{
			input:  "create database if not exists test  charset default collate default",
			output: "create database if not exists test charset default collate default",
		},
		{
			input:  "create database if not exists test collate utf8mb4_bin",
			output: "create database if not exists test collate utf8mb4_bin",
		},
		{
			input:  "create database if not exists test collate utf8mb4_unicode_ci charset utf8mb4",
			output: "create database if not exists test collate utf8mb4_unicode_ci charset utf8mb4",
		},
		{
			input:  "create database if not exists test collate utf8mb4_unicode_ci charset utf8mb4 charset utf8mb4",
			output: "create database if not exists test collate utf8mb4_unicode_ci charset utf8mb4 charset utf8mb4",
		},

		// Alter engine.
		{
			input:  "alter table test engine=tokudb",
			output: "alter table test engine = tokudb",
		},
		{
			input:  "alter table test.t1 engine=tokudb",
			output: "alter table test.t1 engine = tokudb",
		},

		// Alter charset.
		{
			input:  "alter table test.t1 convert to character set utf8",
			output: "alter table test.t1 convert to character set utf8",
		},

		// Index.
		{
			input:  "create index idx on test(a,b)",
			output: "create index idx on test",
		},
		{
			input:  "drop index idx on test",
			output: "drop index idx on test",
		},
		{
			input:  "create unique index a on b",
			output: "create index a on b",
		},
		{
			input:  "create unique index a on b(foo)",
			output: "create index a on b",
		},
		{
			input:  "create fulltext index a on b(foo)",
			output: "create index a on b",
		},
		{
			input:  "create spatial index a on b(foo)",
			output: "create index a on b",
		},

		// Add column.
		{
			input: "alter table test add column(id int primary key)",
			output: "alter table test add column (\n" +
				"	`id` int primary key\n" +
				")",
		},
		{
			input: "alter table test add column(id int primary key, name varchar(100))",
			output: "alter table test add column (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(100)\n" +
				")",
		},
		{
			input: "alter table test add column(status int primary key)",
			output: "alter table test add column (\n" +
				"	`status` int primary key\n" +
				")",
		},
		{
			input: "alter table test add column(bool int primary key)",
			output: "alter table test add column (\n" +
				"	`bool` int primary key\n" +
				")",
		},
		{
			input: "alter table test add column(datetime int primary key)",
			output: "alter table test add column (\n" +
				"	`datetime` int primary key\n" +
				")",
		},
		// for issue #190
		{
			input: "alter table test add column(id int not null, name varchar(100) auto_increment, col3 int primary key, col4 int comment 'RadonDB', col5 int unique key)",
			output: "alter table test add column (\n" +
				"	`id` int not null,\n" +
				"	`name` varchar(100) auto_increment,\n" +
				"	`col3` int primary key,\n" +
				"	`col4` int comment 'RadonDB',\n" +
				"	`col5` int unique key\n" +
				")",
		},
		{
			input: "alter table test add column(id int key, name varchar(100) unique)",
			output: "alter table test add column (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(100) unique key\n" +
				")",
		},
		{
			input: "alter table test add column(id int not null unique auto_increment unique key primary key key, name varchar(100) not null comment 'RadonDB' )",
			output: "alter table test add column (\n" +
				"	`id` int not null auto_increment primary key unique key,\n" +
				"	`name` varchar(100) not null comment 'RadonDB'\n" +
				")",
		},
		{
			input: "alter table test add column(id int unique key key unique not null key comment 'RadonDB' auto_increment, name varchar(100) not null key unique comment 'RadonDB')",
			output: "alter table test add column (\n" +
				"	`id` int not null auto_increment comment 'RadonDB' primary key unique key,\n" +
				"	`name` varchar(100) not null comment 'RadonDB' primary key unique key\n" +
				")",
		},

		// Modify column.
		{
			input:  "alter table test modify column name varchar(200)",
			output: "alter table test modify column `name` varchar(200)",
		},
		{
			input:  "alter table test modify column name varchar(200) not null",
			output: "alter table test modify column `name` varchar(200) not null",
		},
		// for issue #190
		{
			input:  "alter table test modify column name varchar(200) not null",
			output: "alter table test modify column `name` varchar(200) not null",
		},
		{
			input:  "alter table test modify column name varchar(200) auto_increment",
			output: "alter table test modify column `name` varchar(200) auto_increment",
		},
		{
			input:  "alter table test modify column name varchar(200) comment 'RadonDB'",
			output: "alter table test modify column `name` varchar(200) comment 'RadonDB'",
		},
		{
			input:  "alter table test modify column name varchar(200) key",
			output: "alter table test modify column `name` varchar(200) primary key",
		},
		{
			input:  "alter table test modify column name varchar(200) primary key",
			output: "alter table test modify column `name` varchar(200) primary key",
		},
		{
			input:  "alter table test modify column name varchar(200) unique",
			output: "alter table test modify column `name` varchar(200) unique key",
		},
		{
			input:  "alter table test modify column name varchar(200) unique key",
			output: "alter table test modify column `name` varchar(200) unique key",
		},
		{
			input:  "alter table test modify column name varchar(200) primary key not null",
			output: "alter table test modify column `name` varchar(200) not null primary key",
		},
		{
			input:  "alter table test modify column name varchar(200) auto_increment primary key not null",
			output: "alter table test modify column `name` varchar(200) not null auto_increment primary key",
		},
		{
			input:  "alter table test modify column name varchar(200) key auto_increment unique not null comment 'RadonDB'",
			output: "alter table test modify column `name` varchar(200) not null auto_increment comment 'RadonDB' primary key unique key",
		},

		// Drop column.
		{
			input:  "alter table test drop column name",
			output: "alter table test drop column `name`",
		},

		// Rename table
		{
			input:  "alter table test rename newtest",
			output: "rename table test to newtest",
		},
		{
			input:  "alter table test rename to newtest",
			output: "rename table test to newtest",
		},
		{
			input:  "alter table test rename as newtest",
			output: "rename table test to newtest",
		},
	}

	for _, ddl := range validSQL {
		// If we want debug, open the comment, default we close it
		// t.Logf(ddl.input)
		sql := strings.TrimSpace(ddl.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}

		// Walk.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)

		// Walk.
		node := tree.(*DDL)
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, node.TableSpec)

		got := String(tree.(*DDL))
		if ddl.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", ddl.output, got)
		}
	}
}

func TestDDL1ParseError(t *testing.T) {
	invalidSQL := []struct {
		input  string
		output string
	}{
		{
			input:  "create database test1 char set default", // char-->charset
			output: "syntax error at position 27 near 'char'",
		},
		{
			input:  "create database test2 character default", // character-->character set
			output: "syntax error at position 40 near 'default'",
		},
		{
			input:  "create database test4 collate ", // collate_name should not be empty
			output: "syntax error at position 31",
		},
		{
			input:  "create database test4 charset ", // charset_name should not be empty
			output: "syntax error at position 31",
		},
		// test some non_reserved_keyword moved to reserved_keyword, issue:https://github.com/radondb/radon/pull/496
		// e.g.: bigint,blob,char,decimal,integer...
		{
			input: "create table t (\n" +
				"	bigint int primary key,\n" +
				") partition by hash(id)",
			output: "syntax error at position 25 near 'bigint'",
		},
		{
			input: "create table t (\n" +
				"	blob int primary key,\n" +
				") partition by hash(id)",
			output: "syntax error at position 23 near 'blob'",
		},
		{
			input: "create table t (\n" +
				"	char int primary key,\n" +
				") partition by hash(id)",
			output: "syntax error at position 23 near 'char'",
		},
		{
			input: "create table t (\n" +
				"	decimal int primary key,\n" +
				") partition by hash(id)",
			output: "syntax error at position 26 near 'decimal'",
		},
		{
			input: "create table t (\n" +
				"	integer int primary key,\n" +
				") partition by hash(id)",
			output: "syntax error at position 26 near 'integer'",
		},
		{ // Duplicate keyword
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb auto_increment=100 engine=tokudb comment 'comment option' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'engine', the option should only be appeared just one time in RadonDB. at position 164 near 'partition'",
		},
		{ // Duplicate keyword
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb comment 'comment option' comment 'radondb' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'comment', the option should only be appeared just one time in RadonDB. at position 149 near 'partition'",
		},
		{ // The content of comment should be quoted with \' or \"
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb comment option charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 96 near 'option'",
		},
		{ // Keyword "single" used with "partition" at the same time
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") single engine=tokudb comment 'str' charset \"utf8\" partition by hash(id)",
			output: "SINGLE or GLOBAL should not be used simultaneously with PARTITION at position 140",
		},
	}

	for _, ddl := range invalidSQL {
		sql := strings.TrimSpace(ddl.input)
		_, err := Parse(sql)
		assert.Equal(t, ddl.output, err.Error())
	}
}
