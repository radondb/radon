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
				"	`name` varchar(10),\n" +
				"	start varchar(10),\n" +
				"	c bool not null default true,\n" +
				"	d bool not null default false,\n" +
				"	e set('a', \"b\", 'c')\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10),\n" +
				"	`start` varchar(10),\n" +
				"	`c` bool not null default true,\n" +
				"	`d` bool not null default false,\n" +
				"	`e` set('a', 'b', 'c')\n" +
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
				") engine=tokudb comment='comment option' default charset utf8 single",
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
				") tableSpace=storage stats_sample_pageS=65535 stats_persistenT=default Stats_auto_recalC=1 Row_forMat=dynamic PassWord='pwd' pack_keys=default max_rows=3 min_rows=2 key_block_size=1 insert_method=First encryption='n' delay_key_write=1 data directory='/data' index directory='/index' connection='id' comPression='lz4' default collate='utf8_bin' checksum=1 avg_row_length=123 engine=tokudb comment='comment option' character set 'utf8' partition by hash(id)",
			output: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset='utf8' avg_row_length=123 checksum=1 collate='utf8_bin' compression='lz4' connection='id' data directory='/data' index directory='/index' delay_key_write=1 encryption='n' insert_method=First key_block_size=1 max_rows=3 min_rows=2 pack_keys=default password='pwd' row_format=dynamic stats_auto_recalc=1 stats_persistent=default stats_sample_pages=65535 tablespace=storage",
		},

		{
			input: "create table test.t (\n" +
				"	`name` varchar(10)\n" +
				") row_format=tokudb_Quicklz engine=tokudb comment 'comment option' charset \"utf8\" partition by hash(id)",
			output: "create table test.t (\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset='utf8' row_format=tokudb_quicklz",
		},
		{
			input: "create table test.t (\n" +
				"	`name` varchar(10)\n" +
				") Stats_auto_recalC=default engine=tokudb comment 'comment option' charset \"utf8\" partition by hash(id)",
			output: "create table test.t (\n" +
				"	`name` varchar(10)\n" +
				") comment='comment option' engine=tokudb default charset='utf8' stats_auto_recalc=default",
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
				"	`t2`  timestamp ON UPDATE CURRENT_TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'currenttimestamp' DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'currenttimestamp' ON UPDATE CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
				"	`t3` timestamp default LOCALTIMESTAMP() on Update current_timestamp(),\n" +
				"	`t4` timestamp(5) default current_timestamp(5) on Update current_timestamp(5)\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`t1` timestamp default current_timestamp,\n" +
				"	`t2` timestamp not null default current_timestamp on update current_timestamp comment 'currenttimestamp',\n" +
				"	`t3` timestamp default localtimestamp() on update current_timestamp(),\n" +
				"	`t4` timestamp(5) default current_timestamp(5) on update current_timestamp(5)\n" +
				")",
		},

		// BOOL and BOOLEAN.
		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`a` bool,\n" +
				"	`b` boolean\n" +
				") partition by hash(id)",
			output: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`a` bool,\n" +
				"	`b` boolean\n" +
				")",
		},

		// SPATIAL TYPE.

		{
			input: "create table gis (\n" +
				"	`id` int primary key,\n" +
				"	`a` GEOMETRY,\n" +
				"	`b` POINT,\n" +
				"	`c` LINESTRING,\n" +
				"	`d` POLYGON,\n" +
				"	`e` GEOMETRYCOLLECTION,\n" +
				"	`f` MULTIPOINT,\n" +
				"	`g` MULTILINESTRING,\n" +
				"	`h` MULTIPOLYGON\n" +
				") partition by hash(id)",
			output: "create table gis (\n" +
				"	`id` int primary key,\n" +
				"	`a` geometry,\n" +
				"	`b` point,\n" +
				"	`c` linestring,\n" +
				"	`d` polygon,\n" +
				"	`e` geometrycollection,\n" +
				"	`f` multipoint,\n" +
				"	`g` multilinestring,\n" +
				"	`h` multipolygon\n" +
				")",
		},

		// index definition.
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

		{
			input: "create table t (\n" +
				"	id INT,\n" +
				"	title VARCHAR(200),\n" +
				"	gis GEOMETRY,\n" +
				"	INDEX (id) using btree comment 'a',\n" +
				"	INDEX id_idx(id) using btree comment 'a',\n" +
				"	KEY id_idx(id) using btree comment 'a',\n" +
				"	KEY id_idx using btree(id) using btree comment 'a',\n" +
				"	CONSTRAINT symbol UNIQUE id_idx(id) using btree comment 'a',\n" +
				"	CONSTRAINT UNIQUE KEY id_idx(id) using btree comment 'a',\n" +
				"	UNIQUE INDEX id_idx(id) using btree comment 'a',\n" +
				"	FULLTEXT INDEX ngram_idx(title) WITH PARSER ngram,\n" +
				"	SPATIAL INDEX gis_idx(gis) key_block_size=10,\n" +
				"	CONSTRAINT symbol PRIMARY KEY using rtree(id) using btree comment 'a',\n" +
				"	CONSTRAINT PRIMARY KEY Using rtree(id) using btree comment 'a',\n" +
				"	PRIMARY KEY Using rtree(id) using btree comment 'a'\n" +
				")",
			output: "create table t (\n" +
				"	`id` int,\n" +
				"	`title` varchar(200),\n" +
				"	`gis` geometry,\n" +
				"	index (`id`) using btree comment 'a',\n" +
				"	index `id_idx` (`id`) using btree comment 'a',\n" +
				"	key `id_idx` (`id`) using btree comment 'a',\n" +
				"	key `id_idx` (`id`) using btree comment 'a',\n" +
				"	unique `id_idx` (`id`) using btree comment 'a',\n" +
				"	unique key `id_idx` (`id`) using btree comment 'a',\n" +
				"	unique index `id_idx` (`id`) using btree comment 'a',\n" +
				"	fulltext index `ngram_idx` (`title`) WITH PARSER ngram,\n" +
				"	spatial index `gis_idx` (`gis`) key_block_size = 10,\n" +
				"	primary key (`id`) using btree comment 'a',\n" +
				"	primary key (`id`) using btree comment 'a',\n" +
				"	primary key (`id`) using btree comment 'a'\n" +
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
			input:  "truncate t1",
			output: "truncate table t1",
		},

		{
			input:  "drop table t1",
			output: "drop table t1",
		},

		{
			input:  "drop table t1, t2 RESTRICT",
			output: "drop table t1, t2",
		},

		{
			input:  "drop temporary table t1, t2 RESTRICT",
			output: "drop temporary table t1, t2",
		},

		{
			input:  "drop temporary table t1, t2 CASCADE",
			output: "drop temporary table t1, t2",
		},

		{
			input:  "drop temporary table t1, t2",
			output: "drop temporary table t1, t2",
		},

		{
			input:  "drop temporary table if exists t1, t2",
			output: "drop temporary table if exists t1, t2",
		},

		{
			input:  "drop table if exists t1",
			output: "drop table if exists t1",
		},

		// Database or Schema.
		{
			input:  "drop database test",
			output: "drop database test",
		},
		{
			input:  "drop schema test",
			output: "drop database test",
		},
		{
			input:  "create database test",
			output: "create database test",
		},
		{
			input:  "create schema test",
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
		{
			input:  "create schema if not exists test",
			output: "create database if not exists test",
		},

		// Create database with option issue #478
		{
			input:  "create database test1 char set default",
			output: "create database test1 char set default",
		},
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

		// issue #689
		{
			input:  "create database test encryption 'n'",
			output: "create database test encryption 'n'",
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
			input:  "create index idx on test(a,b) using hash comment 'c' lock=EXCLUSIVE",
			output: "create index idx on test(`a`, `b`) using hash comment 'c' lock = EXCLUSIVE",
		},
		{
			input:  "drop index idx on test",
			output: "drop index idx on test",
		},
		{
			input:  "create unique index a on b(foo) using btree key_block_size=10 algorithm=copy",
			output: "create unique index a on b(`foo`) using btree key_block_size = 10 algorithm = copy",
		},
		{
			input:  "create fulltext index a on b(foo) with parser ngram comment 'c' lock=none algorithm=inplace",
			output: "create fulltext index a on b(`foo`) comment 'c' WITH PARSER ngram algorithm = inplace lock = none",
		},
		{
			input:  "create spatial index a on b(foo) comment 'c' key_block_size=10 algorithm=default lock=shared",
			output: "create spatial index a on b(`foo`) comment 'c' key_block_size = 10 algorithm = default lock = shared",
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
			t.Errorf("\nwant:\n%s\ngot:\n%s", ddl.output, got)
		}

		// To improve the code coverage.
		if node.PartitionOption != nil {
			node.PartitionOption.PartitionType()
		}
	}
}

func TestDDL1ParseError(t *testing.T) {
	invalidSQL := []struct {
		input  string
		output string
	}{
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
		{
			input:  "create database test5 encryption = 'y'", // charset_name should not be empty
			output: "The encryption option is parsed but ignored by all storage engines. at position 39 near 'y'",
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
		{
			input: "create table t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") partition by hash(id) partitions 0",
			output: "Number of partitions must be a positive integer at position 97 near '0'",
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
		{ // Duplicate keyword avg_row_length
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb avg_row_length=123 avg_row_length=123 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'avg_row_length', the option should only be appeared just one time in RadonDB. at position 144 near 'partition'",
		},
		{ // Duplicate keyword checksum
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb checksum=1 checksum=1 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'checksum', the option should only be appeared just one time in RadonDB. at position 128 near 'partition'",
		},
		{ // Duplicate keyword collate
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb default collate='utf8_bin' collate='utf8_bin' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for table option keyword 'collate', the option should only be appeared just one time in RadonDB. at position 152 near 'partition'",
		},
		{ // Duplicate keyword compression
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb compression 'zlib' COMPRESSION='Zlib' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'compression', the option should only be appeared just one time in RadonDB. at position 144 near 'partition'",
		},
		{ // Wrong keyword compression
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb compression 'xlib' charset \"utf8\" partition by hash(id)",
			output: "Invalid compression option, argument (should be 'ZLIB', 'LZ4' or 'NONE') at position 100 near 'xlib'",
		},
		{ // Duplicate keyword connection
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb connection='str' connection 'str2' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'connection', the option should only be appeared just one time in RadonDB. at position 141 near 'partition'",
		},
		{ // Duplicate keyword data directory
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb data directory='/data' data directory '/data2' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'data directory', the option should only be appeared just one time in RadonDB. at position 153 near 'partition'",
		},
		{ // Duplicate keyword index directory
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb index directory='/data' index directory '/data2' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'index directory', the option should only be appeared just one time in RadonDB. at position 155 near 'partition'",
		},
		{ // Duplicate keyword DELAY_KEY_WRITE
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb delay_key_write=0 delay_key_write=1 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'delay_key_write', the option should only be appeared just one time in RadonDB. at position 142 near 'partition'",
		},
		{ // Duplicate keyword ENCRYPTION
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb encryption='n' encryption='n' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'encryption', the option should only be appeared just one time in RadonDB. at position 136 near 'partition'",
		},
		{ // Invalid keyword ENCRYPTION option
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb encryption='y' charset \"utf8\" partition by hash(id)",
			output: "The encryption option is parsed but ignored by all storage engines. at position 96 near 'y'",
		},
		{ // Invalid keyword ENCRYPTION option
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb encryption='x' charset \"utf8\" partition by hash(id)",
			output: "Invalid encryption option, argument (should be Y or N) at position 96 near 'x'",
		},
		{ // Duplicate keyword INSERT_METHOD
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb insert_method=no insert_method=first charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'insert_method', the option should only be appeared just one time in RadonDB. at position 143 near 'partition'",
		},
		{ // Invalid keyword INSERT_METHOD
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb insert_method=noo charset \"utf8\" partition by hash(id)",
			output: "Invalid insert_method option, argument (should be NO, FIRST or LAST) at position 99 near 'noo'",
		},
		{ // Invalid keyword KEY_BLOCK_SIZE value
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb key_block_size=-1 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 98",
		},
		{ // Duplicate keyword KEY_BLOCK_SIZE
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb key_block_size=1 key_block_size=1 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'key_block_size', the option should only be appeared just one time in RadonDB. at position 140 near 'partition'",
		},
		{ // Duplicate keyword MAX_ROWS
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb max_rows=1 max_rows=1 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'max_rows', the option should only be appeared just one time in RadonDB. at position 128 near 'partition'",
		},
		{ // Duplicate keyword MIN_ROWS
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb min_rows=1 min_rows=1 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'min_rows', the option should only be appeared just one time in RadonDB. at position 128 near 'partition'",
		},
		{ // Invalid keyword MAX_ROWS value
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb max_rows=-1 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 92",
		},
		{ // Invalid keyword MIN_ROWS value
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb min_rows=-1 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 92",
		},
		{ // Invalid keyword pack_keys value
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb pack_keys=-8 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 93",
		},
		{ // Duplicate keyword PACK_KEYS
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb pack_keys=1 pack_keys=1 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'pack_keys', the option should only be appeared just one time in RadonDB. at position 130 near 'partition'",
		},
		{ // Duplicate keyword PASSWORD
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb password='pwd' password='pwd' charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'password', the option should only be appeared just one time in RadonDB. at position 136 near 'partition'",
		},
		{ // Duplicate keyword ROW_FORMAT
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb row_format=default row_format=dynamic charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'row_format', the option should only be appeared just one time in RadonDB. at position 144 near 'partition'",
		},
		{ // Duplicate keyword STATS_AUTO_RECALC
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb sTats_auto_recalc=0 stats_auto_recalc=0 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'stats_auto_recalc', the option should only be appeared just one time in RadonDB. at position 146 near 'partition'",
		},
		{ // Invalid keyword STATS_AUTO_RECALC option
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb stats_auto_recalc=-1 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 101",
		},
		{ // Duplicate keyword STATS_PERSISTENT
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb sTats_persistenT=0 sTats_persistenT=0 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'stats_persistent', the option should only be appeared just one time in RadonDB. at position 144 near 'partition'",
		},
		{ // Invalid keyword STATS_PERSISTENT option
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb stats_persisTent=-1 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 100",
		},
		{ // Duplicate keyword STATS_SAMPLE_PAGES
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb stats_sample_pageS=1 stats_sample_Pages=2 charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'stats_sample_pages', the option should only be appeared just one time in RadonDB. at position 148 near 'partition'",
		},
		{ // Invalid keyword STATS_SAMPLE_PAGES option
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb stats_sample_pages=-1 charset \"utf8\" partition by hash(id)",
			output: "syntax error at position 102",
		},
		{ // Duplicate keyword STATS_SAMPLE_PAGES
			input: "create table test.t (\n" +
				"	`id` int primary key,\n" +
				"	`name` varchar(10)\n" +
				") engine=tokudb tablespace=aa tablespace=bb charset \"utf8\" partition by hash(id)",
			output: "Duplicate table option for keyword 'tablespace', the option should only be appeared just one time in RadonDB. at position 134 near 'partition'",
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
				") engine=tokudb comment 'str' charset \"utf8\" single partition by hash(id)",
			output: "syntax error at position 127 near 'partition'",
		},
		{ // create index without index columns.
			input:  "create unique index a on b",
			output: "syntax error at position 28",
		},
		{ // create index lock type error.
			input:  "create index idx on t(a,b) lock=d",
			output: "unknown lock type at position 34 near 'd'",
		},
		{ // create index algorithm type error.
			input:  "create index idx on t(a,b) algorithm=d",
			output: "unknown algorithm type at position 39 near 'd'",
		},
		{ // create index in the wrong order.
			input:  "create index idx on t(a,b) algorithm=default using btree",
			output: "syntax error at position 51 near 'using'",
		},
		{ // create index use wrong options.
			input:  "create unique index idx on t(a,b) with parser ngram",
			output: "syntax error at position 39 near 'with'",
		},
	}

	for _, ddl := range invalidSQL {
		sql := strings.TrimSpace(ddl.input)
		_, err := Parse(sql)
		assert.Equal(t, ddl.output, err.Error())
	}
}
