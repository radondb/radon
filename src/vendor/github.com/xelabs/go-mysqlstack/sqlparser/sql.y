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

%{
package sqlparser

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
  yylex.(*Tokenizer).AllowComments = allow
}

func setDDL(yylex interface{}, ddl *DDL) {
  yylex.(*Tokenizer).partialDDL = ddl
}

func incNesting(yylex interface{}) bool {
  yylex.(*Tokenizer).nesting++
  if yylex.(*Tokenizer).nesting == 200 {
    return true
  }
  return false
}

func decNesting(yylex interface{}) {
  yylex.(*Tokenizer).nesting--
}

func forceEOF(yylex interface{}) {
  yylex.(*Tokenizer).ForceEOF = true
}

%}

%union {
  empty         struct{}
  statement     Statement
  selStmt       SelectStatement
  ddl           *DDL
  ins           *Insert
  byt           byte
  bytes         []byte
  bytes2        [][]byte
  str           string
  strs          []string
  selectExprs   SelectExprs
  selectExpr    SelectExpr
  columns       Columns
  colName       *ColName
  tableExprs    TableExprs
  tableExpr     TableExpr
  tableName     TableName
  tableNames    TableNames
  indexHints    *IndexHints
  expr          Expr
  exprs         Exprs
  boolVal       BoolVal
  colTuple      ColTuple
  values        Values
  valTuple      ValTuple
  subquery      *Subquery
  whens         []*When
  when          *When
  orderBy       OrderBy
  order         *Order
  limit         *Limit
  updateExprs   UpdateExprs
  updateExpr    *UpdateExpr
  setExprs      SetExprs
  setExpr       *SetExpr
  colIdent      ColIdent
  colIdents     []ColIdent
  tableIdent    TableIdent
  convertType   *ConvertType
  aliasedTableName *AliasedTableExpr
  TableSpec  *TableSpec
  TableOptionListOpt TableOptionListOpt
  TableOptionList    TableOptionList
  tableOption        *TableOption
  columnType    ColumnType
  colPrimaryKeyOpt   ColumnPrimaryKeyOption
  colUniqueKeyOpt    ColumnUniqueKeyOption
  optVal        *SQLVal
  LengthScaleOption LengthScaleOption
  columnDefinition *ColumnDefinition
  indexDefinition *IndexDefinition
  indexInfo     *IndexInfo
  indexColumn   *IndexColumn
  indexColumns  []*IndexColumn
  columnOptionListOpt ColumnOptionListOpt
  columnOptionList  ColumnOptionList
  columnOption      *ColumnOption
  databaseOptionListOpt DatabaseOptionListOpt
  databaseOptionList  DatabaseOptionList
  databaseOption      *DatabaseOption
  partitionDefinition *PartitionDefinition
  partitionDefinitions []*PartitionDefinition
  showFilter    *ShowFilter
}

%token LEX_ERROR
%left	<bytes>
	UNION

%token	<bytes>
	SELECT
	INSERT
	UPDATE
	DELETE
	FROM
	WHERE
	GROUP
	HAVING
	ORDER
	BY
	LIMIT
	OFFSET
	FOR


// FULLTEXT.
%token	<bytes>
	FULLTEXT
	PARSER
	NGRAM


// Resolve shift/reduce conflict on 'UNIQUE KEY', if we don`t define the precedence, the code
// doesn`t know which way to shift. Such as it can be parsed like 'UNIQUE' and 'KEY'(primary key),
// and also can be parsed like just 'UNIQUE KEY'.
// in mysql sql_yacc.cc, they are: %right UNIQUE_SYM KEY_SYM
// see: https://github.com/percona/percona-server/blob/8.0/sql/sql_yacc.yy#L1258
%right	<bytes>
	UNIQUE
	KEY

%token	<bytes>
	ALL
	DISTINCT
	AS
	EXISTS
	ASC
	DESC
	INTO
	DUPLICATE
	DEFAULT
	SET
	LOCK
	FULL
	CHECKSUM

%token	<bytes>
	VALUES
	LAST_INSERT_ID

%token	<bytes>
	NEXT
	VALUE
	SHARE
	MODE

%token	<bytes>
	SQL_NO_CACHE
	SQL_CACHE

%left	<bytes>
	JOIN
	STRAIGHT_JOIN
	LEFT
	RIGHT
	INNER
	OUTER
	CROSS
	NATURAL
	USE
	FORCE

%left	<bytes>
	ON

%token	<empty>
	'('
	','
	')'

%token	<bytes>
	ID
	HEX
	STRING
	INTEGRAL
	FLOAT
	HEXNUM
	VALUE_ARG
	LIST_ARG
	COMMENT
	COMMENT_KEYWORD

%token	<bytes>
	NULL
	TRUE
	FALSE
	OFF


// Precedence dictated by mysql. But the vitess grammar is simplified.
// Some of these operators don't conflict in our situation. Nevertheless,
// it's better to have these listed in the correct order. Also, we don't
// support all operators yet.
%left	<bytes>
	OR

%left	<bytes>
	AND

%right	<bytes>
	NOT
	'!'

%left	<bytes>
	BETWEEN
	CASE
	WHEN
	THEN
	ELSE
	END

%left	<bytes>
	'='
	'<'
	'>'
	LE
	GE
	NE
	NULL_SAFE_EQUAL
	IS
	LIKE
	REGEXP
	IN

%left	<bytes>
	'|'

%left	<bytes>
	'&'

%left	<bytes>
	SHIFT_LEFT
	SHIFT_RIGHT

%left	<bytes>
	'+'
	'-'

%left	<bytes>
	'*'
	'/'
	DIV
	'%'
	MOD

%left	<bytes>
	'^'

%right	<bytes>
	'~'
	UNARY

%left	<bytes>
	COLLATE

%right	<bytes>
	BINARY

%right	<bytes>
	INTERVAL

%nonassoc	<bytes>
	'.'


// There is no need to define precedence for the JSON
// operators because the syntax is restricted enough that
// they don't cause conflicts.
%token	<empty>
	JSON_EXTRACT_OP
	JSON_UNQUOTE_EXTRACT_OP


// DDL Tokens
%token	<bytes>
	CREATE
	ALTER
	DROP
	RENAME
	ANALYZE
	ADD
	MODIFY

%token	<bytes>
	TABLE
	INDEX
	VIEW
	TO
	IGNORE
	IF
	USING
	PRIMARY
	COLUMN

%token	<bytes>
	SHOW
	DESCRIBE
	EXPLAIN
	DATE
	ESCAPE
	REPAIR
	OPTIMIZE
	TRUNCATE


// Type Tokens
%token	<bytes>
	BIT
	TINYINT
	SMALLINT
	MEDIUMINT
	INT
	INTEGER
	BIGINT
	INTNUM

%token	<bytes>
	REAL
	DOUBLE
	FLOAT_TYPE
	DECIMAL
	NUMERIC

%token	<bytes>
	TIME
	TIMESTAMP
	DATETIME
	YEAR

%token	<bytes>
	CHAR
	VARCHAR
	BOOL
	CHARACTER
	VARBINARY
	NCHAR
	CHARSET

%token	<bytes>
	TEXT
	TINYTEXT
	MEDIUMTEXT
	LONGTEXT

%token	<bytes>
	BLOB
	TINYBLOB
	MEDIUMBLOB
	LONGBLOB
	JSON
	ENUM


// Type Modifiers
%token	<bytes>
	NULLX
	AUTO_INCREMENT
	APPROXNUM
	SIGNED
	UNSIGNED
	ZEROFILL


// Supported SHOW tokens
%token	<bytes>
	DATABASES
	TABLES
	WARNINGS
	VARIABLES
	EVENTS
	BINLOG
	GTID
	STATUS
	COLUMNS
	FIELDS


// Functions
%token	<bytes>
	CURRENT_TIMESTAMP
	DATABASE
	CURRENT_DATE

%token	<bytes>
	CURRENT_TIME
	LOCALTIME
	LOCALTIMESTAMP

%token	<bytes>
	UTC_DATE
	UTC_TIME
	UTC_TIMESTAMP

%token	<bytes>
	REPLACE

%token	<bytes>
	CONVERT
	CAST

%token	<bytes>
	GROUP_CONCAT
	SEPARATOR


// Match
%token	<bytes>
	MATCH
	AGAINST
	BOOLEAN
	LANGUAGE
	WITH
	QUERY
	EXPANSION


// MySQL reserved words that are unused by this grammar will map to this token.
%token	<bytes>
	UNUSED


// RadonDB
%token	<empty>
	PARTITION
	PARTITIONS
	HASH
	LIST
	XA
	DISTRIBUTED

%type	<statement>
	truncate_statement
	xa_statement
	explain_statement
	kill_statement
	transaction_statement
	radon_statement

%token	<bytes>
	ENGINES
	VERSIONS
	PROCESSLIST
	QUERYZ
	TXNZ
	KILL
	ENGINE
	SINGLE


// Transaction Tokens
%token	<bytes>
	BEGIN
	START
	TRANSACTION
	COMMIT
	ROLLBACK


// SET tokens
%token	<bytes>
	GLOBAL
	SESSION
	NAMES


// Radon Tokens
%token	<bytes>
	RADON
	ATTACH
	ATTACHLIST
	DETACH
	RESHARD
	CLEANUP

%type	<statement>
	command

%type	<selStmt>
	select_statement
	base_select
	union_lhs
	union_rhs

%type	<statement>
	insert_statement
	update_statement
	delete_statement
	set_statement

%type	<statement>
	create_statement
	alter_statement
	drop_statement

%type	<ddl>
	create_table_prefix

%type	<statement>
	analyze_statement
	show_statement
	use_statement
	other_statement
	checksum_statement

%type	<bytes2>
	comment_opt
	comment_list

%type	<str>
	union_op
	insert_or_replace

%type	<str>
	distinct_opt
	straight_join_opt
	cache_opt
	match_option
	separator_opt
	binlog_from_opt

%type	<expr>
	like_escape_opt

%type	<selectExprs>
	select_expression_list
	select_expression_list_opt

%type	<selectExpr>
	select_expression

%type	<expr>
	expression

%type	<tableExprs>
	from_opt
	table_references

%type	<tableExpr>
	table_reference
	table_factor
	join_table

%type	<str>
	inner_join
	outer_join
	natural_join

%type	<tableName>
	table_name
	into_table_name
	database_from_opt

%type	<str>
	full_opt
	columns_or_fields

%type	<showFilter>
	like_or_where_opt

%type	<tableNames>
	table_name_list

%type	<aliasedTableName>
	aliased_table_name

%type	<indexHints>
	index_hint_list

%type	<colIdents>
	index_list

%type	<expr>
	where_expression_opt

%type	<expr>
	condition

%type	<boolVal>
	boolean_value

%type	<str>
	compare

%type	<ins>
	insert_data

%type	<expr>
	value
	value_expression
	num_val

%type	<expr>
	function_call_keyword
	function_call_nonkeyword
	function_call_generic
	function_call_conflict

%type	<str>
	is_suffix

%type	<colTuple>
	col_tuple

%type	<exprs>
	expression_list

%type	<values>
	tuple_list

%type	<valTuple>
	row_tuple
	tuple_or_empty

%type	<expr>
	tuple_expression

%type	<subquery>
	subquery

%type	<colName>
	column_name

%type	<whens>
	when_expression_list

%type	<when>
	when_expression

%type	<expr>
	expression_opt
	else_expression_opt

%type	<exprs>
	group_by_opt

%type	<expr>
	having_opt

%type	<orderBy>
	order_by_opt
	order_list

%type	<order>
	order

%type	<str>
	asc_desc_opt

%type	<limit>
	limit_opt

%type	<str>
	lock_opt

%type	<columns>
	ins_column_list

%type	<updateExprs>
	on_dup_opt

%type	<updateExprs>
	update_list

%type	<updateExpr>
	update_expression

%type	<setExprs>
	set_list

%type	<setExpr>
	set_expression

%type	<expr>
	charset_value

%type	<bytes>
	charset_or_character_set

%type	<bytes>
	for_from

%type	<str>
	ignore_opt
	default_opt

%type	<byt>
	exists_opt
	not_exists_opt

%type	<empty>
	non_rename_operation
	to_opt
	index_opt
	constraint_opt

%type	<bytes>
	reserved_keyword
	non_reserved_keyword

%type	<colIdent>
	sql_id
	reserved_sql_id
	col_alias
	as_ci_opt
	col_id

%type	<tableIdent>
	table_id
	reserved_table_id
	table_alias
	as_opt_id

%type	<empty>
	as_opt

%type	<empty>
	force_eof
	ddl_force_eof

%type	<str>
	charset

%type	<str>
	set_session_or_global

%type	<convertType>
	convert_type

%type	<columnType>
	column_type

%type	<columnType>
	int_type
	decimal_type
	numeric_type
	time_type
	char_type

%type	<optVal>
	length_opt
	column_default_opt
	on_update_opt
	column_comment_opt
	table_comment_opt
	engine_option
	charset_option
	tabletype_option
	auto_opt
	parts_num_opt

%type	<str>
	charset_opt
	collate_opt

%type	<optVal>
	id_or_string

%type	<str>
	collate_name_or_default
	opt_charset
	opt_equal
	opt_default
	charset_name_or_default

%type	<boolVal>
	unsigned_opt
	zero_fill_opt

%type	<LengthScaleOption>
	float_length_opt
	decimal_length_opt

%type	<boolVal>
	null_opt
	auto_increment_opt

%type	<colPrimaryKeyOpt>
	column_primary_key_opt

%type	<colUniqueKeyOpt>
	column_unique_key_opt

%type	<strs>
	enum_values

%type	<columnDefinition>
	column_definition

%type	<indexDefinition>
	index_definition

%type	<str>
	index_or_key

%type	<TableSpec>
	table_spec
	table_column_list

%type	<TableOptionListOpt>
	table_option_list_opt

%type	<TableOptionList>
	table_option_list

%type	<tableOption>
	table_option

%type	<indexInfo>
	index_info

%type	<indexColumn>
	index_column

%type	<indexColumns>
	index_column_list

%type	<columnOptionListOpt>
	column_option_list_opt

%type	<columnOptionList>
	column_option_list

%type	<columnOption>
	column_option

%type	<databaseOptionListOpt>
	database_option_list_opt

%type	<databaseOptionList>
	database_option_list

%type	<databaseOption>
	database_option

%type	<partitionDefinition>
	partition_definition

%type	<partitionDefinitions>
	partition_definitions


%start	any_command

%%

any_command:
	command semicolon_opt
	{
		setParseTree(yylex, $1)
	}

semicolon_opt:
	/*empty*/
	{}
|	';'
	{}

command:
	select_statement
	{
		$$ = $1
	}
|	insert_statement
|	update_statement
|	delete_statement
|	set_statement
|	create_statement
|	alter_statement
|	drop_statement
|	truncate_statement
|	analyze_statement
|	show_statement
|	checksum_statement
|	use_statement
|	xa_statement
|	explain_statement
|	kill_statement
|	transaction_statement
|	radon_statement
|	other_statement

select_statement:
	base_select order_by_opt limit_opt lock_opt
	{
		sel := $1.(*Select)
		sel.OrderBy = $2
		sel.Limit = $3
		sel.Lock = $4
		$$ = sel
	}
|	union_lhs union_op union_rhs order_by_opt limit_opt lock_opt
	{
		$$ = &Union{Type: $2, Left: $1, Right: $3, OrderBy: $4, Limit: $5, Lock: $6}
	}
|	SELECT comment_opt cache_opt NEXT num_val for_from table_name
	{
		$$ = &Select{Comments: Comments($2), Cache: $3, SelectExprs: SelectExprs{Nextval{Expr: $5}}, From: TableExprs{&AliasedTableExpr{Expr: $7}}}
	}

// base_select is an unparenthesized SELECT with no order by clause or beyond.
base_select:
	SELECT comment_opt cache_opt distinct_opt straight_join_opt select_expression_list from_opt where_expression_opt group_by_opt having_opt
	{
		$$ = &Select{Comments: Comments($2), Cache: $3, Distinct: $4, Hints: $5, SelectExprs: $6, From: $7, Where: NewWhere(WhereStr, $8), GroupBy: GroupBy($9), Having: NewWhere(HavingStr, $10)}
	}

union_lhs:
	select_statement
	{
		$$ = $1
	}
|	openb select_statement closeb
	{
		$$ = &ParenSelect{Select: $2}
	}

union_rhs:
	base_select
	{
		$$ = $1
	}
|	openb select_statement closeb
	{
		$$ = &ParenSelect{Select: $2}
	}

insert_statement:
	insert_or_replace comment_opt ignore_opt into_table_name insert_data on_dup_opt
	{
		// insert_data returns a *Insert pre-filled with Columns & Values
		ins := $5
		ins.Action = $1
		ins.Comments = $2
		ins.Ignore = $3
		ins.Table = $4
		ins.OnDup = OnDup($6)
		$$ = ins
	}
|	insert_or_replace comment_opt ignore_opt into_table_name SET update_list on_dup_opt
	{
		cols := make(Columns, 0, len($6))
		vals := make(ValTuple, 0, len($7))
		for _, updateList := range $6 {
			cols = append(cols, updateList.Name.Name)
			vals = append(vals, updateList.Expr)
		}
		$$ = &Insert{Action: $1, Comments: Comments($2), Ignore: $3, Table: $4, Columns: cols, Rows: Values{vals}, OnDup: OnDup($7)}
	}

insert_or_replace:
	INSERT
	{
		$$ = InsertStr
	}
|	REPLACE
	{
		$$ = ReplaceStr
	}

update_statement:
	UPDATE comment_opt table_name SET update_list where_expression_opt order_by_opt limit_opt
	{
		$$ = &Update{Comments: Comments($2), Table: $3, Exprs: $5, Where: NewWhere(WhereStr, $6), OrderBy: $7, Limit: $8}
	}

delete_statement:
	DELETE comment_opt FROM table_name where_expression_opt order_by_opt limit_opt
	{
		$$ = &Delete{Comments: Comments($2), Table: $4, Where: NewWhere(WhereStr, $5), OrderBy: $6, Limit: $7}
	}

set_statement:
	SET comment_opt set_list
	{
		$$ = &Set{Comments: Comments($2), Exprs: $3}
	}
|	SET comment_opt set_session_or_global set_list
	{
		$$ = &Set{Comments: Comments($2), Exprs: $4}
	}

set_session_or_global:
	SESSION
	{
		$$ = SessionStr
	}
|	GLOBAL
	{
		$$ = GlobalStr
	}

partition_definitions:
	partition_definition
	{
		$$ = []*PartitionDefinition{$1}
	}
|	partition_definitions ',' partition_definition
	{
		$$ = append($1, $3)
	}

partition_definition:
	PARTITION ID VALUES IN row_tuple
	{
		$$ = &PartitionDefinition{Backend: string($2), Row: $5}
	}

parts_num_opt:
	/* empty */
	{
		$$ = nil
	}
|	PARTITIONS INTEGRAL
	{
		if string($2) == "0" {
			yylex.Error("Number of partitions must be a positive integer")
			return 1
		}
		$$ = NewIntVal($2)
	}

create_statement:
	create_table_prefix table_spec
	{
		$1.Action = CreateTableStr
		$1.TableSpec = $2
		$$ = $1
	}
|	create_table_prefix table_spec PARTITION BY HASH openb col_id closeb parts_num_opt ddl_force_eof
	{
		$1.Action = CreateTableStr
		$1.TableSpec = $2
		$1.PartitionName = $7.String()
		$1.PartitionNum = $9
		if $2.Options.Type == GlobalTableType || $2.Options.Type == SingleTableType {
			yylex.Error("SINGLE or GLOBAL should not be used simultaneously with PARTITION")
			return 1
		} else {
			$1.TableSpec.Options.Type = PartitionTableHash
		}
		$$ = $1
	}
|	create_table_prefix table_spec PARTITION BY LIST openb col_id closeb openb partition_definitions closeb ddl_force_eof
	{
		$1.Action = CreateTableStr
		$1.TableSpec = $2
		$1.PartitionName = $7.String()
		$1.TableSpec.Options.Type = PartitionTableList
		$1.PartitionOptions = $10
		$$ = $1
	}
|	create_table_prefix table_spec DISTRIBUTED BY openb col_id closeb ddl_force_eof
	{
		$1.Action = CreateTableStr
		$1.TableSpec = $2
		$1.BackendName = $6.String()
		if $2.Options.Type == GlobalTableType || $2.Options.Type == SingleTableType {
			yylex.Error("SINGLE or GLOBAL should not be used simultaneously with DISTRIBUTED")
			return 1
		} else {
			$1.TableSpec.Options.Type = SingleTableType
		}
		$$ = $1
	}
|	CREATE DATABASE not_exists_opt table_id database_option_list_opt
	{
		var ifnotexists bool
		if $3 != 0 {
			ifnotexists = true
		}
		$$ = &DDL{Action: CreateDBStr, IfNotExists: ifnotexists, Database: $4, DatabaseOptions: $5}
	}
|	CREATE constraint_opt INDEX ID ON table_name ddl_force_eof
	{
		// Change this to an alter statement
		$$ = &DDL{Action: CreateIndexStr, IndexName: string($4), Table: $6, NewName: $6}
	}

database_option_list_opt:
	{
		$$.DBOptList = []*DatabaseOption{}
	}
|	database_option_list
	{
		$$.DBOptList = $1
	}

database_option_list:
	database_option
	{
		$$ = append($$, $1)
	}
|	database_option_list database_option
	{
		$$ = append($1, $2)
	}

database_option:
	opt_default COLLATE opt_equal collate_name_or_default
	{
		$$ = &DatabaseOption{
			CharsetOrCollate: string($2),
			Value:            $4,
		}
	}
|	opt_default opt_charset opt_equal charset_name_or_default
	{
		$$ = &DatabaseOption{
			CharsetOrCollate: string($2),
			Value:            $4,
		}
	}

opt_default:
	{}
|	DEFAULT
	{}

opt_equal:
	{}
|	'='
	{}

opt_charset:
	CHARSET
	{
		$$ = string($1)
	}
|	CHARACTER SET
	{
		$$ = "character set"
	}

collate_name_or_default:
	ID
	{
		$$ = string($1)
	}
|	DEFAULT
	{
		$$ = "default"
	}

charset_name_or_default:
	ID
	{
		$$ = string($1)
	}
|	BINARY
	{
		$$ = string($1)
	}
|	DEFAULT
	{
		$$ = "default"
	}

create_table_prefix:
	CREATE TABLE not_exists_opt table_name
	{
		var ifnotexists bool
		if $3 != 0 {
			ifnotexists = true
		}
		$$ = &DDL{Action: CreateTableStr, IfNotExists: ifnotexists, Table: $4, NewName: $4}
		setDDL(yylex, $$)
	}

table_spec:
	'(' table_column_list ')' table_option_list_opt
	{
		$$ = $2

		if len($4.TblOptList) != 0 {
			if str := $4.CheckIfTableOptDuplicate(); str != "" {
				yylex.Error(str)
				return 1
			}
			if val := $4.GetTableOptValByType(TableOptionComment); val != nil {
				$$.Options.Comment = String(val)
			}
			if val := $4.GetTableOptValByType(TableOptionEngine); val != nil {
				$$.Options.Engine = String(val)
			}
			if val := $4.GetTableOptValByType(TableOptionCharset); val != nil {
				$$.Options.Charset = String(val)
			}
			if val := $4.GetTableOptValByType(TableOptionTableType); val != nil {
				$$.Options.Type = String(val)
			}
		}
		if $$.Options.Type == "" {
			$$.Options.Type = NormalTableType
		}
	}

table_option_list_opt:
	{
		$$.TblOptList = []*TableOption{}
	}
|	table_option_list
	{
		$$.TblOptList = $1
	}

table_option_list:
	table_option
	{
		$$ = append($$, $1)
	}
|	table_option_list table_option
	{
		$$ = append($1, $2)
	}

table_option:
	table_comment_opt
	{
		$$ = &TableOption{
			Type: TableOptionComment,
			Val:  $1,
		}
	}
|	engine_option
	{
		$$ = &TableOption{
			Type: TableOptionEngine,
			Val:  $1,
		}
	}
|	charset_option
	{
		$$ = &TableOption{
			Type: TableOptionCharset,
			Val:  $1,
		}
	}
|	tabletype_option
	{
		$$ = &TableOption{
			Type: TableOptionTableType,
			Val:  $1,
		}
	}
|	auto_opt
	{
		$$ = &TableOption{
			Type: TableOptionAutoInc,
			Val:  $1,
		}
	}

auto_opt:
	AUTO_INCREMENT opt_equal INTEGRAL
	{}

id_or_string:
	ID
	{
		// Normal str as a identify, without quote
		$$ = NewStrValWithoutQuote($1)
	}
|	STRING
	{
		// Str with Quote, it will be parsed by Lex begin with quote \' or \"
		$$ = NewStrVal($1)
	}

table_comment_opt:
	COMMENT_KEYWORD opt_equal STRING
	{
		$$ = NewStrVal($3)
	}

engine_option:
	ENGINE opt_equal id_or_string
	{
		$$ = $3
	}

charset_option:
	opt_default opt_charset opt_equal id_or_string
	{
		$$ = $4
	}

tabletype_option:
	GLOBAL
	{
		$$ = NewStrValWithoutQuote([]byte(GlobalTableType))
	}
|	SINGLE
	{
		$$ = NewStrValWithoutQuote([]byte(SingleTableType))
	}

table_column_list:
	column_definition
	{
		$$ = &TableSpec{}
		$$.AddColumn($1)
	}
|	table_column_list ',' column_definition
	{
		$$.AddColumn($3)
	}
|	table_column_list ',' index_definition
	{
		$$.AddIndex($3)
	}

column_definition:
	col_id column_type column_option_list_opt
	{
		$2.NotNull = $3.GetColumnOption(ColumnOptionNotNull).NotNull
		$2.Autoincrement = $3.GetColumnOption(ColumnOptionAutoincrement).Autoincrement
		$2.Default = $3.GetColumnOption(ColumnOptionDefault).Default
		$2.Comment = $3.GetColumnOption(ColumnOptionComment).Comment
		$2.OnUpdate = $3.GetColumnOption(ColumnOptionOnUpdate).OnUpdate
		$2.PrimaryKeyOpt = $3.GetColumnOption(ColumnOptionKeyPrimaryOpt).PrimaryKeyOpt
		$2.UniqueKeyOpt = $3.GetColumnOption(ColumnOptionKeyUniqueOpt).UniqueKeyOpt
		$$ = &ColumnDefinition{Name: $1, Type: $2}
	}

col_id:
	ID
	{
		$$ = NewColIdent(string($1))
	}
|	non_reserved_keyword
	{
		$$ = NewColIdent(string($1))
	}

column_type:
	numeric_type unsigned_opt zero_fill_opt
	{
		$$ = $1
		$$.Unsigned = $2
		$$.Zerofill = $3
	}
|	char_type
|	time_type

column_option_list_opt:
	{
		$$.ColOptList = []*ColumnOption{}
	}
|	column_option_list
	{
		$$.ColOptList = $1
	}

column_option_list:
	column_option
	{
		$$ = append($$, $1)
	}
|	column_option_list column_option
	{
		$$ = append($1, $2)
	}

column_option:
	null_opt
	{
		$$ = &ColumnOption{
			typ:     ColumnOptionNotNull,
			NotNull: $1,
		}
	}
|	column_default_opt
	{
		$$ = &ColumnOption{
			typ:     ColumnOptionDefault,
			Default: $1,
		}
	}
|	auto_increment_opt
	{
		$$ = &ColumnOption{
			typ:           ColumnOptionAutoincrement,
			Autoincrement: $1,
		}
	}
|	column_primary_key_opt
	{
		$$ = &ColumnOption{
			typ:           ColumnOptionKeyPrimaryOpt,
			PrimaryKeyOpt: $1,
		}
	}
|	column_unique_key_opt
	{
		$$ = &ColumnOption{
			typ:          ColumnOptionKeyUniqueOpt,
			UniqueKeyOpt: $1,
		}
	}
|	column_comment_opt
	{
		$$ = &ColumnOption{
			typ:     ColumnOptionComment,
			Comment: $1,
		}
	}
|	on_update_opt
	{
		$$ = &ColumnOption{
			typ:      ColumnOptionOnUpdate,
			OnUpdate: $1,
		}
	}

numeric_type:
	int_type length_opt
	{
		$$ = $1
		$$.Length = $2
	}
|	decimal_type
	{
		$$ = $1
	}

int_type:
	BIT
	{
		$$ = ColumnType{Type: string($1)}
	}
|	TINYINT
	{
		$$ = ColumnType{Type: string($1)}
	}
|	SMALLINT
	{
		$$ = ColumnType{Type: string($1)}
	}
|	MEDIUMINT
	{
		$$ = ColumnType{Type: string($1)}
	}
|	INT
	{
		$$ = ColumnType{Type: string($1)}
	}
|	INTEGER
	{
		$$ = ColumnType{Type: string($1)}
	}
|	BIGINT
	{
		$$ = ColumnType{Type: string($1)}
	}

decimal_type:
	REAL float_length_opt
	{
		$$ = ColumnType{Type: string($1)}
		$$.Length = $2.Length
		$$.Scale = $2.Scale
	}
|	DOUBLE float_length_opt
	{
		$$ = ColumnType{Type: string($1)}
		$$.Length = $2.Length
		$$.Scale = $2.Scale
	}
|	FLOAT_TYPE float_length_opt
	{
		$$ = ColumnType{Type: string($1)}
		$$.Length = $2.Length
		$$.Scale = $2.Scale
	}
|	DECIMAL decimal_length_opt
	{
		$$ = ColumnType{Type: string($1)}
		$$.Length = $2.Length
		$$.Scale = $2.Scale
	}
|	NUMERIC decimal_length_opt
	{
		$$ = ColumnType{Type: string($1)}
		$$.Length = $2.Length
		$$.Scale = $2.Scale
	}

time_type:
	DATE
	{
		$$ = ColumnType{Type: string($1)}
	}
|	TIME length_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2}
	}
|	TIMESTAMP length_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2}
	}
|	DATETIME length_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2}
	}
|	YEAR
	{
		$$ = ColumnType{Type: string($1)}
	}

char_type:
	CHAR length_opt charset_opt collate_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2, Charset: $3, Collate: $4}
	}
|	VARCHAR length_opt charset_opt collate_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2, Charset: $3, Collate: $4}
	}
|	BINARY length_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2}
	}
|	VARBINARY length_opt
	{
		$$ = ColumnType{Type: string($1), Length: $2}
	}
|	TEXT charset_opt collate_opt
	{
		$$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
	}
|	TINYTEXT charset_opt collate_opt
	{
		$$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
	}
|	MEDIUMTEXT charset_opt collate_opt
	{
		$$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
	}
|	LONGTEXT charset_opt collate_opt
	{
		$$ = ColumnType{Type: string($1), Charset: $2, Collate: $3}
	}
|	BLOB
	{
		$$ = ColumnType{Type: string($1)}
	}
|	TINYBLOB
	{
		$$ = ColumnType{Type: string($1)}
	}
|	MEDIUMBLOB
	{
		$$ = ColumnType{Type: string($1)}
	}
|	LONGBLOB
	{
		$$ = ColumnType{Type: string($1)}
	}
|	JSON
	{
		$$ = ColumnType{Type: string($1)}
	}
|	ENUM '(' enum_values ')'
	{
		$$ = ColumnType{Type: string($1), EnumValues: $3}
	}

enum_values:
	STRING
	{
		$$ = make([]string, 0, 4)
		$$ = append($$, "'"+string($1)+"'")
	}
|	enum_values ',' STRING
	{
		$$ = append($1, "'"+string($3)+"'")
	}

length_opt:
	{
		$$ = nil
	}
|	'(' INTEGRAL ')'
	{
		$$ = NewIntVal($2)
	}

float_length_opt:
	{
		$$ = LengthScaleOption{}
	}
|	'(' INTEGRAL ',' INTEGRAL ')'
	{
		$$ = LengthScaleOption{
			Length: NewIntVal($2),
			Scale:  NewIntVal($4),
		}
	}

decimal_length_opt:
	{
		$$ = LengthScaleOption{}
	}
|	'(' INTEGRAL ')'
	{
		$$ = LengthScaleOption{
			Length: NewIntVal($2),
		}
	}
|	'(' INTEGRAL ',' INTEGRAL ')'
	{
		$$ = LengthScaleOption{
			Length: NewIntVal($2),
			Scale:  NewIntVal($4),
		}
	}

unsigned_opt:
	{
		$$ = BoolVal(false)
	}
|	UNSIGNED
	{
		$$ = BoolVal(true)
	}

zero_fill_opt:
	{
		$$ = BoolVal(false)
	}
|	ZEROFILL
	{
		$$ = BoolVal(true)
	}

// Null opt returns false to mean NULL (i.e. the default) and true for NOT NULL
null_opt:
	NULL
	{
		$$ = BoolVal(false)
	}
|	NOT NULL
	{
		$$ = BoolVal(true)
	}

column_default_opt:
	DEFAULT STRING
	{
		$$ = NewStrVal($2)
	}
|	DEFAULT INTEGRAL
	{
		$$ = NewIntVal($2)
	}
|	DEFAULT FLOAT
	{
		$$ = NewFloatVal($2)
	}
|	DEFAULT NULL
	{
		$$ = NewValArg($2)
	}
|	DEFAULT CURRENT_TIMESTAMP
	{
		$$ = NewValArg($2)
	}

on_update_opt:
	ON UPDATE CURRENT_TIMESTAMP
	{
		$$ = NewValArg($3)
	}

auto_increment_opt:
	AUTO_INCREMENT
	{
		$$ = BoolVal(true)
	}

charset_opt:
	{
		$$ = ""
	}
|	CHARACTER SET ID
	{
		$$ = string($3)
	}
|	CHARACTER SET BINARY
	{
		$$ = string($3)
	}

collate_opt:
	{
		$$ = ""
	}
|	COLLATE ID
	{
		$$ = string($2)
	}

column_primary_key_opt:
	PRIMARY KEY
	{
		$$ = ColKeyPrimary
	}
|	KEY
	{
		// KEY is normally a synonym for INDEX. The key attribute PRIMARY KEY
		// can also be specified as just KEY when given in a column definition.
		// See http://dev.mysql.com/doc/refman/5.7/en/create-table.html
		$$ = ColKeyPrimary
	}

column_unique_key_opt:
	UNIQUE KEY
	{
		$$ = ColKeyUniqueKey
	}
|	UNIQUE
	{
		$$ = ColKeyUniqueKey
	}

column_comment_opt:
	COMMENT_KEYWORD STRING
	{
		$$ = NewStrVal($2)
	}

index_definition:
	index_info '(' index_column_list ')'
	{
		$$ = &IndexDefinition{Info: $1, Columns: $3}
	}
|	index_info '(' index_column_list ')' WITH PARSER NGRAM
	{
		$$ = &IndexDefinition{Info: $1, Columns: $3}
	}

index_info:
	PRIMARY KEY
	{
		$$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent("PRIMARY"), Primary: true, Unique: true}
	}
|	UNIQUE index_or_key ID
	{
		$$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent(string($3)), Primary: false, Unique: true}
	}
|	UNIQUE ID
	{
		$$ = &IndexInfo{Type: string($1), Name: NewColIdent(string($2)), Primary: false, Unique: true}
	}
|	index_or_key ID
	{
		$$ = &IndexInfo{Type: string($1), Name: NewColIdent(string($2)), Primary: false, Unique: false}
	}
|	FULLTEXT index_or_key ID
	{
		$$ = &IndexInfo{Type: string($1) + " " + string($2), Name: NewColIdent(string($3)), Primary: false, Unique: false, Fulltext: true}
	}

index_or_key:
	INDEX
	{
		$$ = string($1)
	}
|	KEY
	{
		$$ = string($1)
	}

index_column_list:
	index_column
	{
		$$ = []*IndexColumn{$1}
	}
|	index_column_list ',' index_column
	{
		$$ = append($$, $3)
	}

index_column:
	sql_id length_opt
	{
		$$ = &IndexColumn{Column: $1, Length: $2}
	}

alter_statement:
	ALTER ignore_opt TABLE table_name non_rename_operation force_eof
	{
		$$ = &DDL{Action: AlterStr, Table: $4, NewName: $4}
	}
|	ALTER ignore_opt TABLE table_name RENAME to_opt table_name
	{
		// Change this to a rename statement
		$$ = &DDL{Action: RenameStr, Table: $4, NewName: $7}
	}
|	ALTER ignore_opt TABLE table_name RENAME index_opt force_eof
	{
		// Rename an index can just be an alter
		$$ = &DDL{Action: AlterStr, Table: $4, NewName: $4}
	}
|	ALTER ignore_opt TABLE table_name ENGINE '=' ID
	{
		$$ = &DDL{Action: AlterEngineStr, Table: $4, NewName: $4, Engine: string($7)}
	}
|	ALTER ignore_opt TABLE table_name CONVERT TO CHARACTER SET ID
	{
		$$ = &DDL{Action: AlterCharsetStr, Table: $4, NewName: $4, Charset: string($9)}
	}
|	ALTER ignore_opt TABLE table_name ADD COLUMN table_spec
	{
		$$ = &DDL{Action: AlterAddColumnStr, Table: $4, NewName: $4, TableSpec: $7}
	}
|	ALTER ignore_opt TABLE table_name DROP COLUMN ID
	{
		$$ = &DDL{Action: AlterDropColumnStr, Table: $4, NewName: $4, DropColumnName: string($7)}
	}
|	ALTER ignore_opt TABLE table_name MODIFY COLUMN column_definition
	{
		$$ = &DDL{Action: AlterModifyColumnStr, Table: $4, NewName: $4, ModifyColumnDef: $7}
	}

drop_statement:
	DROP TABLE exists_opt table_name_list
	{
		var exists bool
		if $3 != 0 {
			exists = true
		}
		$$ = &DDL{Action: DropTableStr, Tables: $4, IfExists: exists}
	}
|	DROP INDEX ID ON table_name
	{
		// Change this to an alter statement
		$$ = &DDL{Action: DropIndexStr, IndexName: string($3), Table: $5, NewName: $5}
	}
|	DROP DATABASE exists_opt table_id
	{
		var exists bool
		if $3 != 0 {
			exists = true
		}
		$$ = &DDL{Action: DropDBStr, Database: $4, IfExists: exists}
	}

table_name_list:
	table_name
	{
		$$ = TableNames{$1}
	}
|	table_name_list ',' table_name
	{
		$$ = append($$, $3)
	}

truncate_statement:
	TRUNCATE TABLE table_name
	{
		$$ = &DDL{Action: TruncateTableStr, Table: $3, NewName: $3}
	}

analyze_statement:
	ANALYZE TABLE table_name
	{
		$$ = &DDL{Action: AlterStr, Table: $3, NewName: $3}
	}

xa_statement:
	XA force_eof
	{
		$$ = &Xa{}
	}

explain_statement:
	EXPLAIN force_eof
	{
		$$ = &Explain{}
	}

kill_statement:
	KILL INTEGRAL force_eof
	{
		$$ = &Kill{QueryID: &NumVal{raw: string($2)}}
	}
|	KILL QUERY INTEGRAL force_eof
	{
		$$ = &Kill{QueryID: &NumVal{raw: string($3)}}
	}

transaction_statement:
	BEGIN force_eof
	{
		$$ = &Transaction{Action: BeginTxnStr}
	}
|	START TRANSACTION force_eof
	{
		$$ = &Transaction{Action: StartTxnStr}
	}
|	ROLLBACK force_eof
	{
		$$ = &Transaction{Action: RollbackTxnStr}
	}
|	COMMIT force_eof
	{
		$$ = &Transaction{Action: CommitTxnStr}
	}

radon_statement:
	RADON ATTACH row_tuple force_eof
	{
		$$ = &Radon{Action: AttachStr, Row: $3}
	}
|	RADON DETACH row_tuple force_eof
	{
		$$ = &Radon{Action: DetachStr, Row: $3}
	}
|	RADON ATTACHLIST force_eof
	{
		$$ = &Radon{Action: AttachListStr}
	}
|	RADON RESHARD table_name to_opt table_name force_eof
	{
		$$ = &Radon{Action: ReshardStr, Table: $3, NewName: $5}
	}
|   RADON CLEANUP force_eof
    {
		$$ = &Radon{Action: CleanupStr}
	}

show_statement:
	SHOW BINLOG EVENTS binlog_from_opt limit_opt force_eof
	{
		$$ = &Show{Type: ShowBinlogEventsStr, From: $4, Limit: $5}
	}
|	SHOW CREATE TABLE table_name force_eof
	{
		$$ = &Show{Type: ShowCreateTableStr, Table: $4}
	}
|	SHOW CREATE DATABASE table_name force_eof
	{
		$$ = &Show{Type: ShowCreateDatabaseStr, Database: $4}
	}
|	SHOW DATABASES force_eof
	{
		$$ = &Show{Type: ShowDatabasesStr}
	}
|	SHOW ENGINES force_eof
	{
		$$ = &Show{Type: ShowEnginesStr}
	}
|	SHOW full_opt TABLES database_from_opt like_or_where_opt
	{
		$$ = &Show{Full: $2, Type: ShowTablesStr, Database: $4, Filter: $5}
	}
|	SHOW full_opt columns_or_fields FROM table_name like_or_where_opt
	{
		$$ = &Show{Full: $2, Type: ShowColumnsStr, Table: $5, Filter: $6}
	}
|	SHOW PROCESSLIST force_eof
	{
		$$ = &Show{Type: ShowProcesslistStr}
	}
|	SHOW QUERYZ force_eof
	{
		$$ = &Show{Type: ShowQueryzStr}
	}
|	SHOW STATUS force_eof
	{
		$$ = &Show{Type: ShowStatusStr}
	}
|	SHOW TABLE STATUS database_from_opt force_eof
	{
		$$ = &Show{Type: ShowTableStatusStr, Database: $4}
	}
|	SHOW TXNZ force_eof
	{
		$$ = &Show{Type: ShowTxnzStr}
	}
|	SHOW VARIABLES force_eof
	{
		$$ = &Show{Type: ShowVariablesStr}
	}
|	SHOW VERSIONS force_eof
	{
		$$ = &Show{Type: ShowVersionsStr}
	}
|	SHOW WARNINGS force_eof
	{
		$$ = &Show{Type: ShowWarningsStr}
	}
|	SHOW ID force_eof
	{
		$$ = &Show{Type: ShowUnsupportedStr}
	}

binlog_from_opt:
	{
		$$ = ""
	}
|	FROM GTID STRING
	{
		$$ = string($3)
	}

database_from_opt:
	{
		$$ = TableName{}
	}
|	FROM table_name
	{
		$$ = $2
	}

full_opt:
	/* empty */
	{
		$$ = ""
	}
|	FULL
	{
		$$ = "full "
	}

columns_or_fields:
	COLUMNS
	{
		$$ = string($1)
	}
|	FIELDS
	{
		$$ = string($1)
	}

like_or_where_opt:
	/* empty */
	{
		$$ = nil
	}
|	LIKE STRING
	{
		$$ = &ShowFilter{Like: string($2)}
	}
|	WHERE expression
	{
		$$ = &ShowFilter{Filter: $2}
	}

checksum_statement:
	CHECKSUM TABLE table_name force_eof
	{
		$$ = &Checksum{Table: $3}
	}

use_statement:
	USE table_id
	{
		$$ = &Use{DBName: $2}
	}

other_statement:
	DESC force_eof
	{
		$$ = &OtherRead{}
	}
|	DESCRIBE force_eof
	{
		$$ = &OtherRead{}
	}
|	REPAIR force_eof
	{
		$$ = &OtherAdmin{}
	}
|	OPTIMIZE force_eof
	{
		$$ = &OtherAdmin{}
	}

comment_opt:
	{
		setAllowComments(yylex, true)
	} comment_list
	{
		$$ = $2
		setAllowComments(yylex, false)
	}

comment_list:
	{
		$$ = nil
	}
|	comment_list COMMENT
	{
		$$ = append($1, $2)
	}

union_op:
	UNION
	{
		$$ = UnionStr
	}
|	UNION ALL
	{
		$$ = UnionAllStr
	}
|	UNION DISTINCT
	{
		$$ = UnionDistinctStr
	}

cache_opt:
	{
		$$ = ""
	}
|	SQL_NO_CACHE
	{
		$$ = SQLNoCacheStr
	}
|	SQL_CACHE
	{
		$$ = SQLCacheStr
	}

distinct_opt:
	{
		$$ = ""
	}
|	DISTINCT
	{
		$$ = DistinctStr
	}

straight_join_opt:
	{
		$$ = ""
	}
|	STRAIGHT_JOIN
	{
		$$ = StraightJoinHint
	}

select_expression_list_opt:
	{
		$$ = nil
	}
|	select_expression_list
	{
		$$ = $1
	}

select_expression_list:
	select_expression
	{
		$$ = SelectExprs{$1}
	}
|	select_expression_list ',' select_expression
	{
		$$ = append($$, $3)
	}

select_expression:
	'*'
	{
		$$ = &StarExpr{}
	}
|	expression as_ci_opt
	{
		$$ = &AliasedExpr{Expr: $1, As: $2}
	}
|	table_id '.' '*'
	{
		$$ = &StarExpr{TableName: TableName{Name: $1}}
	}
|	table_id '.' reserved_table_id '.' '*'
	{
		$$ = &StarExpr{TableName: TableName{Qualifier: $1, Name: $3}}
	}

as_ci_opt:
	{
		$$ = ColIdent{}
	}
|	col_alias
	{
		$$ = $1
	}
|	AS col_alias
	{
		$$ = $2
	}

col_alias:
	sql_id
|	STRING
	{
		$$ = NewColIdent(string($1))
	}

from_opt:
	{
		$$ = TableExprs{&AliasedTableExpr{Expr: TableName{Name: NewTableIdent("dual")}}}
	}
|	FROM table_references
	{
		$$ = $2
	}

table_references:
	table_reference
	{
		$$ = TableExprs{$1}
	}
|	table_references ',' table_reference
	{
		$$ = append($$, $3)
	}

table_reference:
	table_factor
|	join_table

table_factor:
	aliased_table_name
	{
		$$ = $1
	}
|	subquery as_opt table_id
	{
		$$ = &AliasedTableExpr{Expr: $1, As: $3}
	}
|	openb table_references closeb
	{
		$$ = &ParenTableExpr{Exprs: $2}
	}

aliased_table_name:
	table_name as_opt_id index_hint_list
	{
		$$ = &AliasedTableExpr{Expr: $1, As: $2, Hints: $3}
	}

// There is a grammar conflict here:
// 1: INSERT INTO a SELECT * FROM b JOIN c ON b.i = c.i
// 2: INSERT INTO a SELECT * FROM b JOIN c ON DUPLICATE KEY UPDATE a.i = 1
// When yacc encounters the ON clause, it cannot determine which way to
// resolve. The %prec override below makes the parser choose the
// first construct, which automatically makes the second construct a
// syntax error. This is the same behavior as MySQL.
join_table:
	table_reference inner_join table_factor %prec JOIN
	{
		$$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3}
	}
|	table_reference inner_join table_factor ON expression
	{
		$$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, On: $5}
	}
|	table_reference outer_join table_reference ON expression
	{
		$$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, On: $5}
	}
|	table_reference natural_join table_factor
	{
		$$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3}
	}

as_opt:
	{
		$$ = struct{}{}
	}
|	AS
	{
		$$ = struct{}{}
	}

as_opt_id:
	{
		$$ = NewTableIdent("")
	}
|	table_alias
	{
		$$ = $1
	}
|	AS table_alias
	{
		$$ = $2
	}

table_alias:
	table_id
|	STRING
	{
		$$ = NewTableIdent(string($1))
	}

inner_join:
	JOIN
	{
		$$ = JoinStr
	}
|	INNER JOIN
	{
		$$ = JoinStr
	}
|	CROSS JOIN
	{
		$$ = JoinStr
	}
|	STRAIGHT_JOIN
	{
		$$ = StraightJoinStr
	}

outer_join:
	LEFT JOIN
	{
		$$ = LeftJoinStr
	}
|	LEFT OUTER JOIN
	{
		$$ = LeftJoinStr
	}
|	RIGHT JOIN
	{
		$$ = RightJoinStr
	}
|	RIGHT OUTER JOIN
	{
		$$ = RightJoinStr
	}

natural_join:
	NATURAL JOIN
	{
		$$ = NaturalJoinStr
	}
|	NATURAL outer_join
	{
		if $2 == LeftJoinStr {
			$$ = NaturalLeftJoinStr
		} else {
			$$ = NaturalRightJoinStr
		}
	}

into_table_name:
	INTO table_name
	{
		$$ = $2
	}
|	table_name
	{
		$$ = $1
	}

table_name:
	table_id
	{
		$$ = TableName{Name: $1}
	}
|	table_id '.' reserved_table_id
	{
		$$ = TableName{Qualifier: $1, Name: $3}
	}

index_hint_list:
	{
		$$ = nil
	}
|	USE INDEX openb index_list closeb
	{
		$$ = &IndexHints{Type: UseStr, Indexes: $4}
	}
|	IGNORE INDEX openb index_list closeb
	{
		$$ = &IndexHints{Type: IgnoreStr, Indexes: $4}
	}
|	FORCE INDEX openb index_list closeb
	{
		$$ = &IndexHints{Type: ForceStr, Indexes: $4}
	}

index_list:
	sql_id
	{
		$$ = []ColIdent{$1}
	}
|	index_list ',' sql_id
	{
		$$ = append($1, $3)
	}

where_expression_opt:
	{
		$$ = nil
	}
|	WHERE expression
	{
		$$ = $2
	}

expression:
	condition
	{
		$$ = $1
	}
|	expression AND expression
	{
		$$ = &AndExpr{Left: $1, Right: $3}
	}
|	expression OR expression
	{
		$$ = &OrExpr{Left: $1, Right: $3}
	}
|	NOT expression
	{
		$$ = &NotExpr{Expr: $2}
	}
|	expression IS is_suffix
	{
		$$ = &IsExpr{Operator: $3, Expr: $1}
	}
|	value_expression
	{
		$$ = $1
	}
|	DEFAULT default_opt
	{
		$$ = &Default{ColName: $2}
	}

default_opt:
	/* empty */
	{
		$$ = ""
	}
|	openb ID closeb
	{
		$$ = string($2)
	}

boolean_value:
	TRUE
	{
		$$ = BoolVal(true)
	}
|	FALSE
	{
		$$ = BoolVal(false)
	}

condition:
	value_expression compare value_expression
	{
		$$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
	}
|	value_expression IN col_tuple
	{
		$$ = &ComparisonExpr{Left: $1, Operator: InStr, Right: $3}
	}
|	value_expression NOT IN col_tuple
	{
		$$ = &ComparisonExpr{Left: $1, Operator: NotInStr, Right: $4}
	}
|	value_expression LIKE value_expression like_escape_opt
	{
		$$ = &ComparisonExpr{Left: $1, Operator: LikeStr, Right: $3, Escape: $4}
	}
|	value_expression NOT LIKE value_expression like_escape_opt
	{
		$$ = &ComparisonExpr{Left: $1, Operator: NotLikeStr, Right: $4, Escape: $5}
	}
|	value_expression REGEXP value_expression
	{
		$$ = &ComparisonExpr{Left: $1, Operator: RegexpStr, Right: $3}
	}
|	value_expression NOT REGEXP value_expression
	{
		$$ = &ComparisonExpr{Left: $1, Operator: NotRegexpStr, Right: $4}
	}
|	value_expression BETWEEN value_expression AND value_expression
	{
		$$ = &RangeCond{Left: $1, Operator: BetweenStr, From: $3, To: $5}
	}
|	value_expression NOT BETWEEN value_expression AND value_expression
	{
		$$ = &RangeCond{Left: $1, Operator: NotBetweenStr, From: $4, To: $6}
	}
|	EXISTS subquery
	{
		$$ = &ExistsExpr{Subquery: $2}
	}

is_suffix:
	NULL
	{
		$$ = IsNullStr
	}
|	NOT NULL
	{
		$$ = IsNotNullStr
	}
|	TRUE
	{
		$$ = IsTrueStr
	}
|	NOT TRUE
	{
		$$ = IsNotTrueStr
	}
|	FALSE
	{
		$$ = IsFalseStr
	}
|	NOT FALSE
	{
		$$ = IsNotFalseStr
	}

compare:
	'='
	{
		$$ = EqualStr
	}
|	'<'
	{
		$$ = LessThanStr
	}
|	'>'
	{
		$$ = GreaterThanStr
	}
|	LE
	{
		$$ = LessEqualStr
	}
|	GE
	{
		$$ = GreaterEqualStr
	}
|	NE
	{
		$$ = NotEqualStr
	}
|	NULL_SAFE_EQUAL
	{
		$$ = NullSafeEqualStr
	}

like_escape_opt:
	{
		$$ = nil
	}
|	ESCAPE value_expression
	{
		$$ = $2
	}

col_tuple:
	row_tuple
	{
		$$ = $1
	}
|	subquery
	{
		$$ = $1
	}
|	LIST_ARG
	{
		$$ = ListArg($1)
	}

subquery:
	openb select_statement closeb
	{
		$$ = &Subquery{$2}
	}

expression_list:
	expression
	{
		$$ = Exprs{$1}
	}
|	expression_list ',' expression
	{
		$$ = append($1, $3)
	}

value_expression:
	value
	{
		$$ = $1
	}
|	boolean_value
	{
		$$ = $1
	}
|	column_name
	{
		$$ = $1
	}
|	tuple_expression
	{
		$$ = $1
	}
|	subquery
	{
		$$ = $1
	}
|	value_expression '&' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: BitAndStr, Right: $3}
	}
|	value_expression '|' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: BitOrStr, Right: $3}
	}
|	value_expression '^' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: BitXorStr, Right: $3}
	}
|	value_expression '+' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: PlusStr, Right: $3}
	}
|	value_expression '-' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: MinusStr, Right: $3}
	}
|	value_expression '*' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: MultStr, Right: $3}
	}
|	value_expression '/' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: DivStr, Right: $3}
	}
|	value_expression DIV value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: IntDivStr, Right: $3}
	}
|	value_expression '%' value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: ModStr, Right: $3}
	}
|	value_expression MOD value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: ModStr, Right: $3}
	}
|	value_expression SHIFT_LEFT value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: ShiftLeftStr, Right: $3}
	}
|	value_expression SHIFT_RIGHT value_expression
	{
		$$ = &BinaryExpr{Left: $1, Operator: ShiftRightStr, Right: $3}
	}
|	column_name JSON_EXTRACT_OP value
	{
		$$ = &BinaryExpr{Left: $1, Operator: JSONExtractOp, Right: $3}
	}
|	column_name JSON_UNQUOTE_EXTRACT_OP value
	{
		$$ = &BinaryExpr{Left: $1, Operator: JSONUnquoteExtractOp, Right: $3}
	}
|	value_expression COLLATE charset
	{
		$$ = &CollateExpr{Expr: $1, Charset: $3}
	}
|	BINARY value_expression %prec UNARY
	{
		$$ = &UnaryExpr{Operator: BinaryStr, Expr: $2}
	}
|	'+' value_expression %prec UNARY
	{
		if num, ok := $2.(*SQLVal); ok && num.Type == IntVal {
			$$ = num
		} else {
			$$ = &UnaryExpr{Operator: UPlusStr, Expr: $2}
		}
	}
|	'-' value_expression %prec UNARY
	{
		if num, ok := $2.(*SQLVal); ok && num.Type == IntVal {
			// Handle double negative
			if num.Val[0] == '-' {
				num.Val = num.Val[1:]
				$$ = num
			} else {
				$$ = NewIntVal(append([]byte("-"), num.Val...))
			}
		} else {
			$$ = &UnaryExpr{Operator: UMinusStr, Expr: $2}
		}
	}
|	'~' value_expression
	{
		$$ = &UnaryExpr{Operator: TildaStr, Expr: $2}
	}
|	'!' value_expression %prec UNARY
	{
		$$ = &UnaryExpr{Operator: BangStr, Expr: $2}
	}
|	INTERVAL value_expression sql_id
	{
		// This rule prevents the usage of INTERVAL
		// as a function. If support is needed for that,
		// we'll need to revisit this. The solution
		// will be non-trivial because of grammar conflicts.
		$$ = &IntervalExpr{Expr: $2, Unit: $3.String()}
	}
|	function_call_generic
|	function_call_keyword
|	function_call_nonkeyword
|	function_call_conflict

/*
  Regular function calls without special token or syntax, guaranteed to not
  introduce side effects due to being a simple identifier
*/
function_call_generic:
	sql_id openb select_expression_list_opt closeb
	{
		$$ = &FuncExpr{Name: $1, Exprs: $3}
	}
|	sql_id openb DISTINCT select_expression_list closeb
	{
		$$ = &FuncExpr{Name: $1, Distinct: true, Exprs: $4}
	}
|	table_id '.' reserved_sql_id openb select_expression_list_opt closeb
	{
		$$ = &FuncExpr{Qualifier: $1, Name: $3, Exprs: $5}
	}

/*
  Function calls using reserved keywords, with dedicated grammar rules
  as a result
*/
function_call_keyword:
	LEFT openb select_expression_list closeb
	{
		$$ = &FuncExpr{Name: NewColIdent("left"), Exprs: $3}
	}
|	RIGHT openb select_expression_list closeb
	{
		$$ = &FuncExpr{Name: NewColIdent("right"), Exprs: $3}
	}
|	CONVERT openb expression ',' convert_type closeb
	{
		$$ = &ConvertExpr{Expr: $3, Type: $5}
	}
|	CAST openb expression AS convert_type closeb
	{
		$$ = &ConvertExpr{Expr: $3, Type: $5}
	}
|	CONVERT openb expression USING charset closeb
	{
		$$ = &ConvertUsingExpr{Expr: $3, Type: $5}
	}
|	MATCH openb select_expression_list closeb AGAINST openb value_expression match_option closeb
	{
		$$ = &MatchExpr{Columns: $3, Expr: $7, Option: $8}
	}
|	GROUP_CONCAT openb distinct_opt select_expression_list order_by_opt separator_opt closeb
	{
		$$ = &GroupConcatExpr{Distinct: $3, Exprs: $4, OrderBy: $5, Separator: $6}
	}
|	CASE expression_opt when_expression_list else_expression_opt END
	{
		$$ = &CaseExpr{Expr: $2, Whens: $3, Else: $4}
	}
|	VALUES openb sql_id closeb
	{
		$$ = &ValuesFuncExpr{Name: $3}
	}

/*
  Function calls using non reserved keywords but with special syntax forms.
  Dedicated grammar rules are needed because of the special syntax
*/
function_call_nonkeyword:
	CURRENT_TIMESTAMP func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("current_timestamp")}
	}
|	UTC_TIMESTAMP func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("utc_timestamp")}
	}
|	UTC_TIME func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("utc_time")}
	}
|	UTC_DATE func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("utc_date")}
	}
// now
|	LOCALTIME func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("localtime")}
	}
// now
|	LOCALTIMESTAMP func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("localtimestamp")}
	}
// curdate
|	CURRENT_DATE func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("current_date")}
	}
// curtime
|	CURRENT_TIME func_datetime_precision_opt
	{
		$$ = &FuncExpr{Name: NewColIdent("current_time")}
	}

func_datetime_precision_opt:

/* empty */
|	openb closeb

/*
  Function calls using non reserved keywords with *normal* syntax forms. Because
  the names are non-reserved, they need a dedicated rule so as not to conflict
*/
function_call_conflict:
	IF openb select_expression_list closeb
	{
		$$ = &FuncExpr{Name: NewColIdent("if"), Exprs: $3}
	}
|	DATABASE openb select_expression_list_opt closeb
	{
		$$ = &FuncExpr{Name: NewColIdent("database"), Exprs: $3}
	}
|	MOD openb select_expression_list closeb
	{
		$$ = &FuncExpr{Name: NewColIdent("mod"), Exprs: $3}
	}
|	REPLACE openb select_expression_list closeb
	{
		$$ = &FuncExpr{Name: NewColIdent("replace"), Exprs: $3}
	}

match_option:
	/*empty*/
	{
		$$ = ""
	}
|	IN BOOLEAN MODE
	{
		$$ = BooleanModeStr
	}
|	IN NATURAL LANGUAGE MODE
	{
		$$ = NaturalLanguageModeStr
	}
|	IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
	{
		$$ = NaturalLanguageModeWithQueryExpansionStr
	}
|	WITH QUERY EXPANSION
	{
		$$ = QueryExpansionStr
	}

charset:
	ID
	{
		$$ = string($1)
	}
|	STRING
	{
		$$ = string($1)
	}

convert_type:
	BINARY length_opt
	{
		$$ = &ConvertType{Type: string($1), Length: $2}
	}
|	CHAR length_opt charset_opt
	{
		$$ = &ConvertType{Type: string($1), Length: $2, Charset: $3, Operator: CharacterSetStr}
	}
|	CHAR length_opt ID
	{
		$$ = &ConvertType{Type: string($1), Length: $2, Charset: string($3)}
	}
|	DATE
	{
		$$ = &ConvertType{Type: string($1)}
	}
|	DATETIME length_opt
	{
		$$ = &ConvertType{Type: string($1), Length: $2}
	}
|	DECIMAL decimal_length_opt
	{
		$$ = &ConvertType{Type: string($1)}
		$$.Length = $2.Length
		$$.Scale = $2.Scale
	}
|	JSON
	{
		$$ = &ConvertType{Type: string($1)}
	}
|	NCHAR length_opt
	{
		$$ = &ConvertType{Type: string($1), Length: $2}
	}
|	SIGNED
	{
		$$ = &ConvertType{Type: string($1)}
	}
|	SIGNED INTEGER
	{
		$$ = &ConvertType{Type: string($1)}
	}
|	TIME length_opt
	{
		$$ = &ConvertType{Type: string($1), Length: $2}
	}
|	UNSIGNED
	{
		$$ = &ConvertType{Type: string($1)}
	}
|	UNSIGNED INTEGER
	{
		$$ = &ConvertType{Type: string($1)}
	}

expression_opt:
	{
		$$ = nil
	}
|	expression
	{
		$$ = $1
	}

separator_opt:
	{
		$$ = string("")
	}
|	SEPARATOR STRING
	{
		$$ = " separator '" + string($2) + "'"
	}

when_expression_list:
	when_expression
	{
		$$ = []*When{$1}
	}
|	when_expression_list when_expression
	{
		$$ = append($1, $2)
	}

when_expression:
	WHEN expression THEN expression
	{
		$$ = &When{Cond: $2, Val: $4}
	}

else_expression_opt:
	{
		$$ = nil
	}
|	ELSE expression
	{
		$$ = $2
	}

column_name:
	sql_id
	{
		$$ = &ColName{Name: $1}
	}
|	table_id '.' reserved_sql_id
	{
		$$ = &ColName{Qualifier: TableName{Name: $1}, Name: $3}
	}
|	table_id '.' reserved_table_id '.' reserved_sql_id
	{
		$$ = &ColName{Qualifier: TableName{Qualifier: $1, Name: $3}, Name: $5}
	}

value:
	STRING
	{
		$$ = NewStrVal($1)
	}
|	HEX
	{
		$$ = NewHexVal($1)
	}
|	INTEGRAL
	{
		$$ = NewIntVal($1)
	}
|	FLOAT
	{
		$$ = NewFloatVal($1)
	}
|	HEXNUM
	{
		$$ = NewHexNum($1)
	}
|	VALUE_ARG
	{
		$$ = NewValArg($1)
	}
|	NULL
	{
		$$ = &NullVal{}
	}

num_val:
	sql_id
	{
		// TODO(sougou): Deprecate this construct.
		if $1.Lowered() != "value" {
			yylex.Error("expecting value after next")
			return 1
		}
		$$ = NewIntVal([]byte("1"))
	}
|	INTEGRAL VALUES
	{
		$$ = NewIntVal($1)
	}
|	VALUE_ARG VALUES
	{
		$$ = NewValArg($1)
	}

group_by_opt:
	{
		$$ = nil
	}
|	GROUP BY expression_list
	{
		$$ = $3
	}

having_opt:
	{
		$$ = nil
	}
|	HAVING expression
	{
		$$ = $2
	}

order_by_opt:
	{
		$$ = nil
	}
|	ORDER BY order_list
	{
		$$ = $3
	}

order_list:
	order
	{
		$$ = OrderBy{$1}
	}
|	order_list ',' order
	{
		$$ = append($1, $3)
	}

order:
	expression asc_desc_opt
	{
		$$ = &Order{Expr: $1, Direction: $2}
	}

asc_desc_opt:
	{
		$$ = AscScr
	}
|	ASC
	{
		$$ = AscScr
	}
|	DESC
	{
		$$ = DescScr
	}

limit_opt:
	{
		$$ = nil
	}
|	LIMIT expression
	{
		$$ = &Limit{Rowcount: $2}
	}
|	LIMIT expression ',' expression
	{
		$$ = &Limit{Offset: $2, Rowcount: $4}
	}
|	LIMIT expression OFFSET expression
	{
		$$ = &Limit{Offset: $4, Rowcount: $2}
	}

lock_opt:
	{
		$$ = ""
	}
|	FOR UPDATE
	{
		$$ = ForUpdateStr
	}
|	LOCK IN SHARE MODE
	{
		$$ = ShareModeStr
	}

// insert_data expands all combinations into a single rule.
// This avoids a shift/reduce conflict while encountering the
// following two possible constructs:
// insert into t1(a, b) (select * from t2)
// insert into t1(select * from t2)
// Because the rules are together, the parser can keep shifting
// the tokens until it disambiguates a as sql_id and select as keyword.
insert_data:
	VALUES tuple_list
	{
		$$ = &Insert{Rows: $2}
	}
|	select_statement
	{
		$$ = &Insert{Rows: $1}
	}
|	openb select_statement closeb
	{
		// Drop the redundant parenthesis.
		$$ = &Insert{Rows: $2}
	}
|	openb ins_column_list closeb VALUES tuple_list
	{
		$$ = &Insert{Columns: $2, Rows: $5}
	}
|	openb ins_column_list closeb select_statement
	{
		$$ = &Insert{Columns: $2, Rows: $4}
	}
|	openb ins_column_list closeb openb select_statement closeb
	{
		// Drop the redundant parenthesis.
		$$ = &Insert{Columns: $2, Rows: $5}
	}

ins_column_list:
	sql_id
	{
		$$ = Columns{$1}
	}
|	sql_id '.' sql_id
	{
		$$ = Columns{$3}
	}
|	ins_column_list ',' sql_id
	{
		$$ = append($$, $3)
	}
|	ins_column_list ',' sql_id '.' sql_id
	{
		$$ = append($$, $5)
	}

on_dup_opt:
	{
		$$ = nil
	}
|	ON DUPLICATE KEY UPDATE update_list
	{
		$$ = $5
	}

tuple_list:
	tuple_or_empty
	{
		$$ = Values{$1}
	}
|	tuple_list ',' tuple_or_empty
	{
		$$ = append($1, $3)
	}

tuple_or_empty:
	row_tuple
	{
		$$ = $1
	}
|	openb closeb
	{
		$$ = ValTuple{}
	}

row_tuple:
	openb expression_list closeb
	{
		$$ = ValTuple($2)
	}

tuple_expression:
	row_tuple
	{
		if len($1) == 1 {
			$$ = &ParenExpr{$1[0]}
		} else {
			$$ = $1
		}
	}

update_list:
	update_expression
	{
		$$ = UpdateExprs{$1}
	}
|	update_list ',' update_expression
	{
		$$ = append($1, $3)
	}

update_expression:
	column_name '=' expression
	{
		$$ = &UpdateExpr{Name: $1, Expr: $3}
	}

set_list:
	set_expression
	{
		$$ = SetExprs{$1}
	}
|	set_list ',' set_expression
	{
		$$ = append($1, $3)
	}

set_expression:
	reserved_sql_id '=' expression
	{
		$$ = &SetExpr{Name: $1, Expr: $3}
	}
|	charset_or_character_set charset_value collate_opt
	{
		$$ = &SetExpr{Name: NewColIdent(string($1)), Expr: $2}
	}

charset_or_character_set:
	CHARSET
|	CHARACTER SET
	{
		$$ = []byte("charset")
	}
|	NAMES

charset_value:
	sql_id
	{
		$$ = NewStrVal([]byte($1.String()))
	}
|	STRING
	{
		$$ = NewStrVal($1)
	}
|	DEFAULT
	{
		$$ = &Default{}
	}

for_from:
	FOR
|	FROM

exists_opt:
	{
		$$ = 0
	}
|	IF EXISTS
	{
		$$ = 1
	}

not_exists_opt:
	{
		$$ = 0
	}
|	IF NOT EXISTS
	{
		$$ = 1
	}

ignore_opt:
	{
		$$ = ""
	}
|	IGNORE
	{
		$$ = IgnoreStr
	}

non_rename_operation:
	ALTER
	{
		$$ = struct{}{}
	}
|	AUTO_INCREMENT
	{
		$$ = struct{}{}
	}
|	CHARACTER
	{
		$$ = struct{}{}
	}
|	COMMENT_KEYWORD
	{
		$$ = struct{}{}
	}
|	DEFAULT
	{
		$$ = struct{}{}
	}
|	DROP
	{
		$$ = struct{}{}
	}
|	ORDER
	{
		$$ = struct{}{}
	}
|	CONVERT
	{
		$$ = struct{}{}
	}
|	UNUSED
	{
		$$ = struct{}{}
	}
|	ID
	{
		$$ = struct{}{}
	}

to_opt:
	{
		$$ = struct{}{}
	}
|	TO
	{
		$$ = struct{}{}
	}
|	AS
	{
		$$ = struct{}{}
	}

index_opt:
	INDEX
	{
		$$ = struct{}{}
	}
|	KEY
	{
		$$ = struct{}{}
	}

constraint_opt:
	{
		$$ = struct{}{}
	}
|	UNIQUE
	{
		$$ = struct{}{}
	}
|	sql_id
	{
		$$ = struct{}{}
	}

sql_id:
	ID
	{
		$$ = NewColIdent(string($1))
	}
|	non_reserved_keyword
	{
		$$ = NewColIdent(string($1))
	}

reserved_sql_id:
	sql_id
|	reserved_keyword
	{
		$$ = NewColIdent(string($1))
	}

table_id:
	ID
	{
		$$ = NewTableIdent(string($1))
	}
|	non_reserved_keyword
	{
		$$ = NewTableIdent(string($1))
	}

reserved_table_id:
	table_id
|	reserved_keyword
	{
		$$ = NewTableIdent(string($1))
	}

/*
  These are not all necessarily reserved in MySQL, but some are.

  These are more importantly reserved because they may conflict with our grammar.
  If you want to move one that is not reserved in MySQL (i.e. ESCAPE) to the
  non_reserved_keywords, you'll need to deal with any conflicts.

  Sorted alphabetically
*/
reserved_keyword:
	AND
|	AS
|	ASC
|	AUTO_INCREMENT
|	BETWEEN
|	BIGINT
|	BINARY
|	BLOB
|	BY
|	CASE
|	CHAR
|	CHARACTER
|	CHARSET
|	COLLATE
|	COLUMNS
|	CONVERT
|	CREATE
|	CROSS
|	CURRENT_DATE
|	CURRENT_TIME
|	CURRENT_TIMESTAMP
|	DATABASE
|	DATABASES
|	DECIMAL
|	DEFAULT
|	DELETE
|	DESC
|	DESCRIBE
|	DISTINCT
|	DIV
|	DROP
|	ELSE
|	END
|	ENGINES
|	ESCAPE
|	EXISTS
|	EXPLAIN
|	FALSE
|	FOR
|	FORCE
|	FROM
|	FULL
|	GROUP
|	HAVING
|	IF
|	IGNORE
|	IN
|	INDEX
|	INNER
|	INSERT
|	INT
|	INTEGER
|	INTERVAL
|	INTO
|	IS
|	JOIN
|	KEY
|	LEFT
|	LIKE
|	LIMIT
|	LOCALTIME
|	LOCALTIMESTAMP
|	LOCK
|	LONGBLOB
|	LONGTEXT
|	MATCH
|	MEDIUMBLOB
|	MEDIUMINT
|	MEDIUMTEXT
|	MOD
|	NATURAL
|	NEXT
// next should be doable as non-reserved, but is not due to the special `select next num_val` query that vitess supports
|	NOT
|	NULL
|	NUMERIC
|	OFF
|	ON
|	OPTIMIZE
|	OR
|	ORDER
|	OUTER
|	QUERYZ
|	PRIMARY
|	PROCESSLIST
|	REAL
|	REGEXP
|	RENAME
|	REPLACE
|	RIGHT
|	SELECT
|	SEPARATOR
|	SET
|	SHOW
|	SMALLINT
|	STRAIGHT_JOIN
|	TABLE
|	TABLES
|	TINYBLOB
|	TINYINT
|	TINYTEXT
|	THEN
|	TO
|	TRUE
|	TXNZ
|	UNION
|	UNIQUE
|	UNSIGNED
|	UPDATE
|	USE
|	USING
|	UTC_DATE
|	UTC_TIME
|	UTC_TIMESTAMP
|	VALUES
|	VARBINARY
|	VARCHAR
|	VERSIONS
|	WITH
|	WHEN
|	WHERE
|	ZEROFILL

/*
  These are non-reserved Vitess, because they don't cause conflicts in the grammar.
  Some of them may be reserved in MySQL. The good news is we backtick quote them
  when we rewrite the query, so no issue should arise.

  Sorted alphabetically
*/
non_reserved_keyword:
	AGAINST
|	BIT
|	BOOL
|	COMMENT_KEYWORD
|	DATE
|	DATETIME
|	DOUBLE
|	DUPLICATE
|	ENUM
|	ENGINE
|	EXPANSION
|	FIELDS
|	FLOAT_TYPE
|	FULLTEXT
|	GLOBAL
|	JSON
|	LANGUAGE
|	LAST_INSERT_ID
|	MODE
|	NCHAR
|	OFFSET
|	QUERY
|	REPAIR
|	SHARE
|	SIGNED
|	SINGLE
|	STATUS
|	TEXT
|	TIME
|	TIMESTAMP
|	TRUNCATE
|	UNUSED
|	VIEW
|	YEAR
|	RADON
|	ATTACH
|	DETACH
|	ATTACHLIST
|	RESHARD
|   CLEANUP

openb:
	'('
	{
		if incNesting(yylex) {
			yylex.Error("max nesting level reached")
			return 1
		}
	}

closeb:
	')'
	{
		decNesting(yylex)
	}

force_eof:
	{
		forceEOF(yylex)
	}

ddl_force_eof:
	{
		forceEOF(yylex)
	}
|	openb
	{
		forceEOF(yylex)
	}
|	reserved_sql_id
	{
		forceEOF(yylex)
	}
%%
