/*
Copyright 2019 The Vitess Authors.

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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...SQLNode) error {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		var err error
		var kontinue bool
		pre := func(cursor *Cursor) bool {
			// If we already have found an error, don't visit these nodes, just exit early
			if err != nil {
				return false
			}
			kontinue, err = visit(cursor.Node())
			if err != nil {
				return true // we have to return true here so that post gets called
			}
			return kontinue
		}
		post := func(cursor *Cursor) bool {
			return err == nil // now we can abort the traversal if an error was found
		}

		Rewrite(node, pre, post)
		if err != nil {
			return err
		}
	}
	return nil
}

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node SQLNode) (kontinue bool, err error)

// Append appends the SQLNode to the buffer.
func Append(buf *strings.Builder, node SQLNode) {
	tbuf := &TrackedBuffer{
		Builder: buf,
	}
	node.Format(tbuf)
}

// AddWhere adds the boolean expression to the
// WHERE clause as an AND condition. If the expression
// is an OR clause, it parenthesizes it. Currently,
// the OR operator is the only one that's lower precedence
// than AND.
func (node *Select) AddWhere(expr Expr) {
	if _, ok := expr.(*OrExpr); ok {
		expr = &ParenExpr{Expr: expr}
	}
	if node.Where == nil {
		node.Where = &Where{
			Type: WhereStr,
			Expr: expr,
		}
		return
	}
	node.Where.Expr = &AndExpr{
		Left:  node.Where.Expr,
		Right: expr,
	}
}

// AddHaving adds the boolean expression to the
// HAVING clause as an AND condition. If the expression
// is an OR clause, it parenthesizes it. Currently,
// the OR operator is the only one that's lower precedence
// than AND.
func (node *Select) AddHaving(expr Expr) {
	if _, ok := expr.(*OrExpr); ok {
		expr = &ParenExpr{Expr: expr}
	}
	if node.Having == nil {
		node.Having = &Where{
			Type: HavingStr,
			Expr: expr,
		}
		return
	}
	node.Having.Expr = &AndExpr{
		Left:  node.Having.Expr,
		Right: expr,
	}
}

// DatabaseOption represents database option.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type DatabaseOption struct {
	// type:charset, collate or encryption
	OptType string
	Value   *SQLVal
}

// TableOptionType is the type for table_options
type TableOptionType int

const (
	TableOptionNone TableOptionType = iota
	TableOptionComment
	TableOptionEngine
	TableOptionCharset
	TableOptionAutoInc
	TableOptionAvgRowLength
	TableOptionChecksum
	TableOptionCollate
	TableOptionCompression
	TableOptionConnection
	TableOptionDataDirectory
	TableOptionIndexDirectory
	TableOptionDelayKeyWrite
	TableOptionEncryption
	TableOptionInsertMethod
	TableOptionKeyBlockSize
	TableOptionMaxRows
	TableOptionMinRows
	TableOptionPackKeys
	TableOptionPassword
	TableOptionRowFormat
	TableOptionStatsAutoRecalc
	TableOptionStatsPersistent
	TableOptionStatsSamplePages
	TableOptionTableSpace
)

// Although each option can be appeared many times in MySQL, we make a constraint
// that each option should only be appeared just one time in RadonDB.
func (tblOptList *TableOptionListOpt) CheckIfTableOptDuplicate() string {
	var optOnce = map[TableOptionType]bool{
		TableOptionComment:          false,
		TableOptionEngine:           false,
		TableOptionCharset:          false,
		TableOptionAutoInc:          false,
		TableOptionAvgRowLength:     false,
		TableOptionChecksum:         false,
		TableOptionCollate:          false,
		TableOptionCompression:      false,
		TableOptionConnection:       false,
		TableOptionDataDirectory:    false,
		TableOptionIndexDirectory:   false,
		TableOptionDelayKeyWrite:    false,
		TableOptionEncryption:       false,
		TableOptionInsertMethod:     false,
		TableOptionKeyBlockSize:     false,
		TableOptionMaxRows:          false,
		TableOptionMinRows:          false,
		TableOptionPackKeys:         false,
		TableOptionPassword:         false,
		TableOptionRowFormat:        false,
		TableOptionStatsAutoRecalc:  false,
		TableOptionStatsPersistent:  false,
		TableOptionStatsSamplePages: false,
	}
	for _, opt := range tblOptList.TblOptList {
		switch opt.Type {
		case TableOptionComment:
			if optOnce[TableOptionComment] {
				return "Duplicate table option for keyword 'comment', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionComment] = true
		case TableOptionEngine:
			if optOnce[TableOptionEngine] {
				return "Duplicate table option for keyword 'engine', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionEngine] = true
		case TableOptionCharset:
			if optOnce[TableOptionCharset] {
				return "Duplicate table option for keyword 'charset', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionCharset] = true
		case TableOptionAutoInc:
			if optOnce[TableOptionAutoInc] {
				return "Duplicate table option for keyword 'auto_increment', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionAutoInc] = true
		case TableOptionAvgRowLength:
			if optOnce[TableOptionAvgRowLength] {
				return "Duplicate table option for keyword 'avg_row_length', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionAvgRowLength] = true
		case TableOptionChecksum:
			if optOnce[TableOptionChecksum] {
				return "Duplicate table option for keyword 'checksum', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionChecksum] = true
		case TableOptionCollate:
			if optOnce[TableOptionCollate] {
				return "Duplicate table option for table option keyword 'collate', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionCollate] = true
		case TableOptionCompression:
			if optOnce[TableOptionCompression] {
				return "Duplicate table option for keyword 'compression', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionCompression] = true
		case TableOptionConnection:
			if optOnce[TableOptionConnection] {
				return "Duplicate table option for keyword 'connection', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionConnection] = true
		case TableOptionDataDirectory:
			if optOnce[TableOptionDataDirectory] {
				return "Duplicate table option for keyword 'data directory', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionDataDirectory] = true
		case TableOptionIndexDirectory:
			if optOnce[TableOptionIndexDirectory] {
				return "Duplicate table option for keyword 'index directory', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionIndexDirectory] = true
		case TableOptionDelayKeyWrite:
			if optOnce[TableOptionDelayKeyWrite] {
				return "Duplicate table option for keyword 'delay_key_write', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionDelayKeyWrite] = true
		case TableOptionEncryption:
			if optOnce[TableOptionEncryption] {
				return "Duplicate table option for keyword 'encryption', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionEncryption] = true
		case TableOptionInsertMethod:
			if optOnce[TableOptionInsertMethod] {
				return "Duplicate table option for keyword 'insert_method', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionInsertMethod] = true
		case TableOptionKeyBlockSize:
			if optOnce[TableOptionKeyBlockSize] {
				return "Duplicate table option for keyword 'key_block_size', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionKeyBlockSize] = true
		case TableOptionMaxRows:
			if optOnce[TableOptionMaxRows] {
				return "Duplicate table option for keyword 'max_rows', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionMaxRows] = true
		case TableOptionMinRows:
			if optOnce[TableOptionMinRows] {
				return "Duplicate table option for keyword 'min_rows', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionMinRows] = true
		case TableOptionPackKeys:
			if optOnce[TableOptionPackKeys] {
				return "Duplicate table option for keyword 'pack_keys', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionPackKeys] = true
		case TableOptionPassword:
			if optOnce[TableOptionPassword] {
				return "Duplicate table option for keyword 'password', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionPassword] = true
		case TableOptionRowFormat:
			if optOnce[TableOptionRowFormat] {
				return "Duplicate table option for keyword 'row_format', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionRowFormat] = true
		case TableOptionStatsAutoRecalc:
			if optOnce[TableOptionStatsAutoRecalc] {
				return "Duplicate table option for keyword 'stats_auto_recalc', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionStatsAutoRecalc] = true
		case TableOptionStatsPersistent:
			if optOnce[TableOptionStatsPersistent] {
				return "Duplicate table option for keyword 'stats_persistent', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionStatsPersistent] = true
		case TableOptionStatsSamplePages:
			if optOnce[TableOptionStatsSamplePages] {
				return "Duplicate table option for keyword 'stats_sample_pages', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionStatsSamplePages] = true
		case TableOptionTableSpace:
			if optOnce[TableOptionTableSpace] {
				return "Duplicate table option for keyword 'tablespace', the option should only be appeared just one time in RadonDB."
			}
			optOnce[TableOptionTableSpace] = true
		}
	}
	return ""
}

func (tblOptList *TableOptionListOpt) GetTableOptValByType(optType TableOptionType) *SQLVal {
	for _, opt := range tblOptList.TblOptList {
		if opt.Type == optType {
			return opt.Val
		}
	}
	return nil
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	Backend string
	Row     ValTuple
}

// PartitionDefinitions specifies the partition options.
type PartitionDefinitions []*PartitionDefinition

type (
	// PartitionOption interface。
	PartitionOption interface {
		PartitionType() string
	}

	// PartOptGlobal global table.
	PartOptGlobal struct{}

	// PartOptSingle single table.
	PartOptSingle struct {
		BackendName string
	}

	// PartOptNormal normal table.
	PartOptNormal struct{}

	// PartOptList list table.
	PartOptList struct {
		Name     string
		PartDefs PartitionDefinitions
	}

	// PartOptHash hash table.
	PartOptHash struct {
		Name         string
		PartitionNum *SQLVal
	}
)

// PartitionType return the partition type.
func (*PartOptGlobal) PartitionType() string {
	return GlobalTableType
}

// PartitionType return the partition type.
func (*PartOptSingle) PartitionType() string {
	return SingleTableType
}

// PartitionType return the partition type.
func (*PartOptNormal) PartitionType() string {
	return NormalTableType
}

// PartitionType return the partition type.
func (*PartOptList) PartitionType() string {
	return PartitionTableList
}

// PartitionType return the partition type.
func (*PartOptHash) PartitionType() string {
	return PartitionTableHash
}

// TableOption represents the table options.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type TableOption struct {
	Type TableOptionType
	Val  *SQLVal
}

type TableOptionListOpt struct {
	TblOptList []*TableOption
}

// IndexOption represents the index options.
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html.
type IndexOption struct {
	Type IndexOptionType
	Val  *SQLVal
}

// IndexOptionType is the type for IndexOption.
type IndexOptionType int

const (
	// IndexOptionNone enum.
	IndexOptionNone IndexOptionType = iota
	// IndexOptionComment is 'comment' enum.
	IndexOptionComment
	// IndexOptionUsing is 'using' enum.
	IndexOptionUsing
	// IndexOptionBlockSize is 'key_block_size' enum.
	IndexOptionBlockSize
	// IndexOptionParser is 'with parser' enum.
	IndexOptionParser
	// IndexOptionAlgorithm is 'algorithm' enum.
	IndexOptionAlgorithm
	// IndexOptionLock is 'lock' enum.
	IndexOptionLock
)

// IndexColumn describes a column in an index definition with optional length
type IndexColumn struct {
	Column ColIdent
	Length *SQLVal
}

// NewIndexOptions use to create IndexOptions.
func NewIndexOptions(columns []*IndexColumn, idxOptList []*IndexOption) *IndexOptions {
	idxOpts := &IndexOptions{
		Columns: columns,
	}
	for _, idxOpt := range idxOptList {
		switch idxOpt.Type {
		case IndexOptionComment:
			idxOpts.Comment = String(idxOpt.Val)
		case IndexOptionUsing:
			idxOpts.Using = String(idxOpt.Val)
		case IndexOptionBlockSize:
			idxOpts.BlockSize = idxOpt.Val
		case IndexOptionParser:
			idxOpts.Parser = String(idxOpt.Val)
		case IndexOptionAlgorithm:
			idxOpts.Algorithm = String(idxOpt.Val)
		case IndexOptionLock:
			idxOpts.Lock = String(idxOpt.Val)
		}
	}
	return idxOpts
}

// CheckIndexLock use to check if the string value matches a supported value.
// Supported values: default, exclusive, none, shared.
func CheckIndexLock(lock string) bool {
	switch strings.ToLower(lock) {
	case "default", "exclusive", "none", "shared":
		return true
	}
	return false
}

// CheckIndexAlgorithm use to check if the string value matches a supported value.
// Supported values: inplace, copy, default.
func CheckIndexAlgorithm(algorithm string) bool {
	switch strings.ToLower(algorithm) {
	case "copy", "default", "inplace":
		return true
	}
	return false
}

// AddColumn appends the given column to the list in the spec
func (ts *TableSpec) AddColumn(cd *ColumnDefinition) {
	ts.Columns = append(ts.Columns, cd)
}

// AddIndex appends the given index to the list in the spec
func (ts *TableSpec) AddIndex(id *IndexDefinition) {
	ts.Indexes = append(ts.Indexes, id)
}

type ColumnOptionListOpt struct {
	ColOptList []*ColumnOption
}

type ColumnOption struct {
	typ ColumnOpt
	// Generic field options.
	NotNull       BoolVal
	Autoincrement BoolVal
	Default       *SQLVal
	Comment       *SQLVal
	OnUpdate      string
	Collate       *SQLVal
	ColumnFormat  string
	Storage       string
	// Key specification
	PrimaryKeyOpt ColumnPrimaryKeyOption
	UniqueKeyOpt  ColumnUniqueKeyOption
}

// ColumnPrimaryKeyOption indicates whether or not the given column is defined as an
// index element and contains the type of the option
type ColumnPrimaryKeyOption int

const (
	// ColKeyPrimaryNone enum.
	ColKeyPrimaryNone ColumnPrimaryKeyOption = iota

	// ColKeyPrimary enum.
	ColKeyPrimary
)

// ColumnUniqueKeyOption indicates whether or not the given column is defined as an
// index element and contains the type of the option
type ColumnUniqueKeyOption int

const (
	// ColKeyUniqueNone enum.
	ColKeyUniqueNone ColumnUniqueKeyOption = iota

	// ColKeyUniqueKey enum.
	ColKeyUniqueKey
)

type ColumnOpt int

const (
	ColumnOptionNone ColumnOpt = iota

	// NotNull enum.
	ColumnOptionNotNull

	// Autoincrement enum.
	ColumnOptionAutoincrement

	// Default enum.
	ColumnOptionDefault

	// Comment enum.
	ColumnOptionComment

	// OnUpdate enum
	ColumnOptionOnUpdate

	// PrimarykeyOption enum.
	ColumnOptionKeyPrimaryOpt

	// UniquekeyOption enum.
	ColumnOptionKeyUniqueOpt

	// CollateOption enum
	ColumnOptionCollate

	// ColumnFormatOption enum
	ColumnOptionFormat

	// ColumnStorageOption enum
	ColumnOptionStorage
)

func (col ColumnOption) GetOptType() ColumnOpt {
	return col.typ
}

func (co *ColumnOptionListOpt) GetColumnOption(opt ColumnOpt) *ColumnOption {
	for _, val := range co.ColOptList {
		if val.typ == opt {
			return val
		}
	}

	return &ColumnOption{
		typ:           ColumnOptionNone,
		NotNull:       false,
		Autoincrement: false,
		Default:       nil,
		Comment:       nil,
		OnUpdate:      "",
		Collate:       nil,
		ColumnFormat:  "",
		Storage:       "",
		PrimaryKeyOpt: ColKeyPrimaryNone,
		UniqueKeyOpt:  ColKeyUniqueNone,
	}
}

// FindColumn finds a column in the column list, returning
// the index if it exists or -1 otherwise
func (node Columns) FindColumn(col ColIdent) int {
	for i, colName := range node {
		if colName.Equal(col) {
			return i
		}
	}
	return -1
}

// LengthScaleOption is used for types that have an optional length
// and scale
type LengthScaleOption struct {
	Length *SQLVal
	Scale  *SQLVal
}

// ValType specifies the type for SQLVal.
type ValType int

// These are the possible Valtype values.
// HexNum represents a 0x... value. It cannot
// be treated as a simple value because it can
// be interpreted differently depending on the
// context.
const (
	StrVal = ValType(iota)
	IntVal
	FloatVal
	HexNum
	HexVal
	ValArg
	StrValWithoutQuote
)

// NewStrVal builds a new StrVal.
func NewStrVal(in []byte) *SQLVal {
	return &SQLVal{Type: StrVal, Val: in}
}

// NewStrValWithoutQuote builds a new string that will be output without quote later in Format.
func NewStrValWithoutQuote(in []byte) *SQLVal {
	return &SQLVal{Type: StrValWithoutQuote, Val: in}
}

// NewIntVal builds a new IntVal.
func NewIntVal(in []byte) *SQLVal {
	return &SQLVal{Type: IntVal, Val: in}
}

// NewFloatVal builds a new FloatVal.
func NewFloatVal(in []byte) *SQLVal {
	return &SQLVal{Type: FloatVal, Val: in}
}

// NewHexNum builds a new HexNum.
func NewHexNum(in []byte) *SQLVal {
	return &SQLVal{Type: HexNum, Val: in}
}

// NewHexVal builds a new HexVal.
func NewHexVal(in []byte) *SQLVal {
	return &SQLVal{Type: HexVal, Val: in}
}

// NewValArg builds a new ValArg.
func NewValArg(in []byte) *SQLVal {
	return &SQLVal{Type: ValArg, Val: in}
}

// HexDecode decodes the hexval into bytes.
func (node *SQLVal) HexDecode() ([]byte, error) {
	dst := make([]byte, hex.DecodedLen(len([]byte(node.Val))))
	_, err := hex.Decode(dst, []byte(node.Val))
	if err != nil {
		return nil, err
	}
	return dst, err
}

// IsEmpty returns true if TableName is nil or empty.
func (node TableName) IsEmpty() bool {
	// If Name is empty, Qualifer is also empty.
	return node.Name.IsEmpty()
}

// NewWhere creates a WHERE or HAVING clause out
// of a Expr. If the expression is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// ReplaceExpr finds the from expression from root
// and replaces it with to. If from matches root,
// then to is returned.
func ReplaceExpr(root, from, to Expr) Expr {
	tmp := Rewrite(root, replaceExpr(from, to), nil)
	expr, success := tmp.(Expr)
	if !success {
		// log.Errorf("Failed to rewrite expression. Rewriter returned a non-expression: " + String(tmp))
		// Unreachable.
		return from
	}
	return expr
}

func replaceExpr(from, to Expr) func(cursor *Cursor) bool {
	return func(cursor *Cursor) bool {
		if cursor.Node() == from {
			cursor.Replace(to)
		}
		switch cursor.Node().(type) {
		case *ExistsExpr, *SQLVal, *Subquery, *ValuesFuncExpr, *Default:
			return false
		}
		return true
	}
}

// CloneSelectExpr used to copy a new SelectExpr.
func CloneSelectExpr(node SelectExpr) SelectExpr {
	if node == nil {
		return nil
	}
	return node.clone()
}

// CloneExpr used to copy a new Expr.
func CloneExpr(node Expr) Expr {
	if node == nil {
		return nil
	}
	return node.clone()
}

// Equal returns true if the column names match.
func (node *ColName) Equal(c *ColName) bool {
	// Failsafe: ColName should not be empty.
	if node == nil || c == nil {
		return false
	}
	return node.Name.Equal(c.Name) && node.Qualifier == c.Qualifier
}

// Aggregates is a map of all aggregate functions.
var Aggregates = map[string]bool{
	"avg":          true,
	"bit_and":      true,
	"bit_or":       true,
	"bit_xor":      true,
	"count":        true,
	"group_concat": true,
	"max":          true,
	"min":          true,
	"std":          true,
	"stddev_pop":   true,
	"stddev_samp":  true,
	"stddev":       true,
	"sum":          true,
	"var_pop":      true,
	"var_samp":     true,
	"variance":     true,
}

// IsAggregate returns true if the function is an aggregate.
func (node *FuncExpr) IsAggregate() bool {
	return Aggregates[node.Name.Lowered()]
}

// NewColIdent makes a new ColIdent.
func NewColIdent(str string) ColIdent {
	return ColIdent{
		val: str,
	}
}

// IsEmpty returns true if the name is empty.
func (node ColIdent) IsEmpty() bool {
	return node.val == ""
}

// String returns the unescaped column name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node ColIdent) String() string {
	return node.val
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node ColIdent) CompliantName() string {
	return compliantName(node.val)
}

// Lowered returns a lower-cased column name.
// This function should generally be used only for optimizing
// comparisons.
func (node ColIdent) Lowered() string {
	if node.val == "" {
		return ""
	}
	if node.lowered == "" {
		node.lowered = strings.ToLower(node.val)
	}
	return node.lowered
}

// Equal performs a case-insensitive compare.
func (node ColIdent) Equal(in ColIdent) bool {
	return node.Lowered() == in.Lowered()
}

// EqualString performs a case-insensitive compare with str.
func (node ColIdent) EqualString(str string) bool {
	return node.Lowered() == strings.ToLower(str)
}

// MarshalJSON marshals into JSON.
func (node ColIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.val)
}

// UnmarshalJSON unmarshals from JSON.
func (node *ColIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.val = result
	return nil
}

// NewTableIdent creates a new TableIdent.
func NewTableIdent(str string) TableIdent {
	return TableIdent{v: str}
}

// IsEmpty returns true if TabIdent is empty.
func (node TableIdent) IsEmpty() bool {
	return node.v == ""
}

// String returns the unescaped table name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node TableIdent) String() string {
	return node.v
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node TableIdent) CompliantName() string {
	return compliantName(node.v)
}

// MarshalJSON marshals into JSON.
func (node TableIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.v)
}

// UnmarshalJSON unmarshals from JSON.
func (node *TableIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.v = result
	return nil
}

// Backtick produces a backticked literal given an input string.
func Backtick(in string) string {
	var buf bytes.Buffer
	buf.WriteByte('`')
	for _, c := range in {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
	return buf.String()
}

func formatID(buf *TrackedBuffer, original, lowered string) {
	for i, c := range original {
		if !isLetter(uint16(c)) {
			if i == 0 || !isDigit(uint16(c)) {
				goto mustEscape
			}
		}
	}
	if _, ok := keywords[lowered]; ok {
		goto mustEscape
	}
	buf.Myprintf("%s", original)
	return

mustEscape:
	buf.WriteByte('`')
	for _, c := range original {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
}

func compliantName(in string) string {
	var buf bytes.Buffer
	for i, c := range in {
		if !isLetter(uint16(c)) {
			if i == 0 || !isDigit(uint16(c)) {
				buf.WriteByte('_')
				continue
			}
		}
		buf.WriteRune(c)
	}
	return buf.String()
}

// StrToLower use to convert str to lower string
func StrToLower(str string) string {
	return strings.ToLower(str)
}

// NumVal represents numval tuple.
type NumVal struct {
	raw string
}

// AsUint64 returns uint64 value.
func (exp *NumVal) AsUint64() uint64 {
	v, err := strconv.ParseUint(exp.raw, 10, 64)
	if err != nil {
		return 1<<63 - 1
	}
	return v
}
