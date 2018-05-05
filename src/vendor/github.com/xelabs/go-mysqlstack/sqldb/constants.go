/*
 * This code was derived from https://github.com/youtube/vitess.
 *
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sqldb

/***************************************************/
// https://dev.mysql.com/doc/internals/en/command-phase.html
// include/my_command.h
const (
	COM_SLEEP byte = iota
	COM_QUIT
	COM_INIT_DB
	COM_QUERY
	COM_FIELD_LIST
	COM_CREATE_DB
	COM_DROP_DB
	COM_REFRESH
	COM_SHUTDOWN
	COM_STATISTICS
	COM_PROCESS_INFO
	COM_CONNECT
	COM_PROCESS_KILL
	COM_DEBUG
	COM_PING
	COM_TIME
	COM_DELAYED_INSERT
	COM_CHANGE_USER
	COM_BINLOG_DUMP
	COM_TABLE_DUMP
	COM_CONNECT_OUT
	COM_REGISTER_SLAVE
	COM_STMT_PREPARE
	COM_STMT_EXECUTE
	COM_STMT_SEND_LONG_DATA
	COM_STMT_CLOSE
	COM_STMT_RESET
	COM_SET_OPTION
	COM_STMT_FETCH
	COM_DAEMON
	COM_BINLOG_DUMP_GTID
	COM_RESET_CONNECTION
)

// CommandString used for translate cmd to string.
func CommandString(cmd byte) string {
	switch cmd {
	case COM_SLEEP:
		return "COM_SLEEP"
	case COM_QUIT:
		return "COM_QUIT"
	case COM_INIT_DB:
		return "COM_INIT_DB"
	case COM_QUERY:
		return "COM_QUERY"
	case COM_FIELD_LIST:
		return "COM_FIELD_LIST"
	case COM_CREATE_DB:
		return "COM_CREATE_DB"
	case COM_DROP_DB:
		return "COM_DROP_DB"
	case COM_REFRESH:
		return "COM_REFRESH"
	case COM_SHUTDOWN:
		return "COM_SHUTDOWN"
	case COM_STATISTICS:
		return "COM_STATISTICS"
	case COM_PROCESS_INFO:
		return "COM_PROCESS_INFO"
	case COM_CONNECT:
		return "COM_CONNECT"
	case COM_PROCESS_KILL:
		return "COM_PROCESS_KILL"
	case COM_DEBUG:
		return "COM_DEBUG"
	case COM_PING:
		return "COM_PING"
	case COM_TIME:
		return "COM_TIME"
	case COM_DELAYED_INSERT:
		return "COM_DELAYED_INSERT"
	case COM_CHANGE_USER:
		return "COM_CHANGE_USER"
	case COM_BINLOG_DUMP:
		return "COM_BINLOG_DUMP"
	case COM_TABLE_DUMP:
		return "COM_TABLE_DUMP"
	case COM_CONNECT_OUT:
		return "COM_CONNECT_OUT"
	case COM_REGISTER_SLAVE:
		return "COM_REGISTER_SLAVE"
	case COM_STMT_PREPARE:
		return "COM_STMT_PREPARE"
	case COM_STMT_EXECUTE:
		return "COM_STMT_EXECUTE"
	case COM_STMT_SEND_LONG_DATA:
		return "COM_STMT_SEND_LONG_DATA"
	case COM_STMT_CLOSE:
		return "COM_STMT_CLOSE"
	case COM_STMT_RESET:
		return "COM_STMT_RESET"
	case COM_SET_OPTION:
		return "COM_SET_OPTION"
	case COM_STMT_FETCH:
		return "COM_STMT_FETCH"
	case COM_DAEMON:
		return "COM_DAEMON"
	case COM_BINLOG_DUMP_GTID:
		return "COM_BINLOG_DUMP_GTID"
	case COM_RESET_CONNECTION:
		return "COM_RESET_CONNECTION"
	}
	return "UNKNOWN"
}

// https://dev.mysql.com/doc/internals/en/capability-flags.html
// include/mysql_com.h
const (
	// new more secure password
	CLIENT_LONG_PASSWORD = 1

	// Found instead of affected rows
	CLIENT_FOUND_ROWS = uint32(1 << 1)

	// Get all column flags
	CLIENT_LONG_FLAG = uint32(1 << 2)

	// One can specify db on connect
	CLIENT_CONNECT_WITH_DB = uint32(1 << 3)

	// Don't allow database.table.column
	CLIENT_NO_SCHEMA = uint32(1 << 4)

	// Can use compression protocol
	CLIENT_COMPRESS = uint32(1 << 5)

	// Odbc client
	CLIENT_ODBC = uint32(1 << 6)

	// Can use LOAD DATA LOCAL
	CLIENT_LOCAL_FILES = uint32(1 << 7)

	// Ignore spaces before '('
	CLIENT_IGNORE_SPACE = uint32(1 << 8)

	// New 4.1 protocol
	CLIENT_PROTOCOL_41 = uint32(1 << 9)

	// This is an interactive client
	CLIENT_INTERACTIVE = uint32(1 << 10)

	// Switch to SSL after handshake
	CLIENT_SSL = uint32(1 << 11)

	// IGNORE sigpipes
	CLIENT_IGNORE_SIGPIPE = uint32(1 << 12)

	// Client knows about transactions
	CLIENT_TRANSACTIONS = uint32(1 << 13)

	// Old flag for 4.1 protocol
	CLIENT_RESERVED = uint32(1 << 14)

	// Old flag for 4.1 authentication
	CLIENT_SECURE_CONNECTION = uint32(1 << 15)

	// Enable/disable multi-stmt support
	CLIENT_MULTI_STATEMENTS = uint32(1 << 16)

	// Enable/disable multi-results
	CLIENT_MULTI_RESULTS = uint32(1 << 17)

	// Multi-results in PS-protocol
	CLIENT_PS_MULTI_RESULTS = uint32(1 << 18)

	// Client supports plugin authentication
	CLIENT_PLUGIN_AUTH = uint32(1 << 19)

	// Client supports connection attributes
	CLIENT_CONNECT_ATTRS = uint32(1 << 20)

	//  Enable authentication response packet to be larger than 255 bytes
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = uint32(1 << 21)

	// Don't close the connection for a connection with expired password
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = uint32(1 << 22)

	// Capable of handling server state change information. Its a hint to the
	// server to include the state change information in Ok packet.
	CLIENT_SESSION_TRACK = uint32(1 << 23)

	//Client no longer needs EOF packet
	CLIENT_DEPRECATE_EOF = uint32(1 << 24)
)

const (
	// SSUnknownSQLState is the default SQLState.
	SSUnknownSQLState = "HY000"
)

// Status flags. They are returned by the server in a few cases.
// Originally found in include/mysql/mysql_com.h
// See http://dev.mysql.com/doc/internals/en/status-flags.html
const (
	// SERVER_STATUS_AUTOCOMMIT is the default status of auto-commit.
	SERVER_STATUS_AUTOCOMMIT = 0x0002
)

// A few interesting character set values.
// See http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
const (
	// CharacterSetUtf8 is for UTF8. We use this by default.
	CharacterSetUtf8 = 33

	// CharacterSetBinary is for binary. Use by integer fields for instance.
	CharacterSetBinary = 63
)

// CharacterSetMap maps the charset name (used in ConnParams) to the
// integer value.  Interesting ones have their own constant above.
var CharacterSetMap = map[string]uint8{
	"big5":     1,
	"dec8":     3,
	"cp850":    4,
	"hp8":      6,
	"koi8r":    7,
	"latin1":   8,
	"latin2":   9,
	"swe7":     10,
	"ascii":    11,
	"ujis":     12,
	"sjis":     13,
	"hebrew":   16,
	"tis620":   18,
	"euckr":    19,
	"koi8u":    22,
	"gb2312":   24,
	"greek":    25,
	"cp1250":   26,
	"gbk":      28,
	"latin5":   30,
	"armscii8": 32,
	"utf8":     CharacterSetUtf8,
	"ucs2":     35,
	"cp866":    36,
	"keybcs2":  37,
	"macce":    38,
	"macroman": 39,
	"cp852":    40,
	"latin7":   41,
	"utf8mb4":  45,
	"cp1251":   51,
	"utf16":    54,
	"utf16le":  56,
	"cp1256":   57,
	"cp1257":   59,
	"utf32":    60,
	"binary":   CharacterSetBinary,
	"geostd8":  92,
	"cp932":    95,
	"eucjpms":  97,
}

const (
	// Error codes for server-side errors.
	// Originally found in include/mysql/mysqld_error.h

	// ER_ERROR_FIRST enum.
	ER_ERROR_FIRST uint16 = 1000

	// ER_CON_COUNT_ERROR enum.
	ER_CON_COUNT_ERROR uint16 = 1040

	// ER_ACCESS_DENIED_ERROR enum.
	ER_ACCESS_DENIED_ERROR = 1045

	// ER_NO_DB_ERROR enum.
	ER_NO_DB_ERROR = 1046

	// ER_BAD_DB_ERROR enum.
	ER_BAD_DB_ERROR = 1049

	// ER_UNKNOWN_ERROR enum.
	ER_UNKNOWN_ERROR = 1105

	// ER_HOST_NOT_PRIVILEGED enum.
	ER_HOST_NOT_PRIVILEGED = 1130

	// ER_NO_SUCH_TABLE enum.
	ER_NO_SUCH_TABLE = 1146

	// ER_SYNTAX_ERROR enum.
	ER_SYNTAX_ERROR = 1149

	// ER_SPECIFIC_ACCESS_DENIED_ERROR enum.
	ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227

	// ER_OPTION_PREVENTS_STATEMENT enum.
	ER_OPTION_PREVENTS_STATEMENT = 1290

	// ER_MALFORMED_PACKET enum.
	ER_MALFORMED_PACKET = 1835

	// Error codes for client-side errors.
	// Originally found in include/mysql/errmsg.h
	// Used when:
	// - the client cannot write an initial auth packet.
	// - the client cannot read an initial auth packet.
	// - the client cannot read a response from the server.

	// CR_SERVER_LOST enum.
	CR_SERVER_LOST = 2013

	// CR_VERSION_ERROR enum.
	// This is returned if the server versions don't match what we support.
	CR_VERSION_ERROR = 2007
)

// SQLErrors is the list of sql errors.
var SQLErrors = map[uint16]*SQLError{
	ER_CON_COUNT_ERROR:              &SQLError{Num: ER_CON_COUNT_ERROR, State: "08004", Message: "Too many connections"},
	ER_ACCESS_DENIED_ERROR:          &SQLError{Num: ER_ACCESS_DENIED_ERROR, State: "28000", Message: "Access denied for user '%-.48s'@'%-.64s' (using password: %s)"},
	ER_NO_DB_ERROR:                  &SQLError{Num: ER_NO_DB_ERROR, State: "3D000", Message: "No database selected"},
	ER_BAD_DB_ERROR:                 &SQLError{Num: ER_BAD_DB_ERROR, State: "42000", Message: "Unknown database '%-.192s'"},
	ER_UNKNOWN_ERROR:                &SQLError{Num: ER_UNKNOWN_ERROR, State: "HY000", Message: ""},
	ER_HOST_NOT_PRIVILEGED:          &SQLError{Num: ER_HOST_NOT_PRIVILEGED, State: "HY000", Message: "Host '%-.64s' is not allowed to connect to this MySQL server"},
	ER_NO_SUCH_TABLE:                &SQLError{Num: ER_NO_SUCH_TABLE, State: "42S02", Message: "Table '%s' doesn't exist"},
	ER_SYNTAX_ERROR:                 &SQLError{Num: ER_SYNTAX_ERROR, State: "42000", Message: "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use, %s"},
	ER_SPECIFIC_ACCESS_DENIED_ERROR: &SQLError{Num: ER_SPECIFIC_ACCESS_DENIED_ERROR, State: "42000", Message: "Access denied; you need (at least one of) the %-.128s privilege(s) for this operation"},
	ER_OPTION_PREVENTS_STATEMENT:    &SQLError{Num: ER_OPTION_PREVENTS_STATEMENT, State: "42000", Message: "The MySQL server is running with the %s option so it cannot execute this statement"},
	ER_MALFORMED_PACKET:             &SQLError{Num: ER_MALFORMED_PACKET, State: "HY000", Message: "Malformed communication packet."},
	CR_SERVER_LOST:                  &SQLError{Num: CR_SERVER_LOST, State: "HY000", Message: ""},
}
