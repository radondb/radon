/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"fmt"
	"net"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/xelabs/go-mysqlstack/proto"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Handler interface.
type Handler interface {
	ServerVersion() string
	SetServerVersion()
	NewSession(session *Session)
	SessionInc(session *Session)
	SessionDec(session *Session)
	SessionClosed(session *Session)
	SessionCheck(session *Session) error
	AuthCheck(session *Session) error
	ComInitDB(session *Session, database string) error
	ComQuery(session *Session, query string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error
}

// Listener is a connection handler.
type Listener struct {
	// Logger.
	log *xlog.Log

	address string

	// Query handler.
	handler Handler

	// This is the main listener socket.
	listener net.Listener

	// Incrementing ID for connection id.
	connectionID uint32
}

// NewListener creates a new Listener.
func NewListener(log *xlog.Log, address string, handler Handler) (*Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	return &Listener{
		log:          log,
		address:      address,
		handler:      handler,
		listener:     listener,
		connectionID: 1,
	}, nil
}

// Accept runs an accept loop until the listener is closed.
func (l *Listener) Accept() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// Close() was probably called.
			return
		}
		ID := l.connectionID
		l.connectionID++
		go l.handle(conn, ID)
	}
}

func (l *Listener) parserComInitDB(data []byte) string {
	return string(data[1:])
}

func (l *Listener) parserComQuery(data []byte) string {
	// Trim the right.
	data = data[1:]
	last := len(data) - 1
	if data[last] == ';' {
		data = data[:last]
	}
	return common.BytesToString(data)
}

func (l *Listener) parserComStatement(data []byte, session *Session) (*Statement, error) {
	data = data[1:]
	buf := common.ReadBuffer(data)
	stmtID, err := buf.ReadU32()
	if err != nil {
		return nil, err
	}
	stmt, ok := session.statements[stmtID]
	if !ok {
		return nil, fmt.Errorf("can.not.found.the.stmt.id:%v", stmtID)
	}
	return stmt, nil
}

func (l *Listener) parserComStatementExecute(data []byte, session *Session) (*Statement, error) {
	stmt, err := l.parserComStatement(data, session)
	if err != nil {
		return nil, err
	}

	protoStmt := &proto.Statement{
		ID:         stmt.ID,
		ParamCount: stmt.ParamCount,
		ParamsType: stmt.ParamsType,
		BindVars:   stmt.BindVars,
	}
	if err = proto.UnPackStatementExecute(data[1:], protoStmt, sqltypes.ParseMySQLValues); err != nil {
		return nil, err
	}
	return stmt, nil
}

// handle is called in a go routine for each client connection.
func (l *Listener) handle(conn net.Conn, ID uint32) {
	var err error
	var data []byte
	var authPkt []byte
	var greetingPkt []byte
	log := l.log

	// Catch panics, and close the connection in any case.
	defer func() {
		conn.Close()
		if x := recover(); x != nil {
			log.Error("server.handle.panic:\n%v\n%s", x, debug.Stack())
		}
	}()

	// set server version if backend MySQL version is different.
	l.handler.SetServerVersion()

	session := newSession(log, ID, l.handler.ServerVersion(), conn)
	// Session check.
	if err = l.handler.SessionCheck(session); err != nil {
		log.Warning("session[%v].check.failed.error:%+v", ID, err)
		session.writeErrFromError(err)
		return
	}

	// Session register.
	l.handler.NewSession(session)
	defer l.handler.SessionClosed(session)

	// Greeting packet.
	greetingPkt = session.greeting.Pack()
	if err = session.packets.Write(greetingPkt); err != nil {
		log.Error("server.write.greeting.packet.error: %v", err)
		return
	}

	// Auth packet.
	if authPkt, err = session.packets.Next(); err != nil {
		log.Error("server.read.auth.packet.error: %v", err)
		return
	}
	if err = session.auth.UnPack(authPkt); err != nil {
		log.Error("server.unpack.auth.error: %v", err)
		return
	}

	//  Auth check.
	if err = l.handler.AuthCheck(session); err != nil {
		log.Warning("server.user[%+v].auth.check.failed", session.User())
		session.writeErrFromError(err)
		return
	}

	// Check the database.
	db := session.auth.Database()
	if db != "" {
		if err = l.handler.ComInitDB(session, db); err != nil {
			log.Error("server.cominitdb[%s].error:%+v", db, err)
			session.writeErrFromError(err)
			return
		}
		session.SetSchema(db)
	}

	if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
		return
	}

	l.handler.SessionInc(session)
	defer l.handler.SessionDec(session)

	// Reset packet sequence ID.
	session.packets.ResetSeq()
	for {
		if data, err = session.packets.Next(); err != nil {
			return
		}

		// Update the session last query time for session idle.
		session.updateLastQueryTime(time.Now())
		switch data[0] {
		// COM_QUIT
		case sqldb.COM_QUIT:
			return
			// COM_INIT_DB
		case sqldb.COM_INIT_DB:
			db := l.parserComInitDB(data)
			if err = l.handler.ComInitDB(session, db); err != nil {
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			} else {
				session.SetSchema(db)
				if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
					return
				}
			}
			// COM_PING
		case sqldb.COM_PING:
			if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
				return
			}
			// COM_QUERY
		case sqldb.COM_QUERY:
			query := l.parserComQuery(data)
			if err = l.handler.ComQuery(session, query, nil, func(qr *sqltypes.Result) error {
				return session.writeTextRows(qr)
			}); err != nil {
				log.Error("server.handle.query.from.session[%v].error:%+v.query[%s]", ID, err, query)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			}
			// COM_STMT_PREPARE
		case sqldb.COM_STMT_PREPARE:
			session.statementID++
			id := session.statementID
			query := l.parserComQuery(data)
			paramCount := uint16(strings.Count(query, "?"))
			stmt := &Statement{
				ID:          id,
				PrepareStmt: query,
				ParamCount:  paramCount,
				ParamsType:  make([]int32, paramCount),
				BindVars:    make(map[string]*querypb.BindVariable, paramCount),
			}
			for i := uint16(0); i < paramCount; i++ {
				stmt.BindVars[fmt.Sprintf("v%d", i+1)] = &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: []byte("?")}
			}
			session.statements[id] = stmt
			if err := session.writeStatementPrepareResult(stmt); err != nil {
				log.Error("server.handle.stmt.prepare.from.session[%v].error:%+v.query[%s]", ID, err, query)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
				delete(session.statements, id)
			}
			// COM_STMT_EXECUTE
		case sqldb.COM_STMT_EXECUTE:
			stmt, err := l.parserComStatementExecute(data, session)
			if err != nil {
				log.Error("server.handle.stmt.execute.from.session[%v].error:%+v", ID, err)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			}

			if err = l.handler.ComQuery(session, stmt.PrepareStmt, sqltypes.CopyBindVariables(stmt.BindVars), func(qr *sqltypes.Result) error {
				return session.writeBinaryRows(qr)
			}); err != nil {
				log.Error("server.handle.stmt.prepare.from.session[%v].error:%+v", ID, err)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			}
			// COM_STMT_RESET
		case sqldb.COM_STMT_RESET:
			stmt, err := l.parserComStatement(data, session)
			if err != nil {
				log.Error("server.handle.stmt.reset.from.session[%v].error:%+v", ID, err)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			}
			if stmt.ParamCount > 0 {
				stmt.BindVars = make(map[string]*querypb.BindVariable, stmt.ParamCount)
			}
			if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
				return
			}
			// COM_STMT_CLOSE
		case sqldb.COM_STMT_CLOSE:
			stmt, err := l.parserComStatement(data, session)
			if err != nil {
				log.Error("server.handle.stmt.close.from.session[%v].error:%+v", ID, err)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			}
			delete(session.statements, stmt.ID)
		default:
			cmd := sqldb.CommandString(data[0])
			log.Error("session.command:%s.not.implemented", cmd)
			sqlErr := sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "command handling not implemented yet: %s", cmd)
			if err := session.writeErrFromError(sqlErr); err != nil {
				return
			}
		}
		// Reset packet sequence ID.
		session.packets.ResetSeq()
	}
}

// Addr returns the client address.
func (l *Listener) Addr() string {
	return l.address
}

// Close close the listener and all connections.
func (l *Listener) Close() {
	l.listener.Close()
}
