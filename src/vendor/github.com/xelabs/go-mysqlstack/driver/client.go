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
	"context"
	"net"
	"strings"
	"time"

	"github.com/xelabs/go-mysqlstack/packet"
	"github.com/xelabs/go-mysqlstack/proto"
	"github.com/xelabs/go-mysqlstack/sqldb"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var _ Conn = &conn{}

// Conn interface.
type Conn interface {
	Ping() error
	Quit()
	Close() error
	Closed() bool
	Cleanup()
	NextPacket() ([]byte, error)

	// ConnectionID is the connection id at greeting.
	ConnectionID() uint32

	InitDB(db string) error
	Command(command byte) error
	Query(sql string) (Rows, error)
	Exec(sql string) error
	FetchAll(sql string, maxrows int) (*sqltypes.Result, error)
	FetchAllWithFunc(sql string, maxrows int, fn Func) (*sqltypes.Result, error)
	ComStatementPrepare(sql string) (*Statement, error)
}

type conn struct {
	netConn  net.Conn
	auth     *proto.Auth
	greeting *proto.Greeting
	packets  *packet.Packets
}

func (c *conn) handleErrorPacket(data []byte) error {
	if data[0] == proto.ERR_PACKET {
		return c.packets.ParseERR(data)
	}
	return nil
}

func (c *conn) handShake(username, password, database, charset string) error {
	var err error
	var data []byte

	//Parses the initial handshake from the server.
	{
		// greeting read
		if data, err = c.packets.Next(); err != nil {
			return err
		}

		// check greeting packet
		if err = c.handleErrorPacket(data); err != nil {
			return err
		}

		// unpack greeting packet
		if err = c.greeting.UnPack(data); err != nil {
			return err
		}

		// check greating Capability
		if c.greeting.Capability&sqldb.CLIENT_PROTOCOL_41 == 0 {
			err = sqldb.NewSQLError(sqldb.CR_VERSION_ERROR, "cannot connect to servers earlier than 4.1")
			return err
		}
	}

	{
		cs, ok := sqldb.CharacterSetMap[strings.ToLower(charset)]
		if !ok {
			cs = sqldb.CharacterSetUtf8
		}
		// auth pack
		data := c.auth.Pack(
			proto.DefaultClientCapability,
			cs,
			username,
			password,
			c.greeting.Salt,
			database,
		)

		// auth write
		if err = c.packets.Write(data); err != nil {
			return err
		}

		// clean the authreponse bytes to improve the gc pause.
		c.auth.CleanAuthResponse()
	}

	{
		// read
		if data, err = c.packets.Next(); err != nil {
			return err
		}

		if err = c.handleErrorPacket(data); err != nil {
			return err
		}
	}
	return nil
}

// NewConn used to create a new client connection.
// The timeout is 30 seconds.
func NewConn(username, password, address, database, charset string) (Conn, error) {
	var err error
	c := &conn{}
	timeout := time.Duration(30) * time.Second
	if c.netConn, err = net.DialTimeout("tcp", address, timeout); err != nil {
		return nil, err
	}

	// Set KeepAlive to True and period to 180s.
	if tcpConn, ok := c.netConn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(time.Second * 180)
		c.netConn = tcpConn
	}

	defer func() {
		if err != nil {
			c.Cleanup()
		}
	}()
	// Set timeouts, make the handshake timeout if the underflying connection blocked.
	// This timeout only used in handshake, we will disable(set zero time) it at last.
	c.netConn.SetReadDeadline(time.Now().Add(timeout))
	defer c.netConn.SetReadDeadline(time.Time{})

	c.auth = proto.NewAuth()
	c.greeting = proto.NewGreeting(0, "")
	c.packets = packet.NewPackets(c.netConn)
	if err = c.handShake(username, password, database, charset); err != nil {
		return nil, err
	}
	return c, nil
}

// NextPacket used to get the next packet
func (c *conn) NextPacket() ([]byte, error) {
	return c.packets.Next()
}

func (c *conn) baseQuery(mode RowMode, command byte, datas []byte) (Rows, error) {
	var ok *proto.OK
	var myerr, err error
	var columns []*querypb.Field
	var colNumber int

	// if err != nil means the connection is broken(packet error)
	defer func() {
		if err != nil {
			c.Cleanup()
		}
	}()

	// Query.
	if err = c.packets.WriteCommand(command, datas); err != nil {
		return nil, err
	}

	// Read column number.
	ok, colNumber, myerr, err = c.packets.ReadComQueryResponse()
	if err != nil {
		return nil, err
	}
	if myerr != nil {
		return nil, myerr
	}

	if colNumber > 0 {
		if columns, err = c.packets.ReadColumns(colNumber); err != nil {
			return nil, err
		}

		// Read EOF.
		if (c.greeting.Capability & sqldb.CLIENT_DEPRECATE_EOF) == 0 {
			if err = c.packets.ReadEOF(); err != nil {
				return nil, err
			}
		}
	}
	var rows Rows
	switch mode {
	case TextRowMode:
		textRows := NewTextRows(c)
		textRows.rowsAffected = ok.AffectedRows
		textRows.insertID = ok.LastInsertID
		textRows.fields = columns
		rows = textRows
	case BinaryRowMode:
		binRows := NewBinaryRows(c)
		binRows.rowsAffected = ok.AffectedRows
		binRows.insertID = ok.LastInsertID
		binRows.fields = columns
		rows = binRows
	}
	return rows, nil
}

func (c *conn) comQuery(command byte, datas []byte) (Rows, error) {
	return c.baseQuery(TextRowMode, command, datas)
}

func (c *conn) stmtQuery(command byte, datas []byte) (Rows, error) {
	return c.baseQuery(BinaryRowMode, command, datas)
}

// ConnectionID is the connection id at greeting
func (c *conn) ConnectionID() uint32 {
	return c.greeting.ConnectionID
}

// Query execute the query and return the row iterator
func (c *conn) Query(sql string) (Rows, error) {
	return c.comQuery(sqldb.COM_QUERY, common.StringToBytes(sql))
}

// Ping -- ping command.
func (c *conn) Ping() error {
	rows, err := c.comQuery(sqldb.COM_PING, []byte{})
	if err != nil {
		return err
	}
	return rows.Close()
}

// InitDB -- Init DB command.
func (c *conn) InitDB(db string) error {
	rows, err := c.comQuery(sqldb.COM_INIT_DB, common.StringToBytes(db))
	if err != nil {
		return err
	}
	return rows.Close()
}

// Exec executes the query and drain the results
func (c *conn) Exec(sql string) error {
	rows, err := c.comQuery(sqldb.COM_QUERY, common.StringToBytes(sql))
	if err != nil {
		return err
	}

	if err := rows.Close(); err != nil {
		c.Cleanup()
	}
	return nil
}

// FetchAll -- fetch all command.
func (c *conn) FetchAll(sql string, maxrows int) (*sqltypes.Result, error) {
	return c.FetchAllWithFunc(sql, maxrows, func(rows Rows) error { return nil })
}

// Func calls on every rows.Next.
// If func returns error, the row.Next() is interrupted and the error is return.
type Func func(rows Rows) error

func (c *conn) FetchAllWithFunc(sql string, maxrows int, fn Func) (*sqltypes.Result, error) {
	var err error
	var iRows Rows
	var qrRow []sqltypes.Value
	var qrRows [][]sqltypes.Value

	if iRows, err = c.comQuery(sqldb.COM_QUERY, common.StringToBytes(sql)); err != nil {
		return nil, err
	}

	for iRows.Next() {
		// callback check.
		if err = fn(iRows); err != nil {
			break
		}

		// Max rows check.
		if len(qrRows) == maxrows {
			break
		}
		if qrRow, err = iRows.RowValues(); err != nil {
			c.Cleanup()
			return nil, err
		}
		if qrRow != nil {
			qrRows = append(qrRows, qrRow)
		}
	}

	// Drain the results and check last error.
	if err := iRows.Close(); err != nil {
		c.Cleanup()
		return nil, err
	}

	rowsAffected := iRows.RowsAffected()
	if rowsAffected == 0 {
		rowsAffected = uint64(len(qrRows))
	}
	qr := &sqltypes.Result{
		Fields:       iRows.Fields(),
		RowsAffected: rowsAffected,
		InsertID:     iRows.LastInsertID(),
		Rows:         qrRows,
	}
	return qr, err
}

// ComStatementPrepare -- statement prepare command.
func (c *conn) ComStatementPrepare(sql string) (*Statement, error) {
	if err := c.packets.WriteCommand(sqldb.COM_STMT_PREPARE, common.StringToBytes(sql)); err != nil {
		return nil, err
	}
	stmt, err := c.packets.ReadStatementPrepareResponse(c.greeting.Capability)
	if err != nil {
		return nil, err
	}
	return &Statement{
		conn:        c,
		ID:          stmt.ID,
		ColumnNames: stmt.ColumnNames,
	}, nil
}

// Command -- execute a command.
func (c *conn) Command(command byte) error {
	rows, err := c.comQuery(command, []byte{})
	if err != nil {
		return err
	}

	if err := rows.Close(); err != nil {
		c.Cleanup()
	}
	return nil
}

// Quit -- quite command.
func (c *conn) Quit() {
	c.packets.WriteCommand(sqldb.COM_QUIT, nil)
}

// Cleanup -- cleanup connection.
func (c *conn) Cleanup() {
	if c.netConn != nil {
		c.netConn.Close()
		c.netConn = nil
	}
}

// Close closes the connection
func (c *conn) Close() error {
	if c != nil && c.netConn != nil {
		quitCh := make(chan struct{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		// First to send quit, if quit timeout force to do cleanup.
		go func(c *conn) {
			c.Quit()
			close(quitCh)
		}(c)

		select {
		case <-ctx.Done():
			c.Cleanup()
		case <-quitCh:
			c.Cleanup()
		}
	}
	return nil
}

// Closed checks the connection broken or not
func (c *conn) Closed() bool {
	return c.netConn == nil
}
