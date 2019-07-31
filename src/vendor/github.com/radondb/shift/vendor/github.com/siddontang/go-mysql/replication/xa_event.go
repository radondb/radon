package replication

import (
	"encoding/binary"
	"fmt"
	"io"
)

type XAPrepareEvent struct {
	formatID     int32
	gtrid_length int32
	bqual_length int32
	data         []byte
	Query        []byte
}

// GetXAQuery return 'xa prepare' statement
// see MySQL sql/xa_aux.h, function - serialize_xid()
func (e *XAPrepareEvent) getXAQuery() []byte {

	xa := "XA PREPARE"

	pos := 0

	xa = fmt.Sprintf("%s X'%x'", xa, e.data[pos:e.gtrid_length])

	pos += int(e.gtrid_length)

	xa = fmt.Sprintf("%s,X'%x'", xa, e.data[pos:e.gtrid_length+e.bqual_length])
	xa = fmt.Sprintf("%s,%v", xa, e.formatID)

	return []byte(xa)
}

func (e *XAPrepareEvent) Decode(data []byte) error {
	pos := 0

	//skip one_phace
	pos += 1

	e.formatID = int32(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	e.gtrid_length = int32(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	e.bqual_length = int32(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	e.data = data[pos:]

	e.Query = e.getXAQuery()

	return nil
}

func (e *XAPrepareEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "formatID: %d\n", e.formatID)
	fmt.Fprintf(w, "gtrid_length: %d\n", e.gtrid_length)
	fmt.Fprintf(w, "bqual_length: %d\n", e.bqual_length)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}
