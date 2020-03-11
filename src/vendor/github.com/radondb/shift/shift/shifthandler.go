package shift

type ShiftHandler interface {
	// Start used to start a shift work.
	Start() error

	// WaitFinish used to wait success or fail signal to finish.
	WaitFinish() error

	// ChecksumTable used to checksum data src tbl and dst tbl.
	ChecksumTable() error

	// SetStopSignal() used set a stop signal to stop a shift work.
	SetStopSignal()
}
