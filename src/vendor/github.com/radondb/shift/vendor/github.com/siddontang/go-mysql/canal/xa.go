package canal

import ()

// XAEvent --
type XAEvent struct {
	Action string
	Query  []byte
}

func newXAEvent(query []byte) *XAEvent {
	return &XAEvent{
		Query: query,
	}
}
