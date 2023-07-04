package protocol

import (
	"io"
)

const (
	ClientInit = iota
	ClientWaitGet
	ClientWaitResponse
)

type StatefulReadWriter interface {
	io.ReadWriter
	GetState() int
	SetState(state int)
	GetName() string
}

type ClientError struct {
	errStr string
}

func (e ClientError) Error() string {
	return e.errStr
}

var (
	ClientErrInvalid    = ClientError{"E_INVALID"}
	ClientErrBadTopic   = ClientError{"E_BAD_TOPIC"}
	ClientErrBadMessage = ClientError{"E_BAD_MESSAGE"}
)
