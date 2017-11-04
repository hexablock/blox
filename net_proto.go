package blox

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hexablock/blox/block"
)

const (
	maxFrameSize uint64 = 0xFFFFFFFFFFFFFFFF
	headerSize   uint8  = 2
)

// Header is an arbitrary header for future purposes.
type Header [headerSize]byte

type request struct {
	Type  byte
	Flags byte
	Hash  []byte
}

// protoConn is the connection with the protocol reader and writer. It implements the net.Conn
// interface
type protoConn struct {
	host string
	net.Conn
	lastused time.Time
}

func (conn *protoConn) WriteHeader(header Header) error {
	// Write header.
	n, err := conn.Write(header[:])
	if err == nil {
		if n != int(headerSize) {
			err = errIncompleteWrite
		}
	}
	return err
}

func (conn *protoConn) WriteFrame(header Header, data []byte) error {
	// Check size accounting for header
	size := uint64(len(data))
	if size > (maxFrameSize - uint64(headerSize)) {
		return errExceededPayload
	}
	//log.Println("FRAME SIZE", size)

	sz := make([]byte, 8)
	binary.BigEndian.PutUint64(sz, size)
	hdr := append(header[:], sz...)

	// Write header and size atomically
	n, err := conn.Write(hdr)
	if err != nil {
		return err
	} else if n != len(hdr) {
		return errIncompleteWrite
	}

	// Write payload
	var c uint64
	for {
		if c == size {
			break
		}

		var n int
		if n, err = conn.Write(data[c:]); err != nil {
			break
		}

		c += uint64(n)
	}

	// Return written bytes and/or error
	return err
}

func (conn *protoConn) ReadSize() (uint64, error) {
	sz := make([]byte, 8)
	if _, err := io.ReadFull(conn, sz); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(sz), nil
}

// ReadData reads the size of data then reads that many bytes from the connection.
func (conn *protoConn) ReadData() ([]byte, error) {
	sz, err := conn.ReadSize()
	if err != nil {
		return nil, err
	}

	// Check payload size
	if sz > (maxFrameSize - uint64(headerSize)) {
		return nil, errExceededPayload
	}

	// Read payload
	p := make([]byte, sz)
	_, err = io.ReadFull(conn, p)
	return p, err
}

// readResponseHeader reads the header and checks the response status.  If there
// is failure, it tries to read the error message.
func (conn *protoConn) readResponseHeader() error {
	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return err
	}

	if hdr[1] == respOk {
		return nil
	}

	// Read in the error if there was a failure
	ebytes, err := conn.ReadData()
	if err == nil {
		err = block.ParseError(string(ebytes))
	} else {
		err = fmt.Errorf("failed to read error message")
	}
	return err

}

// readRequest reads the op and id for the request made by a client
//func (conn *protoConn) readRequest(hashSize int) (byte, []byte, error) {
func (conn *protoConn) readRequest(hashSize int) (*request, error) {
	raw := make([]byte, 2+hashSize)

	if _, err := io.ReadFull(conn, raw); err != nil {
		return nil, err
	}

	return &request{Type: raw[0], Flags: raw[1], Hash: raw[2:]}, nil
}
