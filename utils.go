package blox

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/hexablock/blox/block"
)

var (
	errIncompleteWrite = errors.New("incomplete write")
	errIncompleteRead  = errors.New("incomplete read")
)

func writeHeaderAndID(wr io.Writer, header Header, id []byte) error {
	d := append(header[:], id...)
	// Write header
	n, err := wr.Write(d)
	if err == nil {
		if n != len(d) {
			err = errIncompleteWrite
		}
	}

	return err
}

func writeBlockTypeAndSize(conn *protoConn, typ block.BlockType, size uint64) error {

	sz := make([]byte, 8)
	binary.BigEndian.PutUint64(sz, size)
	d := append([]byte{byte(typ)}, sz...)

	n, err := conn.Write(d)
	if err != nil {
		return err
	} else if n != len(d) {
		return errIncompleteWrite
	}
	return nil
}

func readBlockTypeAndSize(conn *protoConn) (block.BlockType, uint64, error) {
	a := make([]byte, 9)
	if _, err := io.ReadFull(conn, a); err != nil {
		return block.BlockType(0), 0, err
	}

	// n, err := conn.Read(a)
	// if err != nil {
	// 	return block.BlockType(0), 0, err
	// } else if n != len(a) {
	// 	return block.BlockType(0), 0, errIncompleteRead
	// }

	return block.BlockType(a[0]), binary.BigEndian.Uint64(a[1:]), nil

	// if btype, err = block.ReadBlockType(conn); err == nil {
	// 	size, err = conn.ReadSize()
	// }
	// return btype, size, err
}

func setupTCPConn(conn net.Conn) {
	c := conn.(*net.TCPConn)
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
}
