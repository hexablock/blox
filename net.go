package blox

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/utils"
	"github.com/hexablock/log"
)

const (
	//reqTypeNew byte = iota + 3
	reqTypeGet byte = iota + 3
	reqTypeSet
	reqTypeRemove
)

const (
	respOk byte = iota
	respFail
)

var (
	errExceededPayload   = errors.New("payload size exceeded")
	errTransportShutdown = errors.New("transport shutdown")
)

// BlockDevice implements a block storage interface.  It abstracts the index, tree and data
// block operations.
type BlockDevice interface {
	SetBlock(block.Block) ([]byte, error)
	GetBlock(id []byte) (block.Block, error)
	RemoveBlock(id []byte) error
	BlockExists(id []byte) bool
}

// NetTransport is the network transport for block operations
type NetTransport struct {
	// Client transport
	*NetClient
	// Listener
	ln net.Listener
	// Incoming connections
	inbound *inPool
	// Underlying local storage
	store BlockDevice

	shutdown int32
}

// NewNetTransport initializes a new network transport for the store
func NewNetTransport(ln net.Listener, opts NetClientOptions) *NetTransport {
	trans := &NetTransport{
		ln:        ln,
		inbound:   newInPool(),
		NetClient: NewNetClient(opts),
	}

	return trans
}

// Register registers the store with the transport
func (trans *NetTransport) Register(store BlockDevice) {
	trans.store = store
	go trans.listen()
}

func (trans *NetTransport) listen() {

	for {
		// Exit if a shutdown was signalled
		if atomic.LoadInt32(&trans.shutdown) == 1 {
			log.Println("[INFO] Transport shutting down")
			break
		}
		// Accept a TCP connection
		c, err := trans.ln.Accept()
		if err != nil {
			log.Println("[ERROR]", err)
			continue
		}

		log.Println("[DEBUG] Connection accepted:", c.RemoteAddr().String())

		// Register and get wrapped connection
		conn := trans.inbound.register(c)

		// Fire off handler
		go trans.handleConn(conn)
	}

	log.Println("[INFO] Network transport shutdown!")
}

func (trans *NetTransport) setBlockServe(id []byte, conn *protoConn) (bool, error) {
	//log.Printf("[DEBUG] Server SetBlock block=%x", id)

	// If we already have the block, simply return
	if trans.store.BlockExists(id) {
		return false, block.ErrBlockExists
	}

	// Ack that we can accept the block
	if err := conn.WriteHeader(Header{reqTypeSet, respOk}); err != nil {
		return true, err
	}

	typ, size, err := readBlockTypeAndSize(conn)
	if err != nil {
		return false, err
	}
	log.Printf("NetTransport.setBlockServe id=%x type=%s size=%d", id, typ, size)

	//
	// TODO: check type and create block accordingly.
	//

	// source address
	us := "tcp://" + conn.RemoteAddr().String() + "/" + hex.EncodeToString(id)
	uri := block.NewURI(us)
	netblk := block.NewStreamedBlock(typ, uri, trans.hasher, conn, size)
	nid, err := trans.store.SetBlock(netblk)
	if err != nil {
		log.Printf("[ERROR] setBlockServe %v", err)
		return false, err
	}

	//log.Printf("NetTransport.setBlockServe new-id=%x id=%x", nid, id)

	// Let the requestor know we've accepted the block.
	if err = writeHeaderAndID(conn, Header{reqTypeSet, respOk}, nid); err != nil {
		return true, err
	}

	//log.Printf("NetTransport.setBlockServe block set id=%x", nid)
	return false, nil
}

// func (trans *NetTransport) newBlockServe(conn *protoConn) (bool, error) {
// 	typ, err := block.ReadBlockType(conn)
// 	if err != nil {
// 		return true, err
// 	}
//
// 	nblk, _ := trans.store.NewBlock(typ)
// 	wr, err := nblk.Writer()
// 	if err == nil {
// 		defer wr.Close()
//
// 		if _, err = io.Copy(wr, conn); err == nil {
// 			return false, nil
// 		}
// 	}
//
// 	return true, err
// }

func (trans *NetTransport) getBlockServe(conn *protoConn, id []byte) (bool, error) {
	//log.Printf("[DEBUG] NetTransport.getBlockServe id=%x", id)

	// Get the block from the local store
	blk, err := trans.store.GetBlock(id)
	if err != nil {
		return false, err
	}
	// Acknowledge that we have the block.
	if err = conn.WriteHeader(Header{reqTypeGet, respOk}); err != nil {
		// Immediately close connection if the ack fails
		return true, err
	}

	//log.Println("WRITING", blk.Type(), blk.Size())

	// Write the type and size of the block.
	if err = writeBlockTypeAndSize(conn, blk.Type(), blk.Size()); err != nil {
		return true, err
	}
	log.Printf("[DEBUG] NetTransport.getBlockServe id=%x type=%s size=%d", blk.ID(), blk.Type(), blk.Size())

	// Get reader for local block
	var src io.ReadCloser
	if src, err = blk.Reader(); err != nil {
		return true, err
	}

	// Copy data to the connection
	err = utils.CopyNAndCheck(conn, src, int64(blk.Size()))
	// Close the source
	src.Close()
	// Disconnect if there was an error copying the byte stream
	if err != nil {
		return true, err
	}

	return false, nil
}

func (trans *NetTransport) handleConn(conn *protoConn) {
	// Release the connection upon exiting this function
	defer trans.inbound.release(conn)

	caddr := conn.RemoteAddr().String()
	// Request handler loop
	for {
		// Get request
		op, id, err := conn.readRequest(trans.blockHashSize)
		if err != nil {
			if err != io.EOF {
				log.Println("[ERROR] Reading request:", err)
			}
			return
		}

		log.Printf("[DEBUG] TCP request client=%s op=%x id=%x", caddr, op, id)
		var disconnect bool

		// Serve op
		switch op {
		// case reqTypeNew:
		// 	var disconnect bool
		// 	if disconnect, err = trans.newBlockServe(conn); err != nil {
		// 		if disconnect {
		// 			log.Println("[ERROR]", err)
		// 			return
		// 		}
		// 	}

		case reqTypeGet:
			disconnect, err = trans.getBlockServe(conn, id)

		case reqTypeRemove:
			if err = trans.store.RemoveBlock(id); err == nil {
				if err = conn.WriteHeader(Header{op, respOk}); err != nil {
					disconnect = true
				}
			}

		case reqTypeSet:
			disconnect, err = trans.setBlockServe(id, conn)

		default:
			log.Printf("[ERROR] Invalid request client=%s op=%x id=%x", caddr, op, id)
			return

		}

		if err == nil {
			log.Printf("[INFO] TCP response client=%s op=%x id=%x", caddr, op, id)
			continue
		} else if disconnect {
			log.Printf("[ERROR] Disconnecting client=%s reason='%v'", caddr, err)
			return
		}

		// Write error response
		hdr := Header{op, respFail}
		log.Printf("[DEBUG] TCP response client=%s header=%x id=%x error='%v'",
			caddr, hdr, id, err)

		if err = conn.WriteFrame(hdr, []byte(err.Error())); err != nil {
			log.Printf("[ERROR] Failed to write error frame: %v", err)
			// Close the connection if we failed to write the error response
			return
		}

	}

}

func (trans *NetTransport) returnConn(o *protoConn) {
	// Close and return if we are shutting down
	if atomic.LoadInt32(&trans.shutdown) == 1 {
		o.Close()
		return
	}

	trans.pool.returnConn(o)
}

func (trans *NetTransport) getConn(host string) (*protoConn, error) {
	if atomic.LoadInt32(&trans.shutdown) == 1 {
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	return trans.pool.getConn(host)
}

// Shutdown issues a shutdown of the transport.  It stops taking new incoming connections
func (trans *NetTransport) Shutdown() error {
	atomic.StoreInt32(&trans.shutdown, 1)
	return nil
}
