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

// TODO
// This needs to be re-done to allow connection re-use.  Consider using http
// with re-usable connections to simplify all of net

const (
	reqTypeGet byte = iota + 3
	reqTypeExists
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

// NetTransport is the network transport for block operations
type NetTransport struct {
	// Client transport
	*NetClient

	// TCP Listener
	ln *net.TCPListener

	// Incoming connections
	inbound *inPool

	// Underlying local storage
	dev BlockDevice

	shutdown int32
}

// NewNetTransport initializes a new network transport for the store
func NewNetTransport(opts NetClientOptions) *NetTransport {
	trans := &NetTransport{
		inbound:   newInPool(),
		NetClient: NewNetClient(opts),
	}

	return trans
}

// Register registers a BlockDevice with the transport.   This must be called
// before a call to Start is made
func (trans *NetTransport) Register(dev BlockDevice) {
	trans.dev = dev

}

// Start checks for a block device registration and starts a server to service
// incoming requests. Register must be called before starting the transport.
// Register and Start are not thread safe
func (trans *NetTransport) Start(ln *net.TCPListener) error {
	if trans.dev == nil {
		return fmt.Errorf("block device not registered")
	}

	trans.ln = ln

	go trans.listen()

	return nil
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
	if ok, _ := trans.dev.BlockExists(id); ok {
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
	log.Printf("[DEBUG] NetTransport.setBlockServe id=%x type=%s size=%d", id, typ, size)

	//
	// TODO: check type and create block accordingly.
	//

	// source address
	us := "tcp://" + conn.RemoteAddr().String() + "/" + hex.EncodeToString(id)
	uri := block.NewURI(us)
	netblk := block.NewStreamedBlock(typ, uri, trans.hasher, conn, size)
	nid, err := trans.dev.SetBlock(netblk)
	if err != nil {
		//log.Printf("[ERROR] setBlockServe %v", err)
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

func (trans *NetTransport) blockExistsServe(conn *protoConn, id []byte) (bool, error) {
	ok, err := trans.dev.BlockExists(id)
	if err != nil {
		return false, err
	}
	err = conn.WriteHeader(Header{reqTypeExists, respOk})
	if err != nil {
		// Immediately close connection if the ack fails
		return true, err
	}

	if !ok {
		_, err = conn.Write([]byte{0})
	} else {
		_, err = conn.Write([]byte{1})
	}

	if err != nil {
		return true, err
	}

	return false, nil
}

func (trans *NetTransport) getBlockServe(conn *protoConn, id []byte) (bool, error) {
	//log.Printf("[DEBUG] NetTransport.getBlockServe id=%x", id)

	// Get the block from the local store
	blk, err := trans.dev.GetBlock(id)
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
		req, err := conn.readRequest(trans.blockHashSize)
		if err != nil {
			if err != io.EOF {
				log.Println("[ERROR] Reading request:", err)
			}
			return
		}

		log.Printf("[DEBUG] TCP request client=%s method=%x id=%x", caddr, req.Type, req.Hash)

		var disconnect bool

		// Serve op
		switch req.Type {
		case reqTypeExists:
			disconnect, err = trans.blockExistsServe(conn, req.Hash)

		case reqTypeGet:
			disconnect, err = trans.getBlockServe(conn, req.Hash)

		case reqTypeRemove:
			if err = trans.dev.RemoveBlock(req.Hash); err == nil {
				if err = conn.WriteHeader(Header{req.Type, respOk}); err != nil {
					disconnect = true
				}
			}

		case reqTypeSet:
			disconnect, err = trans.setBlockServe(req.Hash, conn)

		default:
			log.Printf("[ERROR] Invalid request client=%s op=%x id=%x", caddr, req.Type, req.Hash)
			return

		}

		if err == nil {
			log.Printf("[DEBUG] TCP response client=%s op=%x id=%x", caddr, req.Type, req.Hash)
			continue
		} else if disconnect {
			log.Printf("[ERROR] Disconnecting client=%s reason='%v'", caddr, err)
			return
		}

		// Write error response
		hdr := Header{req.Type, respFail}
		log.Printf("[DEBUG] TCP response client=%s header=%x id=%x error='%v'",
			caddr, hdr, req.Hash, err)

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
