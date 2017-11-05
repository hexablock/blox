package blox

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/utils"
	"github.com/hexablock/log"
)

// NetBlock is network block backed by the network connection.  It primarily is
// used to return the connection back to the pool
type NetBlock struct {
	*block.StreamedBlock
	conn *protoConn
	pool *outPool
}

// Close returns the connection to the pool. If there was an error closing the underlying
// StreamedBlock the connection is closed.  This is used for a network GetBlock request
func (blk *NetBlock) Close() error {
	err := blk.StreamedBlock.Close()
	if err == nil {
		blk.pool.returnConn(blk.conn)
		return nil
	}
	blk.conn.Close()
	return err
}

// NetClientOptions are options available when using the network client
type NetClientOptions struct {
	Timeout      time.Duration
	MaxIdle      time.Duration
	ReapInterval time.Duration
	Hasher       func() hash.Hash
}

// DefaultNetClientOptions returns a set of sane defaults.  It takes the hash size
// as input
func DefaultNetClientOptions(hasher func() hash.Hash) NetClientOptions {
	return NetClientOptions{
		Timeout:      3 * time.Second,
		MaxIdle:      3 * time.Minute,
		ReapInterval: 30 * time.Second,
		Hasher:       hasher,
	}
}

// NetClient is a network client to perform block operations remotely
type NetClient struct {
	// Hash function to use
	hasher func() hash.Hash

	// hash size calculated internally
	blockHashSize int

	// Outbound connection pool
	reapInterval time.Duration
	pool         *outPool
}

// NewNetClient inits a new client transport with the given connection pool,
// connection reap interval and hash size of ids
func NewNetClient(opt NetClientOptions) *NetClient {
	client := &NetClient{
		pool:          newOutPool(opt.Timeout, opt.MaxIdle),
		reapInterval:  opt.ReapInterval,
		hasher:        opt.Hasher,
		blockHashSize: opt.Hasher().Size(),
	}

	go client.reap()

	return client
}

func (trans *NetClient) BlockExists(host string, id []byte) (bool, error) {
	if len(id) != trans.blockHashSize {
		return false, block.ErrInvalidBlock
	}

	conn, err := trans.pool.getConn(host)
	if err != nil {
		return false, err
	}

	if err = writeHeaderAndID(conn, Header{reqTypeExists, respOk}, id); err != nil {
		conn.Close()
		return false, err
	}

	// Read response header
	if err = conn.readResponseHeader(); err != nil {
		trans.pool.returnConn(conn)
		return false, err
	}

	b := make([]byte, 1)
	if _, err = conn.Read(b); err != nil {
		conn.Close()
		return false, err
	}

	if b[0] == 0 {
		return false, nil
	}
	return true, nil
}

// GetBlock makes a GetBlock request to the remote host.
func (trans *NetClient) GetBlock(host string, id []byte) (block.Block, error) {
	if len(id) != trans.blockHashSize {
		return nil, block.ErrInvalidBlock
	}

	conn, err := trans.pool.getConn(host)
	if err != nil {
		return nil, err
	}

	if err = writeHeaderAndID(conn, Header{reqTypeGet, respOk}, id); err != nil {
		conn.Close()
		return nil, err
	}

	// Read response header
	if err = conn.readResponseHeader(); err != nil {
		trans.pool.returnConn(conn)
		return nil, err
	}

	//log.Printf("NetClient.GetBlock confirmation id=%x", id)

	// Create new NetBlock.  Size is the size of the network payload not to be confused
	// with Block.Size().  That is only true for DataBlocks.
	typ, size, err := readBlockTypeAndSize(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	if typ == block.BlockTypeData {
		// Return the new Block with the conn attached as the reader that can be read later.
		uri := block.NewURI("tcp://" + host + "/" + hex.EncodeToString(id))
		strBlk := block.NewStreamedBlock(typ, uri, trans.hasher, conn, size)
		netBlk := &NetBlock{StreamedBlock: strBlk, pool: trans.pool, conn: conn}
		return netBlk, nil
	}

	blk, err := block.New(typ, nil, trans.hasher)
	if err != nil {
		//trans.pool.returnConn(conn)
		conn.Close()
		return nil, err
	}

	wr, _ := blk.Writer()
	_, err = io.CopyN(wr, conn, int64(size))
	if err != nil {
		// Close the connection and throw it away
		conn.Close()
		return nil, err
	}

	trans.pool.returnConn(conn)

	err = wr.Close()

	//
	// TODO:
	//

	return blk, err
}

// SetBlock makes a SetBlock call on the remote host.
func (trans *NetClient) SetBlock(host string, blk block.Block) ([]byte, error) {
	// Get connection
	conn, err := trans.pool.getConn(host)
	if err != nil {
		return nil, err
	}

	// Write request
	//log.Printf("NetClient.SetBlock request op=%d id=%x", reqTypeSet, blk.ID())
	id := blk.ID()
	if err = writeHeaderAndID(conn, Header{reqTypeSet, 0}, id); err != nil {
		conn.Close()
		return nil, err
	}

	// Check if block already exists
	if err = conn.readResponseHeader(); err != nil {
		//log.Println("[INFO] NetClient.SetBlock returning conn", err)
		trans.pool.returnConn(conn)
		return nil, err
	}

	// Write block type and size
	if writeBlockTypeAndSize(conn, blk.Type(), blk.Size()); err != nil {
		conn.Close()
		return nil, err
	}
	//log.Printf("Sent id=%x type=%s size=%d", blk.ID(), blk.Type(), blk.Size())

	// Get reader for the provided block
	rd, err := blk.Reader()
	if err != nil {
		conn.Close()
		return nil, err
	}
	//log.Printf("Copying id=%x size=%d", blk.ID(), blk.Size())

	// Write block data to network connection
	if err = utils.CopyNAndCheck(conn, rd, int64(blk.Size())); err != nil {
		//log.Printf("[ERROR] Failed to set block id=%x error='%v", blk.ID(), err)
		conn.Close()
		return nil, err
	}
	//log.Println("Copied", blk.Size())

	defer trans.pool.returnConn(conn)

	// Response header
	err = conn.readResponseHeader()
	if err != nil {
		return nil, err
	}

	// Confirmation hash id
	cid := make([]byte, trans.blockHashSize)
	_, err = conn.Read(cid)
	if err == nil {
		if bytes.Compare(cid, blk.ID()) != 0 {
			err = fmt.Errorf("id mismatch %x != %x", cid, blk.ID())
		}
	}

	log.Printf("[DEBUG] NetClient.SetBlock id=%x error='%v'", blk.ID(), err)

	return cid, err
}

// RemoveBlock makes a RemoveBlock call to the remote host.
func (trans *NetClient) RemoveBlock(host string, id []byte) error {

	// Get connection
	conn, err := trans.pool.getConn(host)
	if err != nil {
		return err
	}
	defer trans.pool.returnConn(conn)

	// make a request
	if err = writeHeaderAndID(conn, Header{reqTypeRemove, 0}, id); err != nil {
		return err
	}

	// Response header
	return conn.readResponseHeader()
}

func (trans *NetClient) reap() {
	for {
		trans.pool.reap()
		time.Sleep(trans.reapInterval)
	}
}

// Shutdown closes all connections in the pool.  Connections in flight will be closed when
// they are returned to the pool.
func (trans *NetClient) Shutdown() {
	trans.pool.shutdown()
}
