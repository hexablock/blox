package blox

import (
	"fmt"
	"net"

	"github.com/hexablock/blox/block"
)

// Transport is the transport interface to access blocks. This is used to abstract and
// combine local and remote
type Transport interface {
	GetBlock(host string, id []byte) (block.Block, error)
	SetBlock(host string, blk block.Block) ([]byte, error)
	BlockExists(host string, id []byte) (bool, error)
	RemoveBlock(host string, id []byte) error
	Register(store BlockDevice)
	Start(ln *net.TCPListener) error
	Shutdown() error
}

// LocalNetTranport is an interface to interact with blocks locally and remotely.
// It selects between making a remote vs a local call based on the given host
type LocalNetTranport struct {
	host   string
	local  BlockDevice
	remote Transport
}

// NewLocalNetTranport creates a new instance using the provided store and transport.
// It registers the store with the transport.  The transport is started once the
// local BlockDevice has been registered.
func NewLocalNetTranport(host string, remote Transport) *LocalNetTranport {
	lt := &LocalNetTranport{
		host:   host,
		remote: remote,
	}

	return lt
}

// GetBlock calls GetBlock on a local or remote host
func (trans *LocalNetTranport) GetBlock(host string, id []byte) (block.Block, error) {
	if trans.host == host {
		return trans.local.GetBlock(id)
	}
	return trans.remote.GetBlock(host, id)
}

// SetBlock calls a SetBlock on a local or remote host
func (trans *LocalNetTranport) SetBlock(host string, blk block.Block) ([]byte, error) {
	if trans.host == host {
		return trans.local.SetBlock(blk)
	}
	return trans.remote.SetBlock(host, blk)
}

// BlockExists calls BlockExists on a local or remote host
func (trans *LocalNetTranport) BlockExists(host string, id []byte) (bool, error) {
	if trans.host == host {
		return trans.local.BlockExists(id)
	}
	return trans.remote.BlockExists(host, id)
}

// RemoveBlock calls RemoveBlock on a local or remote host
func (trans *LocalNetTranport) RemoveBlock(host string, id []byte) error {
	if trans.host == host {
		return trans.local.RemoveBlock(id)
	}
	return trans.remote.RemoveBlock(host, id)
}

// Register registers the store locally as well as with the network transport
func (trans *LocalNetTranport) Register(local BlockDevice) {
	trans.local = local
	trans.remote.Register(local)
}

// Start starts the the remote transport
func (trans *LocalNetTranport) Start(ln *net.TCPListener) error {
	if trans.host == "" {
		return fmt.Errorf("transport host not set")
	}
	return trans.remote.Start(ln)
}

// Shutdown shuts the remote network transport down
func (trans *LocalNetTranport) Shutdown() error {
	return trans.remote.Shutdown()
}
