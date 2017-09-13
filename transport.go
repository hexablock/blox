package blox

import (
	"github.com/hexablock/blox/block"
)

// Transport is the transport interface to access blocks. This is used to abstract and
// combine local and remote
type Transport interface {
	GetBlock(host string, id []byte) (block.Block, error)
	SetBlock(host string, blk block.Block) ([]byte, error)
	RemoveBlock(host string, id []byte) error
	Register(store BlockDevice)
	Shutdown() error
}

// LocalTransport is an interface to interact with blocks locally and remotely.  It
// selects between making a remote vs a local call based on the given host
type LocalTransport struct {
	host   string
	local  BlockDevice
	remote Transport
}

// NewLocalTransport creates a new instance using the provided store and transport.  It
// registers the store with the transport.  The transport is started once the local BlockDevice
// has been registered.
func NewLocalTransport(host string, remote Transport) *LocalTransport {
	lt := &LocalTransport{
		host:   host,
		remote: remote,
	}

	return lt
}

// GetBlock calls GetBlock on a local or remote host
func (trans *LocalTransport) GetBlock(host string, id []byte) (block.Block, error) {
	if trans.host == host {
		return trans.local.GetBlock(id)
	}
	return trans.remote.GetBlock(host, id)
}

// SetBlock calls a SetBlock on a local or remote host
func (trans *LocalTransport) SetBlock(host string, blk block.Block) ([]byte, error) {
	if trans.host == host {
		return trans.local.SetBlock(blk)
	}
	return trans.remote.SetBlock(host, blk)
}

// RemoveBlock calls RemoveBlock on a local or remote host
func (trans *LocalTransport) RemoveBlock(host string, id []byte) error {
	if trans.host == host {
		return trans.local.RemoveBlock(id)
	}
	return trans.remote.RemoveBlock(host, id)
}

func (trans *LocalTransport) Register(local BlockDevice) {
	trans.local = local
	// Register store with remote transport to start the transport
	trans.remote.Register(local)
}

func (trans *LocalTransport) Shutdown() error {
	return trans.remote.Shutdown()
}
