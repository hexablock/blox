package blox

import (
	"github.com/hexablock/blox/block"
	"github.com/hexablock/hexatype"
)

// NetDevice is a network BlockDevice. It allows to make direct block operations on a
// single remote device.
type NetDevice struct {
	// Remote host
	remote string
	// Network client
	client *NetClient
}

// NewNetDevice inits a new network device connected to the provided single remote.  The
// provided options are used to establish the connection
func NewNetDevice(remote string, opts NetClientOptions) *NetDevice {
	nc := NewNetClient(opts)
	return &NetDevice{remote: remote, client: nc}
}

// Hasher returns the hash function generator for hash ids for the device
func (dev *NetDevice) Hasher() hexatype.Hasher {
	return dev.client.hasher
}

// SetBlock writes the block to the device
func (dev *NetDevice) SetBlock(blk block.Block) ([]byte, error) {
	return dev.client.SetBlock(dev.remote, blk)
}

// GetBlock gets a block from the device
func (dev *NetDevice) GetBlock(id []byte) (block.Block, error) {
	return dev.client.GetBlock(dev.remote, id)
}

// RemoveBlock submits a request to remove a block on the device
func (dev *NetDevice) RemoveBlock(id []byte) error {
	return dev.client.RemoveBlock(dev.remote, id)
}

// Close shutdowns the underlying network transport
func (dev *NetDevice) Close() error {
	dev.client.Shutdown()
	return nil
}
