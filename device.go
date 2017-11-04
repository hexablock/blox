package blox

import (
	"hash"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/device"
)

// BlockDevice implements a local block storage interface.  It abstracts the
// index, tree and data block operations.
type BlockDevice interface {
	Hasher() func() hash.Hash
	SetBlock(block.Block) ([]byte, error)
	GetBlock(id []byte) (block.Block, error)
	RemoveBlock(id []byte) error
	BlockExists(id []byte) bool
	Stats() *device.Stats
}
