package blox

import (
	"hash"
	"io"

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
	BlockExists(id []byte) (bool, error)
	Stats() *device.Stats
}

// Blox is used to read and write data streams to a block device
type Blox struct {
	dev BlockDevice
}

// NewBlox inits a new Blox instance with a block device.
func NewBlox(dev BlockDevice) *Blox {
	return &Blox{dev: dev}
}

// ReadIndex reads the index id and writes the block data to the writer
func (blox *Blox) ReadIndex(id []byte, wr io.Writer, parallel int) error {
	asm := NewAssembler(blox.dev, parallel)
	_, err := asm.SetRoot(id)
	if err == nil {
		err = asm.Assemble(wr)
	}
	return err
}

// WriteIndex reads from the reader and writes to blox storage
func (blox *Blox) WriteIndex(rd io.ReadCloser, parallel int) (idx *block.IndexBlock, err error) {
	sharder := NewStreamSharder(blox.dev, parallel)
	if err = sharder.Shard(rd); err == nil {
		idx = sharder.IndexBlock()
		_, err = blox.dev.SetBlock(idx)
	}

	return
}
