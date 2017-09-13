package filesystem

import (
	"os"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/hexatype"
)

const defaulBlockBufSize = 8

// BlockDevice implements an interface to abstract underlying device ops.  The filesystem makes
// requests to the device to layout the filesystem
type BlockDevice interface {
	Hasher() hexatype.Hasher
	SetBlock(block.Block) ([]byte, error)
	GetBlock(id []byte) (block.Block, error)
	RemoveBlock(id []byte) error
	Close() error
}

// BloxFS provides a file-system type interface to the content-addressable store.  This
// interface only supports interactions using the hash id.
type BloxFS struct {
	dev    BlockDevice
	hasher hexatype.Hasher
}

// NewBloxFS inits a new Blox file-system
func NewBloxFS(dev BlockDevice) *BloxFS {
	return &BloxFS{dev: dev, hasher: dev.Hasher()}
}

// Name returns the name of the filesystem
func (fs *BloxFS) Name() string {
	return "blox"
}

// Create creates a new BloxFile. If successful, methods on the returned file can be used
// for writing.  The name of the file is only available after a call to Close which writes
// the hash id.
func (fs *BloxFS) Create() (*BloxFile, error) {
	idx := block.NewIndexBlock(nil, fs.hasher)
	fb := &filebase{dev: fs.dev, blk: idx}

	bf := &BloxFile{filebase: fb, idx: idx}
	bf.initWriter(defaulBlockBufSize)

	return bf, nil
}

// Open opens the hash id for reading. If successful, methods on the returned file can be
// used for reading.  It initializes the file based on the block type associated to id.
// It returns en error if the block type is invalid
func (fs *BloxFS) Open(sh []byte) (*BloxFile, error) {
	// Load BloxFile from the hash
	bf, err := bloxFileFromHash(fs.dev, sh)
	if err == nil {
		// Init for reading
		bf.initReader(defaulBlockBufSize)
	}
	return bf, err
}

// Remove removes a given block by id.  It does not remove the underlying blocks in the
// case of IndexBlock
func (fs *BloxFS) Remove(id []byte) error {
	return fs.dev.RemoveBlock(id)
}

// Stat stats the given hash id returning FileInfo regarding it.
func (fs *BloxFS) Stat(id []byte) (os.FileInfo, error) {
	return bloxFileFromHash(fs.dev, id)
}

// Shutdown shuts the filesystem down performing all necessary cleanup
func (fs *BloxFS) Shutdown() error {
	return fs.dev.Close()
}

// retrieve the block from the device and create a BloxFile
func bloxFileFromHash(dev BlockDevice, sh []byte) (*BloxFile, error) {

	blk, err := dev.GetBlock(sh)
	if err != nil {
		return nil, err
	}

	fb := &filebase{dev: dev, blk: blk}
	bf := &BloxFile{filebase: fb}

	switch blk.Type() {
	case block.BlockTypeData:

	case block.BlockTypeIndex:
		bf.idx = blk.(*block.IndexBlock)

	case block.BlockTypeTree:
		bf.tree = blk.(*block.TreeBlock)

	default:
		return nil, block.ErrInvalidBlockType
	}
	return bf, nil
}
