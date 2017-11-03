package block

import (
	"encoding/hex"
	"hash"
	"io"
)

const (
	// DefaultBlockSize is 1MB
	DefaultBlockSize uint64 = 1024 * 1024
)

// BlockType holds the type of block
type BlockType byte

const (
	// BlockTypeData is data block. This type of block contains a chunk of arbitrary data.
	// This could be a whole or piece of a file or data
	BlockTypeData BlockType = iota + 1
	// BlockTypeIndex is an index block
	BlockTypeIndex
	// BlockTypeTree defines an tree block containing other data, index, or tree entries
	BlockTypeTree
	// BlockTypeMeta defines a metadata block containing the id of the a tree, index or
	// data block and key-value metadata
	BlockTypeMeta
)

func (blockType BlockType) String() (str string) {
	switch blockType {
	case BlockTypeData:
		str = "data"
	case BlockTypeIndex:
		str = "index"
	case BlockTypeTree:
		str = "tree"
	case BlockTypeMeta:
		str = "meta"
	default:
		str = "0x" + hex.EncodeToString([]byte{byte(blockType)})
	}

	return str
}

// Block represents a block interface.  Blocks may live in-memory, on-disk or remote.
type Block interface {
	// Returns the hash id of the block.  This is the hash of the overall block data.
	ID() []byte
	// Type of block
	Type() BlockType
	// Size of the block data
	Size() uint64
	// Set the size of the block
	SetSize(size uint64)
	// Reader to read data from block
	Reader() (io.ReadCloser, error)
	// Writer to write data to block
	Writer() (io.WriteCloser, error)
	// Returns the hash id of the block given the hash function
	Hash() []byte
	// URI returns the location uri of the block
	URI() *URI
}

// New returns a new Block of the given type. It takes a uri used to determine the source
// of the block and a hasher.  The hasher is only required for TreeBlocks
func New(typ BlockType, uri *URI, hasher func() hash.Hash) (blk Block, err error) {
	switch typ {
	case BlockTypeData:
		blk = NewDataBlock(uri, hasher)
	case BlockTypeIndex:
		blk = NewIndexBlock(uri, hasher)
	case BlockTypeTree:
		blk = NewTreeBlock(uri, hasher)
	default:
		err = ErrInvalidBlockType
	}

	return
}
