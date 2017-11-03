package block

import "hash"

type baseBlock struct {
	// Hash id of the block
	id []byte
	// Type of block 1-byte
	typ BlockType
	// The size of the block.  This is the size of the binary block data
	size uint64
	// This holds a file location, network address or any other uri where
	// the data can actually be accessed.
	uri *URI

	// Hash function to use
	hasher func() hash.Hash

	// Write and hash
	hw *HasherWriter

	// Read and hash
	hr *HasherReader
}

func (block *baseBlock) URI() *URI {
	return block.uri
}

// ID returns the cached hash id of the block.  The hex value of the id is used as the file
// basename
func (block *baseBlock) ID() []byte {
	return block.id
}

// Type returns the type of block.  The value is internally set when the block
// is loaded.
func (block *baseBlock) Type() BlockType {
	return block.typ
}

// Size returns the size of the block data.  The value is internally set when the block is loaded.
// This is strictly the size of the data.
func (block *baseBlock) Size() uint64 {
	return block.size
}

func (block *baseBlock) SetSize(size uint64) {
	block.size = size
}
