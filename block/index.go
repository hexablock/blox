package block

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
	"io"
	"sync"
)

// IndexBlock is an index of data blocks. It contains an ordered list
// of block ids making up the data set. This is essentially an index of shards
// making up the whole file. It is thread safe
type IndexBlock struct {
	*baseBlock
	// Total file size represented by this index block
	fileSize uint64
	// Block size of each member block
	blockSize uint64
	// Block ids that belong to this block
	mu     sync.RWMutex
	blocks map[uint64][]byte
	// Read buffer used when calling Reader
	rbuf *bytes.Buffer
}

// NewIndexBlock instantiates a new Data index.
func NewIndexBlock(uri *URI, hasher func() hash.Hash) *IndexBlock {
	di := &IndexBlock{
		baseBlock: &baseBlock{hasher: hasher, uri: uri, typ: BlockTypeIndex, size: 16},
		blockSize: DefaultBlockSize,
		blocks:    make(map[uint64][]byte),
	}

	return di
}

// SetFileSize sets the file size for the index the file is representing
func (block *IndexBlock) SetFileSize(size uint64) {
	block.fileSize = size
}

// FileSize returns the file size represented by this block
func (block *IndexBlock) FileSize() uint64 {
	return block.fileSize
}

// SetBlockSize sets the block size for the data blocks in this index.  A re-index is
// required after setting the blocksize
func (block *IndexBlock) SetBlockSize(size uint64) {
	block.blockSize = size
}

// BlockSize returns the block size for the overall dataset.
func (block *IndexBlock) BlockSize() uint64 {
	return block.blockSize
}

// AddBlock adds a block to the IndexBlock at the given index.
func (block *IndexBlock) AddBlock(index uint64, blk Block) {
	id := blk.ID()

	block.mu.Lock()
	// Update the index
	block.blocks[index] = id
	block.fileSize += blk.Size()
	// Update the actual size of this block.
	block.size += uint64(len(id))
	block.mu.Unlock()
}

// Iter iterates over each block id in order.  It sorts based on block posittion
// and issues the callback with the index and id.
func (block *IndexBlock) Iter(f func(index uint64, id []byte) error) error {
	// Get sorted block ids
	block.mu.RLock()
	// bcount := block.size / block.blockSize
	// if (block.size % block.blockSize) != 0 {
	bcount := block.fileSize / block.blockSize
	if (block.fileSize % block.blockSize) != 0 {
		bcount++
	}
	ids := make([][]byte, bcount)
	for i := range block.blocks {
		ids[i-1] = block.blocks[i]
	}

	// Iterate over sorted set
	for i, k := range ids {
		if err := f(uint64(i), k); err != nil {
			return err
		}
	}
	block.mu.RUnlock()

	return nil
}

// Blocks returns sorted block ids by file order.  It assumes there are no
// wholes in the file
func (block *IndexBlock) Blocks() [][]byte {
	// Sort by block index
	block.mu.RLock()
	// bcount := block.size / block.blockSize
	// if (block.size % block.blockSize) != 0 {
	bcount := block.fileSize / block.blockSize
	if (block.fileSize % block.blockSize) != 0 {
		bcount++
	}
	ids := make([][]byte, bcount)
	for i := range block.blocks {
		ids[i-1] = block.blocks[i]
	}
	block.mu.RUnlock()
	return ids
}

// BlockCount returns the number of blocks in the index
func (block *IndexBlock) BlockCount() int {
	block.mu.RLock()
	defer block.mu.RUnlock()
	return len(block.blocks)
}

// Hash computes the hash of the block given the hash function updating the indertal id
// and returns the hash id.
func (block *IndexBlock) Hash() []byte {
	h := block.hasher()
	h.Write(block.MarshalBinary())
	sh := h.Sum(nil)

	// Update internal cache
	block.id = sh[:]
	return block.id
}

// MarshalBinary marshals the IndexBlock into bytes.  It writes the type, size, blocksize,
// and finally the block ids in that order.
func (block *IndexBlock) MarshalBinary() []byte {
	sz := make([]byte, 8)
	binary.BigEndian.PutUint64(sz, block.fileSize)
	bsz := make([]byte, 8)
	binary.BigEndian.PutUint64(bsz, block.blockSize)

	out := append(append([]byte{byte(block.typ)}, sz...), bsz...)

	// Write all the block ids in order
	ids := block.Blocks()
	for _, id := range ids {
		out = append(out, id...)
	}

	return out
}

// UnmarshalBinary takes the byte slice and unmarshals it into an IndexBlock.
func (block *IndexBlock) UnmarshalBinary(b []byte) error {
	if len(b) < 17 {
		return ErrInvalidBlock
	}

	block.typ = BlockType(b[0])
	block.size = uint64(len(b[1:]))
	block.fileSize = binary.BigEndian.Uint64(b[1:9])
	block.blockSize = binary.BigEndian.Uint64(b[9:17])

	// Block count
	var bcount uint64
	if block.fileSize < block.blockSize {
		bcount = 1
	} else {
		bcount = block.fileSize / block.blockSize
		if (block.fileSize % block.blockSize) != 0 {
			bcount++
		}
	}

	// No entries
	if bcount == 0 {
		return nil
	}

	ids := b[17:]
	w := uint64(len(ids)) / bcount

	block.blocks = make(map[uint64][]byte)
	var last uint64
	for i := uint64(1); i <= bcount; i++ {
		p := i * w
		block.blocks[i] = ids[last:p]
		last = p
	}

	return nil
}

// MarshalJSON is custom json marshaller for IndexBlock
func (block *IndexBlock) MarshalJSON() ([]byte, error) {
	t := struct {
		ID         string
		Size       uint64
		FileSize   uint64
		BlockSize  uint64
		BlockCount int
		Blocks     []string
	}{
		ID:         hex.EncodeToString(block.ID()),
		Size:       block.Size(),
		FileSize:   block.FileSize(),
		BlockSize:  block.BlockSize(),
		BlockCount: block.BlockCount(),
		Blocks:     make([]string, len(block.blocks)),
	}

	block.Iter(func(idx uint64, id []byte) error {
		t.Blocks[idx] = hex.EncodeToString(id)
		return nil
	})

	return json.Marshal(t)
}

// Reader returns a ReadCloser to this block.  It contains a byte stream with
// the 8-byte size, 8-byte block size followed by an ordered list of block id's.
func (block *IndexBlock) Reader() (io.ReadCloser, error) {
	b := block.MarshalBinary()
	block.rbuf = bytes.NewBuffer(b[1:])
	return block, nil
}

func (block *IndexBlock) Read(p []byte) (int, error) {
	return block.rbuf.Read(p)
}

// Writer inits a new WriterCloser backed by hasher.  It writes the type and returns
// the WriteCloser
func (block *IndexBlock) Writer() (io.WriteCloser, error) {
	block.hw = NewHasherWriter(block.hasher(), bytes.NewBuffer(nil))
	err := WriteBlockType(block.hw, block.typ)
	return block, err
}

func (block *IndexBlock) Write(p []byte) (int, error) {
	return block.hw.Write(p)
}

// Close closes the reader buffer by setting it to nil
func (block *IndexBlock) Close() error {
	//if block.rbuf != nil {
	block.rbuf = nil
	//}

	if block.hw == nil {
		return nil
	}

	block.id = block.hw.Hash()
	buf := block.hw.uw.(*bytes.Buffer)
	b := buf.Bytes()
	block.hw = nil
	// Perform actual unmarshalling
	return block.UnmarshalBinary(b)
}
