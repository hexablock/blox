package block

import (
	"encoding/hex"
	"hash"
	"io"
	"sync/atomic"
)

const (
	blockReading int32 = iota + 1
	blockWriting
)

// StreamedBlock is a block backed by an underly reader/writer
type StreamedBlock struct {
	*baseBlock
	// This is used prevent instantiating simultaneous readers and writers on the block
	//  1 = read, 2 = write
	rwr int32
	// underlying reader-writer
	fh io.ReadWriteCloser
	// Read hasher
	hasher func() hash.Hash
}

// NewStreamedBlock initializes a block with a read/writer.  It hashes both on reads as well
// as writes.  It takes a BlockType, hash function for read operations, network connection as
// the reader/writer and the size of the block as parameters.
func NewStreamedBlock(typ BlockType, uri *URI, hasher func() hash.Hash, fh io.ReadWriteCloser, size uint64) *StreamedBlock {
	nb := &StreamedBlock{
		baseBlock: &baseBlock{
			size: size,
			typ:  typ,
			uri:  uri,
		},
		fh:     fh,
		hasher: hasher,
	}

	// Set the block id initially based on provided uri
	if uri.Path != "" {
		path := uri.Path[1:]
		nb.id, _ = hex.DecodeString(path)
	}

	return nb
}

// Reader gets a reader that wraps the underlying network connection in to a block size
// limited reader.
func (block *StreamedBlock) Reader() (io.ReadCloser, error) {
	// Do not allow opening reader if writer is open
	if swapped := atomic.CompareAndSwapInt32(&block.rwr, 0, blockReading); !swapped {
		return nil, errReaderWriterOpen
	}
	// We do not burn the type as it is not expected to be in the underlying stream
	block.hr = NewHasherReader(block.hasher(), block.fh)
	//_, err := block.hr.hasher.Write([]byte{byte(block.typ)})

	return block, nil
}

// Writer returns a write to a remote block.
func (block *StreamedBlock) Writer() (io.WriteCloser, error) {
	// Do not allow opening writer if reader is open
	if swapped := atomic.CompareAndSwapInt32(&block.rwr, 0, blockWriting); !swapped {
		return nil, errReaderWriterOpen
	}

	block.hw = NewHasherWriter(block.hasher(), block.fh)
	// Write BlockType
	err := WriteBlockType(block.hw, block.typ)
	return block, err
}

// Close closes the Reader or Writer depending on which one is open.  It does not close the
// underlying reader
func (block *StreamedBlock) Close() error {

	if swapped := atomic.CompareAndSwapInt32(&block.rwr, blockReading, 0); swapped {
		block.id = block.hr.Hash()
		block.hr = nil
		//block.hasher = nil
	} else if swapped := atomic.CompareAndSwapInt32(&block.rwr, blockWriting, 0); swapped {
		block.id = block.hw.Hash()
		block.hw = nil
	}

	// May need to check this state
	return nil
}

// Read reads until the block size then returns an EOF
func (block *StreamedBlock) Read(b []byte) (int, error) {
	// TODO: this will not work for index blocks.
	r := block.hr.DataSize()
	//log.Printf("[INFO] StreamedBlock.Read read=%d bs=%d", r, block.size)
	if block.hr.DataSize() == block.size {
		return 0, io.EOF
	}

	var (
		err error
		n   int
	)

	// Read only the remaining based on size
	nsz := r + uint64(len(b))
	if nsz > block.size {
		chop := nsz - block.size
		sl := b[:uint64(len(b))-chop]
		n, err = block.hr.Read(sl)
	} else {
		// Read the whole buffer
		n, err = block.hr.Read(b)
	}

	//log.Printf("total=%d curr=%d", block.hr.DataSize(), n)
	return n, err
}

func (block *StreamedBlock) Write(p []byte) (int, error) {
	return block.hw.Write(p)
}

// Hash is a no-op to satisfy the block interface
func (block *StreamedBlock) Hash() []byte {
	return nil
}
