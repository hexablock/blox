package block

import (
	"bytes"
	"io"

	"github.com/hexablock/hexatype"
)

// NewDataBlock inits a new DataBlock based on the given scheme
func NewDataBlock(uri *URI, hasher hexatype.Hasher) Block {
	if uri != nil {
		switch uri.Scheme {
		case SchemeFile:
			return NewFileDataBlock(uri, hasher)
		}

	}
	// Default to memory block
	return NewMemDataBlock(uri, hasher)
}

type MemDataBlock struct {
	*baseBlock
	data []byte
	rb   *bytes.Buffer
}

// NewDataBlock inits a new DataBlock
func NewMemDataBlock(uri *URI, hasher hexatype.Hasher) *MemDataBlock {
	return &MemDataBlock{
		baseBlock: &baseBlock{uri: uri, typ: BlockTypeData, hasher: hasher}}
}

// Writer initializes a write buffer returning a WriteCloser
func (block *MemDataBlock) Writer() (io.WriteCloser, error) {
	block.hw = NewHasherWriter(block.hasher.New(), bytes.NewBuffer(nil))
	// Write type to hasher. we do not actually write/persist it
	_, err := block.hw.hasher.Write([]byte{byte(block.typ)})
	return block, err
}

// Reader initializes the read buffer with the data returning a ReadCloser
func (block *MemDataBlock) Reader() (io.ReadCloser, error) {
	block.rb = bytes.NewBuffer(block.data)
	return block, nil
}

func (block *MemDataBlock) Write(p []byte) (int, error) {
	return block.hw.Write(p)
}

func (block *MemDataBlock) Read(p []byte) (int, error) {
	return block.rb.Read(p)
}

// Close closes the writer if it is not nil.  For a read close this is simply a no-op
func (block *MemDataBlock) Close() error {
	if block.hw == nil {
		return nil
	}

	buf := block.hw.uw.(*bytes.Buffer)
	block.data = buf.Bytes()

	block.size = uint64(len(block.data))
	block.id = block.hw.Hash()

	block.hw = nil

	return nil
}

// Hash returns the hash id of the block given the hash function
func (block *MemDataBlock) Hash() []byte {
	h := block.hasher.New()
	h.Write([]byte{byte(block.typ)})

	rd, _ := block.Reader()
	io.Copy(h, rd)
	sh := h.Sum(nil)
	rd.Close()

	// Update id cache
	block.id = sh[:]
	return block.id
}
