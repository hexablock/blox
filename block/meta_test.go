package block

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func Test_MetaBlock(t *testing.T) {
	hasher := sha256.New
	mb := NewMetaBlock(nil, hasher)
	m := map[string]string{
		"foo": "bar",
		"bas": "biz",
	}

	mb.SetMetadata(m)
	b := mb.MarshalBinary()
	mb1 := NewMetaBlock(nil, hasher)
	if err := mb1.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}

	if len(mb.m) != len(mb1.m) {
		t.Fatalf("%d != %d", len(mb.m), len(mb1.m))
	}

	if bytes.Compare(mb.ID(), mb1.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	for k, v := range mb.m {
		val, ok := mb1.m[k]
		if !ok {
			t.Fatal("key not found", k)
		}
		if v != val {
			t.Fatalf("%s != %s", v, val)
		}
	}

}

// NullBlock is used calculate the hash of a stream of bytes via reading or writing to the
// block
type NullBlock struct {
	*baseBlock
	fh io.ReadWriteCloser
}

// NewNullBlock instantiates a new block of type with the supplied file uri.
func NewNullBlock(typ BlockType, uri *URI, hasher func() hash.Hash) *NullBlock {
	return &NullBlock{baseBlock: &baseBlock{typ: typ, uri: uri, hasher: hasher}}
}

// Writer returns a WriteCloser that does nothing but hash the data being written
func (block *NullBlock) Writer() (io.WriteCloser, error) {
	// Hasher to hash and discard the data
	block.hw = NewHasherWriter(block.hasher(), ioutil.Discard)
	err := WriteBlockType(block.hw, block.Type())
	return block, err
}

// Reader returns a ReadCloser with an underlying hasher.  It simply reads the
// the file passing it through the hashing function.
func (block *NullBlock) Reader() (io.ReadCloser, error) {
	var err error
	if block.fh, err = os.Open(block.uri.Path); err != nil {
		return nil, err
	}

	block.hr = NewHasherReader(block.hasher(), block.fh)
	block.hr.hasher.Write([]byte{byte(block.Type())})

	return block, nil
}

func (block *NullBlock) Read(p []byte) (int, error) {
	return block.hr.Read(p)
}

func (block *NullBlock) Write(p []byte) (int, error) {
	return block.hw.Write(p)
}

// Close close the underlying handle and sets the id and size of the block.
func (block *NullBlock) Close() (err error) {

	err = block.fh.Close()

	if block.hr != nil {
		block.id = block.hr.Hash()
		block.size = block.hr.DataSize()
	} else if block.hw != nil {
		block.id = block.hw.Hash()
		block.size = block.hw.DataSize()
	}

	return err
}
