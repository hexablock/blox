package block

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/hexablock/hexatype"
)

func newMemDataBlock(p []byte) (*MemDataBlock, error) {
	uri := NewURI("memory://")
	mem := NewMemDataBlock(uri, &hexatype.SHA256Hasher{})
	wr, err := mem.Writer()
	if err == nil {
		defer wr.Close()
		_, err = wr.Write(p)
	}

	return mem, err
}

func TestIndexBlock(t *testing.T) {
	testdata1 := []byte("1234509876543223456")
	testdata2 := []byte("plokijuhygqakvoekfk")
	testdata3 := []byte("1234509876549823456")

	mem1, _ := newMemDataBlock(testdata1)
	mem2, _ := newMemDataBlock(testdata2)
	mem3, _ := newMemDataBlock(testdata3)

	uri := NewURI("memory://")
	idx := NewIndexBlock(uri, &hexatype.SHA256Hasher{})
	idx.SetBlockSize(19)
	idx.AddBlock(1, mem1)
	idx.AddBlock(2, mem2)
	idx.AddBlock(3, mem3)
	idx.Hash()
	if len(idx.blocks) != 3 {
		t.Fatal("should have 3 blocks")
	}

	if idx.Type() != BlockTypeIndex {
		t.Fatal("should be of type index")
	}

	if idx.FileSize() != uint64(len(testdata3)+len(testdata1)+len(testdata2)) {
		t.Fatal("size mismatch")
	}

	var cnt int
	idx.Iter(func(index uint64, id []byte) error {
		cnt++
		return nil
	})
	if cnt != 3 {
		t.Fatal("should have 3 blocks")
	}

	cnt = 0
	idx.Iter(func(index uint64, id []byte) error {
		cnt++
		return fmt.Errorf("test")
	})
	if cnt != 1 {
		t.Fatal("count should be 1")
	}

	// if idx.BlockSize() != DefaultBlockSize {
	// 	t.Fatal("block size should be default")
	// }

	rd, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	b, err := ioutil.ReadAll(rd)
	if err != nil {
		t.Fatal(err)
	}
	rd.Close()

	if len(b) != 112 {
		t.Fatalf("incomplete data have=%d", len(b))
	}
	if idx.Size() != 112 {
		t.Fatalf("incomplete data have=%d", len(b))
	}

	// Get source reader
	rd, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}

	nidx := NewIndexBlock(nil, &hexatype.SHA256Hasher{})
	wr, err := nidx.Writer()
	if err != nil {
		t.Fatal(err)
	}

	if _, err = io.Copy(nidx, rd); err != nil {
		t.Fatal(err)
	}
	if err = rd.Close(); err != nil {
		t.Fatal(err)
	}
	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}

	if nidx.Size() != idx.Size() {
		t.Fatalf("size mismatch have=%d want=%d", nidx.Size(), idx.Size())
	}
	if nidx.BlockSize() != idx.BlockSize() {
		t.Fatal("block size mismatch")
	}

	if nidx.Type() != idx.Type() {
		t.Fatal("type mismatch")
	}

	if len(nidx.Blocks()) != len(idx.Blocks()) {
		t.Fatal("block count mismatch")
	}

	for k, v := range nidx.blocks {
		d, ok := idx.blocks[k]
		if !ok {
			t.Fatal("id not found")
		}

		if bytes.Compare(d, v) != 0 {
			t.Errorf("block id mismatch %x != %x", d, v)
		}
	}

	if bytes.Compare(nidx.ID(), idx.ID()) != 0 {
		t.Fatalf("id mismatch want=%x have=%x", idx.ID(), nidx.ID())
	}
}
