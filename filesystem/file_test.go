package filesystem

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/hexablock/blox/block"
)

func TestBloxFS_write_large_file(t *testing.T) {
	stat, _ := os.Stat(testfile2)
	bs := uint64(64 * 1024 * 1024)
	bcnt := uint64(stat.Size()) / bs

	if (uint64(stat.Size()) % bcnt) != 0 {
		bcnt++
	}

	vt, err := newFSTester()
	if err != nil {
		t.Fatal(err)
	}
	defer vt.cleanup()

	fs := NewBloxFS(vt.dev)

	bfh, err := fs.Create()
	if err != nil {
		t.Fatal(err)
	}
	// must be the first call in order override default
	bfh.SetBlockSize(bs)

	fh, err := os.Open(testfile2)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	_, err = io.Copy(bfh, fh)
	if err != nil {
		t.Fatal(err)
	}
	if err = bfh.Close(); err != nil {
		t.Fatal(err)
	}

	if bfh.idx.BlockCount() != int(bcnt) {
		t.Fatal("block count wrong")
	}

	if bfh.Size() != stat.Size() {
		t.Fatal("size mismatch")
	}

	if _, ok := bfh.Sys().(*block.IndexBlock); !ok {
		t.Fatal("should be an index block")
	}
	t.Logf("Write time %v", bfh.Runtime())
}

func TestBloxFile_write_file(t *testing.T) {
	vt, err := newFSTester()
	if err != nil {
		t.Fatal(err)
	}
	defer vt.cleanup()

	fs := NewBloxFS(vt.dev)

	bfh, err := fs.Create()
	if err != nil {
		t.Fatal(err)
	}
	// must be the first call in order override default
	bs := uint64(1024 * 1024)
	bfh.SetBlockSize(bs)

	fh, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	_, err = io.Copy(bfh, fh)
	if err != nil {
		t.Fatal(err)
	}
	if err = bfh.Close(); err != nil {
		t.Fatal(err)
	}

	if bfh.idx.BlockCount() != 16 {
		t.Fatal("should have written 16 blocks", bfh.idx.BlockCount())
	}

	// Check index for block ids
	blks := bfh.idx.Blocks()
	var missing int
	for _, v := range blks {
		if _, err = bfh.dev.GetBlock(v); err != nil {
			t.Errorf("block missing %x", v)
			missing++
		}
	}

	if missing > 0 {
		t.Errorf("index missing entries %d", missing)
	}

	var c int
	err = bfh.idx.Iter(func(index uint64, id []byte) error {
		c++
		if id == nil {
			return fmt.Errorf("nil id")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if c != 16 {
		t.Fatal("should have 16 blocks got", c)
	}

	// Check index.
	idxBlk, err := vt.dev.GetBlock(bfh.idx.ID())
	if err != nil {
		t.Fatalf("index block missing %x", bfh.idx.ID())
	}

	idx := idxBlk.(*block.IndexBlock)
	if idx.FileSize() != uint64(bfh.Size()) {
		t.Fatal("size mismatch", idxBlk.Size(), bfh.Size())
	}

	if bytes.Compare(idxBlk.ID(), bfh.idx.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	ib1 := idxBlk.(*block.IndexBlock)
	if ib1.BlockCount() != bfh.idx.BlockCount() {
		t.Fatal("block count mismatch", ib1.BlockCount(), bfh.idx.BlockCount())
	}

	//
	// Check if we can read the file from the filesystem
	//
	tfile, err := ioutil.TempFile(vt.df, "outfile")
	if err != nil {
		t.Fatal(err)
	}
	defer tfile.Close()

	rbf, err := fs.Open(bfh.idx.ID())
	if err != nil {
		t.Fatal(err)
	}

	if rbf.idx == nil {
		t.Fatal("failed to init index")
	}

	if rbf.idx.Size() != bfh.idx.Size() {
		t.Fatal("size mismatch")
	}

	if rbf.idx.BlockCount() != 16 {
		t.Fatal("should have 16 blocks got", rbf.idx.BlockCount())
	}

	n, err := io.Copy(tfile, rbf)
	if err != nil {
		t.Fatal(err)
	}
	if err = rbf.Close(); err != nil {
		t.Fatal(err)
	}

	stat, err := tfile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	sz := stat.Size()
	if n != sz {
		t.Fatal("size mismatch", n, sz)
	}

	if sz == 0 {
		t.Fatal("output file size should not be 0")
	}

	if rbf.Name() == "" {
		t.Fatal("name should not be empty")
	}

	t.Logf("Write time %v", bfh.Runtime())
}
