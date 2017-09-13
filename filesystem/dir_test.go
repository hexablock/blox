package filesystem

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hexablock/blox/block"
)

func TestBloxFS_Dir(t *testing.T) {
	fst, err := newFSTester()
	if err != nil {
		t.Fatal(err)
	}
	// First
	fh1, _ := os.Open(testfile)
	defer fh1.Close()

	f1, err := fst.fs.Create()
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.Copy(f1, fh1)
	if err != nil {
		t.Fatal(err)
	}

	f1.Close()
	t.Logf("%s %x", f1.Name(), f1.blk.ID())
	tn1 := &block.TreeNode{
		Name:    filepath.Base(fh1.Name()),
		Address: f1.blk.ID(), Type: block.BlockTypeIndex}

	// Second
	fh2, _ := os.Open(testfile1)
	defer fh2.Close()

	f2, err := fst.fs.Create()
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.Copy(f2, fh2)
	if err != nil {
		t.Fatal(err)
	}

	f2.Close()
	//t.Logf("%s %x", f2.Name(), f2.blk.ID())
	tn2 := &block.TreeNode{
		Name:    filepath.Base(fh2.Name()),
		Address: f2.blk.ID(), Type: block.BlockTypeIndex}

	tree := block.NewTreeBlock(nil, fst.hasher)
	tree.AddNodes(tn1, tn2)

	id, err := fst.fs.dev.SetBlock(tree)
	if err != nil {
		t.Fatal(err)
	}

	// Check store
	tblk, err := fst.fs.dev.GetBlock(id)
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := tblk.(*block.TreeBlock)
	if !ok {
		t.Fatal("should be a tree block")
	}
	if bytes.Compare(tr.ID(), tree.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	//t.Logf("%d", f2.mode)

	if tr.NodeCount() != 2 {
		t.Fatal("should have 2 nodes")
	}

	if bytes.Compare(tree.ID(), id) != 0 {
		t.Fatal("id mismatch")
	}

	dir, err := fst.fs.Open(id)
	if err != nil {
		t.Fatal(err)
	}
	if dir.blk == nil {
		t.Fatal("blk should not be nil")
	}
	if dir.tree == nil {
		t.Fatal("tree should not be nil")
	}
	if tree.Size() == 0 {
		t.Fatal("tree size should not be 0")
	}
	if tree.Size() != dir.tree.Size() {
		t.Fatal("dir size mismatch")
	}
	//t.Logf("%#v", dir.tree)

	if dir.tree.NodeCount() != 2 {
		t.Fatal("should have 2 nodes", dir.tree.NodeCount())
	}

	names, err := dir.Readdirnames(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatal("should have 2 names")
	}

	fis, err := dir.Readdir(0)
	if err != nil {
		t.Fatal(err)
	}

	if len(fis) != 2 {
		t.Fatal("should have 2 names")
	}
	for _, f := range fis {
		if f.Name() == "" {
			t.Fatal("name should not be empty")
		}
		if _, err := hex.DecodeString(f.Name()); err == nil {
			t.Fatal("name should not be hash id", f.Name())
		}
	}
}
