package device

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/hexablock/blox/block"
)

func TestBlockDevice(t *testing.T) {
	vt, err := newDevTester()
	if err != nil {
		t.Fatal(err)
	}
	defer vt.cleanup()

	ib := block.NewIndexBlock(nil, vt.hasher)
	ib.SetBlockSize(32)
	ib.Hash()

	blocks := []block.Block{
		ib,
		block.NewTreeBlock(nil, vt.hasher),
	}

	blocks[1].SetSize(512)
	blocks[1].Hash()

	for _, v := range blocks {
		_, err = vt.dev.SetBlock(v)
		if err != nil {
			t.Fatal(err)
		}
	}

	db := block.NewDataBlock(nil, vt.hasher)
	wr, _ := db.Writer()
	wr.Write(testdata)
	wr.Close()

	idb, err := vt.dev.SetBlock(db)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(idb, db.ID()) != 0 {
		t.Fatalf("id mismatch %x !=%x", idb, db.ID())
	}

	dblk := vt.raw.NewBlock()
	wr, _ = dblk.Writer()
	wr.Write([]byte("testdatai have slfdkdjflkj"))
	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = vt.raw.GetBlock(dblk.ID()); err != nil {
		t.Fatal(err)
	}
	// if _, err = vt.dev.SetBlock(dblk); err != block.ErrBlockExists {
	// 	t.Fatalf("should fail with='%v' got='%v'", block.ErrBlockExists, err)
	// }

	// if bytes.Compare(nid, dblk.ID()) != 0 {
	// 	t.Fatalf("id mismatch want=%x have=%x", dblk.ID(), nid)
	// }

	data := block.NewDataBlock(nil, vt.hasher)
	wr, _ = data.Writer()
	wr.Write([]byte("superkalafredgealisticexpielladocious"))
	wr.Close()
	jid, err := vt.dev.SetBlock(data)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(data.ID(), jid) != 0 {
		t.Fatal("id mismatch")
	}

	jblk, err := vt.dev.GetBlock(data.ID())
	if err != nil {
		t.Fatal(err)
	}
	if jblk.Size() != data.Size() {
		t.Fatalf("size mismatch want=%d have=%d", data.Size(), jblk.Size())
	}

	iblk := block.NewIndexBlock(nil, vt.hasher)
	iblk.AddBlock(1, data)
	iblk.Hash()
	iid, err := vt.dev.SetBlock(iblk)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(iblk.ID(), iid) != 0 {
		t.Fatalf("id mismatch want=%x have=%x", iid, iblk.ID())
	}

	idx2, err := vt.dev.GetBlock(iid)
	if err != nil {
		t.Fatal(err)
	}

	if iblk.Size() != idx2.Size() {
		t.Fatalf("size mismatch want=%d have=%d", iblk.Size(), idx2.Size())
	}

	if bytes.Compare(idx2.ID(), iid) != 0 {
		t.Fatalf("id mismatch want=%x have=%x", iid, idx2.ID())
	}

	idxt := idx2.(*block.IndexBlock)
	if idxt.FileSize() != data.Size() {
		t.Fatalf("size mismatch have=%x want=%x", idxt.FileSize(), data.Size())
	}

	if err = vt.dev.RemoveBlock(iid); err != nil {
		t.Fatal(err)
	}

	//
	blk := block.NewDataBlock(nil, vt.hasher)
	wr, _ = blk.Writer()

	pl := make([]byte, maxJournalDataValSize+1)
	rand.Read(pl)
	if _, err = wr.Write(pl); err != nil {
		t.Fatal(err)
	}
	wr.Close()

	id, err := vt.dev.SetBlock(blk)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(id, blk.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	// Tree tests

	nodes := []*block.TreeNode{
		{Name: "index", Address: idx2.ID(), Type: idx2.Type()},
		{Name: "data", Address: blk.ID(), Type: blk.Type()},
	}

	tree := block.NewTreeBlock(nil, vt.hasher)
	tree.AddNodes(nodes...)
	if tree.ID() == nil || len(tree.ID()) == 0 {
		t.Fatal("id should not be empty")
	}

	tid, err := vt.dev.SetBlock(tree)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(tree.ID(), tid) != 0 {
		t.Fatal("id mismatch")
	}

	tblk, err := vt.dev.GetBlock(tid)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(tblk.ID(), tid) != 0 {
		t.Fatal("id mismatch")
	}
	if tree.Size() != tblk.Size() {
		t.Fatal("size mismatch")
	}

	tb := tblk.(*block.TreeBlock)
	if tb.NodeCount() != tree.NodeCount() {
		t.Logf("%#v", tb)
		t.Fatalf("node count mismatch have=%d want=%d", tb.NodeCount(), tree.NodeCount())
	}

}
