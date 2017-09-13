package blox

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hexablock/blox/block"
)

func TestNetTransport(t *testing.T) {
	ts1, err := newTestServer()
	if err != nil {
		t.Fatal(err)
	}
	ts2, err := newTestServer()
	if err != nil {
		t.Fatal(err)
	}

	defer ts1.cleanup()
	defer ts2.cleanup()

	// Write new block directly from teh raw device
	blk2 := ts2.rdev.NewBlock()
	wr2, err := blk2.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wr2.Write(testData); err != nil {
		t.Fatal(err)
	}
	if err = wr2.Close(); err != nil {
		t.Fatal(err)
	}
	// Write to block device updating journal
	if _, err = ts2.dev.SetBlock(blk2); err != nil {
		t.Fatal(err)
	}

	if blk2.Size() != uint64(len(testData)) {
		t.Fatalf("size mismatch want=%d have=%d", len(testData), blk2.Size())
	}

	h := ts1.hasher.New()
	h.Write([]byte{byte(block.BlockTypeData)})
	h.Write(testData)
	s := h.Sum(nil)
	sh := s[:]

	if bytes.Compare(sh, blk2.ID()) != 0 {
		t.Fatalf("wrong hash want=%x have=%x", sh, blk2.ID())
	}

	//bs := ts2.blox.local.(*storage.BlockStore)
	rb1, err := ts2.rdev.GetBlock(blk2.ID())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(rb1.ID(), blk2.ID()) != 0 {
		t.Fatalf("id mismatch %x != %x", rb1.ID(), blk2.ID())
	}

	rb2, err := ts1.trans.GetBlock(ts2.addr(), blk2.ID())
	if err != nil {
		t.Fatalf("%v: %x", err, blk2.ID())
	}
	id, err := ts1.dev.SetBlock(rb2)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatal("id should not be nil")
	}

	if bytes.Compare(id, blk2.ID()) != 0 {
		t.Fatalf("id mismatch %x != %x", id, blk2.ID())
	}

	gb, err := ts1.dev.GetBlock(id)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(gb.ID(), id) != 0 {
		t.Fatal("id mismatch")
	}

	if gb.Size() != blk2.Size() {
		t.Fatal("size mismatch")
	}

	blk1, err := ts1.trans.GetBlock(ts2.addr(), blk2.ID())
	if err != nil {
		t.Fatalf("%x %v", blk2.ID(), err)
	}

	// if bytes.Compare(blk1.ID(), blk2.ID()) != 0 {
	// 	t.Errorf("id mismatch %x != %x", blk2.ID(), blk1.ID())
	// }

	if blk1.Size() != blk2.Size() {
		t.Fatalf("block sizes don't match %d != %d", blk1.Size(), blk2.Size())
	}

	if blk1.Type() != blk2.Type() {
		t.Fatalf("block types don't match %d != %d", blk1.Type(), blk2.Type())
	}

	//
	// if remoteBlk.Size() != blk1.Size() {
	// 	t.Fatalf("size mismatch: want=%d have=%d", blk1.Size(), remoteBlk.Size())
	// }
	// // check block data
	// rdr, err := transBlk.Reader()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// // if err = rdr.Close(); err != nil {
	// // 	t.Fatal(err)
	// // }
	// d, err := ioutil.ReadAll(rdr)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if uint64(len(d)) != transBlk.Size() {
	// 	t.Fatal("data and size mismatch", d, transBlk.Size())
	// }

	// local to remote
	// if err = ts1.trans.RemoveBlock(ts2.addr(), blk2.ID()); err != nil {
	// 	t.Fatal(err)
	// }

	db := block.NewDataBlock(nil, ts2.hasher)
	wr, _ := db.Writer()
	wr.Write([]byte("test"))
	wr.Close()

	sid, err := ts2.trans.SetBlock(ts1.addr(), db)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(db.ID(), sid) != 0 {
		t.Fatal("id mismatch")
	}

	// _, err = ts2.trans.GetBlock(ts1.addr(), sid)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	//
	// if _, err = ts1.trans.GetBlock(ts2.addr(), make([]byte, ts1.trans.blockHashSize)); err != block.ErrBlockNotFound {
	// 	t.Fatal("should fail with", block.ErrBlockNotFound, err)
	// }

	if err = ts2.trans.RemoveBlock(ts1.addr(), sid); err != nil {
		t.Fatal(err)
	}
	_, err = ts2.trans.GetBlock(ts1.addr(), sid)
	if !strings.Contains(err.Error(), "not found") {
		t.Fatal("should fail with 'not found' got:", err)
	}

	// Check errors
	// _, err = ts2.blox.GetBlock(ts1.addr(), make([]byte, 32))
	// if err != block.ErrBlockNotFound {
	// 	t.Fatal("should fail with", block.ErrBlockNotFound, err)
	// }

	//
	// if err = ts2.blox.RemoveBlock(ts1.addr(), make([]byte, 32)); err == nil {
	// 	t.Fatal("remove should fail")
	// }

	ts1.trans.Shutdown()
	ts2.trans.Shutdown()

	// Test closed transport
	// if _, err = ts1.trans.GetBlock(ts2.addr(), blk2.ID()); err == nil {
	// 	t.Fatal("should fail with Transport closed error")
	// } else if !strings.Contains(err.Error(), "transport is shutdown") {
	// 	t.Fatalf("wrong error.  have='%v'", err)
	// }

}
