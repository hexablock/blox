package blox

import (
	"bytes"
	"testing"

	"github.com/hexablock/blox/block"
)

func TestLocalTransport(t *testing.T) {

	ts1, err := newTestServer()
	if err != nil {
		t.Fatal(err)
	}

	host := "127.0.0.1:9999"
	trans := NewLocalTransport(host, ts1.trans)
	trans.Register(ts1.dev)

	blk := block.NewDataBlock(nil, ts1.hasher)
	wr, _ := blk.Writer()
	wr.Write([]byte("some-local-transport-data"))
	wr.Close()

	id, err := trans.SetBlock(ts1.addr(), blk)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(id, blk.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	gbr1, err := trans.GetBlock(host, id)
	if err != nil {
		t.Fatal(err)
	}

	gbl1, err := trans.GetBlock(ts1.addr(), id)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(gbr1.ID(), gbl1.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	blk2 := block.NewDataBlock(nil, ts1.hasher)
	wr, _ = blk2.Writer()
	wr.Write([]byte("second-data-block"))
	wr.Close()

	id, err = trans.SetBlock(host, blk2)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(id, blk2.ID()) != 0 {
		t.Fatalf("id mismatch want=%x have=%x", blk2.ID(), id)
	}

	if _, err = trans.SetBlock(ts1.addr(), blk2); err != block.ErrBlockExists {
		t.Fatalf("should fail with='%v' got='%v'", block.ErrBlockExists, err)
	}

	if _, err = trans.SetBlock(host, blk2); err != block.ErrBlockExists {
		t.Fatalf("should fail with='%v' got='%v'", block.ErrBlockExists, err)
	}

	if err = trans.RemoveBlock(host, blk.ID()); err != nil {
		t.Fatal(err)
	}

	if err = trans.RemoveBlock(ts1.addr(), blk2.ID()); err != nil {
		t.Fatal(err)
	}

}
