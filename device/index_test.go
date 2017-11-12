package device

import (
	"bytes"
	"testing"

	"github.com/hexablock/blox/block"
)

func Test_IndexEntry(t *testing.T) {
	ie := &IndexEntry{
		id:   []byte("12345678123456781234567812345678"),
		typ:  block.BlockTypeData,
		size: 15,
		data: []byte("123456789123456"),
	}
	b, err := ie.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	var out IndexEntry
	if err = out.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(out.ID(), ie.ID()) != 0 {
		t.Errorf("id mismatch %x!=%x", out.ID(), ie.ID())
	}
	if out.Type() != ie.Type() {
		t.Error("type mismatch")
	}
	if out.Size() != ie.Size() {
		t.Error("size mismatch")
	}
	if bytes.Compare(out.Data(), ie.Data()) != 0 {
		t.Error("data mismatch")
	}

	var eout IndexEntry
	if err = eout.UnmarshalBinary(b[:40]); err == nil {
		t.Fatal("should fail")
	}
}
