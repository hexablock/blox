package block

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/hexablock/hexatype"
)

func TestTreeNode(t *testing.T) {
	tn := NewDirTreeNode("test", []byte("foo"))
	bn := tn.MarshalBinary()

	tn1 := &TreeNode{}
	if err := tn1.UnmarshalBinary(bn); err != nil {
		t.Fatal(err)
	}
	if tn1.Name != tn.Name {
		t.Fatal("name mismatch")
	}

	if bytes.Compare(tn.Address, tn1.Address) != 0 {
		t.Fatal("index mismatch")
	}
	if tn.Mode != tn1.Mode {
		t.Fatal("mode mismatch")
	}
}

func TestTreeBlock(t *testing.T) {
	tt := []*TreeNode{
		{Name: "test1", Address: []byte("foo"), Mode: os.ModePerm | os.ModeDir, Type: BlockTypeData},
		{Name: "test2", Address: []byte("foo"), Mode: os.ModePerm | os.ModeDir, Type: BlockTypeData},
		NewFileTreeNode("test3", []byte("foo")),
		NewDirTreeNode("test4", []byte("foo")),
	}

	hasher := &hexatype.SHA256Hasher{}
	uri := NewURI("memory://")
	tb := NewTreeBlock(uri, hasher)
	tb.AddNodes(tt...)

	b := tb.MarshalBinary()

	tb1 := NewTreeBlock(uri, hasher)
	if err := tb1.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}

	for _, v := range tt {
		n, ok := tb1.nodes[v.Name]
		if !ok {
			t.Fatal("missing node", v.Name)
		}

		if !bytes.Equal(v.Address, n.Address) {
			t.Fatal("index mismatch")
		}

		if v.Mode != n.Mode {
			t.Fatal("mode mismatch")
		}

		if v.Type != n.Type {
			t.Fatal("type mismatch")
		}
	}

	var c int
	tb1.Iter(func(tn *TreeNode) error {
		c++
		return nil
	})
	if c != 4 {
		t.Fatal("wrong number of tree nodes")
	}

	tb2 := NewTreeBlock(uri, hasher)
	wr, err := tb2.Writer()
	if err != nil {
		t.Fatal(err)
	}

	rd, err := tb.Reader()
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(wr, rd)
	if err != nil {
		t.Fatal(err)
	}

	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}
	if err = rd.Close(); err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(tb.ID(), tb2.ID()) != 0 {
		t.Fatalf("id mismatch %x != %x", tb.ID(), tb2.ID())
	}

	th := tb2.Hash()
	if bytes.Compare(tb.ID(), th) != 0 {
		t.Fatalf("mismatch %x %x", th, tb.ID())
	}
	if tb.Size() != tb2.Size() {
		t.Fatal("size mismatch", tb.Size(), tb2.Size())
	}
	if tb.NodeCount() != tb2.NodeCount() {
		t.Fatal("count mismatch", tb.NodeCount(), tb2.NodeCount())
	}
}
