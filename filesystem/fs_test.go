package filesystem

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/device"
	"github.com/hexablock/hexatype"
)

var (
	testdata  = []byte("somedata-part-of-the-block")
	testfile  string
	testfile1 string
	testdir   string
	testfile2 string
)

func TestMain(m *testing.M) {
	var err error
	testdir, err = filepath.Abs("../tmp")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	testfile, _ = filepath.Abs("../test-data/Crypto101.pdf")
	testfile1, _ = filepath.Abs("../test-data/tcp.ppt")
	testfile2, _ = filepath.Abs("../test-data/large.iso")

	os.Exit(m.Run())
}

type fsTester struct {
	df     string
	raw    *device.FileRawDevice
	dev    *device.BlockDevice
	fs     *BloxFS
	hasher hexatype.Hasher
}

func (vt *fsTester) cleanup() error {
	return os.RemoveAll(vt.df)
}

func newFSTester() (*fsTester, error) {
	df, _ := ioutil.TempDir(testdir, "data")
	vt := &fsTester{
		df:     df,
		hasher: &hexatype.SHA256Hasher{},
	}
	rdev, err := device.NewFileRawDevice(df, vt.hasher)
	if err == nil {
		vt.raw = rdev
		vt.dev = device.NewBlockDevice(rdev)
		vt.fs = NewBloxFS(vt.dev)
	}

	return vt, err
}

func TestBloxFS(t *testing.T) {
	vt, err := newFSTester()
	if err != nil {
		t.Fatal(err)
	}
	defer vt.cleanup()

	bfs := NewBloxFS(vt.dev)
	if bfs.Name() != "blox" {
		t.Fatal("wrong fs name")
	}

	db := block.NewDataBlock(nil, bfs.hasher)
	wr, _ := db.Writer()
	wr.Write(testdata)
	wr.Close()

	idb, err := vt.dev.SetBlock(db)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(idb, db.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	bf, err := bfs.Open(idb)
	if err != nil {
		t.Fatal(err)
	}

	if bf.idx != nil {
		t.Fatal("index should be nil")
	}

	if bf.blk == nil {
		t.Fatal("block should not be nil")
	}

	if bf.blk.Size() != db.Size() {
		t.Fatal("size mismatch")
	}
	// Stat
	stat, err := bfs.Stat(db.ID())
	if err != nil {
		t.Fatal(err)
	}

	if stat.Size() != int64(db.Size()) {
		t.Fatal("size mismatch")
	}
	if stat.IsDir() {
		t.Fatal("should not be a dir")
	}

	// bf := stat.(*BloxFile)
	// b := bf.Sys()
	// if _, ok := b.(block.Block); !ok {
	// 	t.Fatal("should be of type block.Block")
	// }

	// Remove
	if err = bfs.Remove(idb); err != nil {
		t.Fatal(err)
	}

	f1, err := bfs.Create()
	if err != nil {
		t.Fatal(err)
	}
	if f1.idx == nil {
		t.Fatal("index should not be nil")
	}

	f1.SetBlockSize(1024)
	f1.idx.AddBlock(1, db)
	if err = f1.SetBlockSize(2048); err == nil {
		t.Fatal("should fail")
	}
	if f1.idx.BlockSize() != 1024 {
		t.Fatal("block size should 1024")
	}

}
