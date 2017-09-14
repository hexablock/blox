package device

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/hexatype"
)

var (
	testdata = []byte("somedata-part-of-the-block")
	testfile string
	testdir  string
)

func TestMain(m *testing.M) {
	var err error
	testdir, err = filepath.Abs("../tmp")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	testfile, err = filepath.Abs("../test-data/Crypto101.pdf")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

type devTester struct {
	df     string
	raw    *FileRawDevice
	dev    *BlockDevice
	hasher hexatype.Hasher
}

func (vt *devTester) cleanup() error {
	return os.RemoveAll(vt.df)
}

func newDevTester() (*devTester, error) {
	df, _ := ioutil.TempDir(testdir, "data")
	vt := &devTester{
		df:     df,
		hasher: &hexatype.SHA256Hasher{},
	}

	rdev, err := NewFileRawDevice(df, vt.hasher)
	if err == nil {
		vt.raw = rdev
		vt.dev = NewBlockDevice(NewInmemJournal(), rdev)
	}

	return vt, err
}

func TestFileRawDevice_SetBlock(t *testing.T) {

	df, _ := ioutil.TempDir(testdir, "data")
	defer os.RemoveAll(df)

	hasher := &hexatype.SHA256Hasher{}
	fbs, err := NewFileRawDevice(df, hasher)
	if err != nil {
		t.Fatal(err)
	}

	fh, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	uri := block.NewURI("file://" + df)
	blk := block.NewDataBlock(uri, hasher)
	//blk := fbs.NewBlock()
	wr, err := blk.Writer()
	if err != nil {
		t.Fatal(err)
	}

	if _, err = io.Copy(wr, fh); err != nil {
		t.Fatal(err)
	}

	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}
	// Confirm its there
	b1, err := fbs.GetBlock(blk.ID())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(b1.ID(), blk.ID()) != 0 {
		t.Fatal("id mismatch")
	}

	if _, err = fbs.SetBlock(blk); err == nil {
		t.Fatal("should fail")
	}

}

func TestFileDataBlockStore(t *testing.T) {
	df, _ := ioutil.TempDir(testdir, "data")
	defer os.RemoveAll(df)

	hasher := &hexatype.SHA256Hasher{}
	fbs, err := NewFileRawDevice(df, hasher)
	if err != nil {
		t.Fatal(err)
	}
	//uri, _ := url.Parse("file://" + df)
	//block.NewDataBlock(uri)
	blk := fbs.NewBlock()
	// Write new block
	wr, err := blk.Writer()
	if err != nil {
		t.Fatal(err)
	}
	_, err = wr.Write(testdata)
	if err != nil {
		t.Fatal(err)
	}
	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}

	h := hasher.New()
	h.Write([]byte{byte(block.BlockTypeData)})
	h.Write(testdata)
	s := h.Sum(nil)
	sh := s[:]

	if bytes.Compare(sh, blk.ID()) != 0 {
		t.Fatal("wrong hash")
	}

	gblk, err := fbs.GetBlock(blk.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gblk.ID(), blk.ID()) {
		t.Fatalf("id mismatch type=%s want=%x have=%x", blk.Type(), blk.ID(), gblk.ID())
	}
	// Check write on disk
	b, err := ioutil.ReadFile(filepath.Join(df, hex.EncodeToString(gblk.ID())))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(testdata)+1 {
		t.Fatal("write length mismatch")
	}

	if bytes.Compare(b[1:], testdata) != 0 {
		t.Fatal("data mismatch")
	}

	blk1 := fbs.NewBlock()
	wr, err = blk1.Writer()
	if err != nil {
		t.Fatal(err)
	}
	_, err = wr.Write(testdata)
	if err != nil {
		t.Fatal(err)
	}

	if err = wr.Close(); err != block.ErrBlockExists {
		t.Fatal("should fail with", block.ErrBlockExists, err)
	}

}
