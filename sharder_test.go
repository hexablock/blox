package blox

import (
	"crypto/sha256"
	"io/ioutil"
	"os"
	"testing"

	"github.com/hexablock/blox/device"
)

func Test_StreamSharder(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("/tmp", "sharder-")
	defer os.RemoveAll(tmpdir)

	j := device.NewInmemIndex()
	d, err := device.NewFileRawDevice(tmpdir, sha256.New)
	if err != nil {
		t.Fatal(err)
	}
	dev := device.NewBlockDevice(j, d)

	fh, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}

	stat, err := fh.Stat()
	if err != nil {
		t.Fatal(err)
	}

	blockSize := uint64(4096)

	fsize := uint64(stat.Size())
	blks := fsize / blockSize
	if (fsize % blockSize) != 0 {
		blks++
	}

	sharder := NewStreamSharder(dev, 3)
	sharder.SetBlockSize(blockSize)

	if err = sharder.Shard(fh); err != nil {
		t.Fatal(err)
	}

	idx := sharder.idx
	if fsize != idx.FileSize() {
		t.Fatalf("%d != %d", idx.FileSize(), fsize)
	}
	if int(blks) != idx.BlockCount() {
		t.Fatalf("block count %d!=%d", blks, idx.BlockCount())
	}

	if _, err = dev.SetBlock(idx); err != nil {
		t.Fatal(err)
	}

	tmpfile, _ := ioutil.TempFile("/tmp", "readfile-")

	asm := NewAssembler(dev, 3)
	if _, err = asm.SetRoot(idx.ID()); err != nil {
		t.Fatal(err)
	}
	if err = asm.Assemble(tmpfile); err != nil {
		t.Fatal(err)
	}
	if err = tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	s1, _ := os.Stat(tmpfile.Name())
	if s1.Size() != int64(fsize) {
		t.Fatal("size mismatch")
	}
	//t.Log(s1.Name())
}
