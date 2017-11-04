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

	fsize := uint64(stat.Size())
	blks := fsize / 4096
	blks++

	sharder := NewStreamSharder(dev, 3)
	sharder.SetBlockSize(4096)

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
}
