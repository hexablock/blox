package blox

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/device"
	"github.com/hexablock/log"
)

var (
	testData    = []byte("1qazxsw23edcvvfr45tgbnh667ujmki89ollp0")
	testdir     string
	testfile    string
	testfile2   string
	errCheckStr = "should fail with='%v' got='%v'"
)

func TestMain(m *testing.M) {
	log.SetLevel("DEBUG")
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)

	var err error
	testdir, err = filepath.Abs("./tmp")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	testfile, err = filepath.Abs("./test-data/Crypto101.pdf")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	testfile2, _ = filepath.Abs("./test-data/large.iso")

	os.Exit(m.Run())
}

type testDelegate struct{}

func (dlg *testDelegate) BlockSet(blk block.Block) {
	log.Printf("Block set %x %s %d", blk.ID(), blk.Type(), blk.Size())
}
func (dlg *testDelegate) BlockRemove(id []byte) {
	log.Printf("Block removed %x", id)
}

type testServer struct {
	d      string
	rdev   device.RawDevice
	dev    *device.BlockDevice
	hasher func() hash.Hash

	trans *NetTransport
	ln    *net.TCPListener
}

func newTestServer() (*testServer, error) {
	ts := &testServer{hasher: sha256.New}
	var err error

	ts.d, _ = ioutil.TempDir("./tmp", "data")

	rdev, err := device.NewFileRawDevice(ts.d, ts.hasher)
	if err != nil {
		return nil, err
	}
	ts.rdev = rdev
	ts.dev = device.NewBlockDevice(device.NewInmemIndex(), rdev)
	ts.dev.SetDelegate(&testDelegate{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ts.ln = ln.(*net.TCPListener)
	opts := DefaultNetClientOptions(ts.hasher)
	ts.trans = NewNetTransport(opts)
	ts.trans.Register(ts.dev)
	err = ts.trans.Start(ts.ln)

	return ts, err
}

func (ts *testServer) cleanup() {
	os.RemoveAll(ts.d)
}

func (ts *testServer) addr() string {
	return ts.ln.Addr().String()
}
