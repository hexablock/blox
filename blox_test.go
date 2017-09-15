package blox

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/hexablock/blox/device"
	"github.com/hexablock/hexatype"
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

type testServer struct {
	d      string
	rdev   device.RawDevice
	dev    *device.BlockDevice
	hasher hexatype.Hasher

	trans *NetTransport
	ln    net.Listener
}

func newTestServer() (*testServer, error) {
	ts := &testServer{hasher: &hexatype.SHA256Hasher{}}
	var err error

	ts.d, _ = ioutil.TempDir("./tmp", "data")

	rdev, err := device.NewFileRawDevice(ts.d, ts.hasher)
	if err != nil {
		return nil, err
	}
	ts.rdev = rdev
	ts.dev = device.NewBlockDevice(device.NewInmemJournal(), rdev)

	ts.ln, _ = net.Listen("tcp", "127.0.0.1:0")
	opts := DefaultNetClientOptions(ts.hasher)
	ts.trans = NewNetTransport(ts.ln, opts)
	ts.trans.Register(ts.dev)

	return ts, nil
}

func (ts *testServer) cleanup() {
	os.RemoveAll(ts.d)
}

func (ts *testServer) addr() string {
	return ts.ln.Addr().String()
}
