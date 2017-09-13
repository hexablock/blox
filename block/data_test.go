package block

import (
	"path/filepath"
	"testing"

	"github.com/hexablock/hexatype"
)

func TestFileDataBlock(t *testing.T) {
	ap, _ := filepath.Abs(testFile)
	//uri, _ := url.Parse("file://" + ap)
	uri := NewURI("file://" + ap)
	h := &hexatype.SHA256Hasher{}
	blk := NewFileDataBlock(uri, h)
	if blk.uri.Path != ap {
		t.Fatal("invalid path", blk.uri.Path, ap)
	}

	blk, err := LoadFileDataBlock(uri, h)
	if err == nil {
		t.Fatal("should fail parsing id")
	}

}
