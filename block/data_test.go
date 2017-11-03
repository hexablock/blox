package block

import (
	"crypto/sha256"
	"path/filepath"
	"testing"
)

func Test_FileDataBlock(t *testing.T) {
	ap, _ := filepath.Abs(testFile)
	uri := NewURI("file://" + ap)
	h := sha256.New
	blk := NewFileDataBlock(uri, h)
	if blk.uri.Path != ap {
		t.Fatal("invalid path", blk.uri.Path, ap)
	}

	blk, err := LoadFileDataBlock(uri, h)
	if err == nil {
		t.Fatal("should fail parsing id")
	}

}
