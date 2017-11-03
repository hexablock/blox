package block

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

var testFile = "../test-data/Crypto101.pdf"

func hashFile(fp string) ([]byte, error) {
	fsh := sha256.New()
	fh, err := os.Open(testFile)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	if _, err = io.Copy(fsh, fh); err == nil {
		sh := fsh.Sum(nil)
		return sh[:], nil
	}
	return nil, err
}

func hashFileWithType(fp string, typ BlockType) ([]byte, error) {
	fsh := sha256.New()
	fsh.Write([]byte{byte(typ)})
	fh, err := os.Open(testFile)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	if _, err = io.Copy(fsh, fh); err == nil {
		sh := fsh.Sum(nil)
		return sh[:], nil
	}
	return nil, err
}

func TestHashReader(t *testing.T) {
	fh, _ := os.Open(testFile)
	hr := NewHasherReader(sha256.New(), fh)

	_, err := ioutil.ReadAll(hr)
	if err != nil {
		fh.Close()
		t.Fatal(err)
	}
	fh.Close()

	hsh, err := hashFile(testFile)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(hsh, hr.Hash()) {
		t.Fatalf("hash mismatch: want=%x have=%x", hsh, hr.Hash())
	}
	t.Logf("%x", hsh)
}

// func TestNullBlock(t *testing.T) {
// 	fp, _ := filepath.Abs(testFile)
// 	uri := NewURI("file://" + fp)
// 	nb := NewNullBlock(BlockTypeData, uri)
//
// 	rd, _ := nb.Reader()
// 	_, err := ioutil.ReadAll(rd)
// 	if err != nil {
// 		rd.Close()
// 		t.Fatal(err)
// 	}
// 	rd.Close()
//
// 	hsh, err := hashFileWithType(testFile, nb.Type())
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	if !bytes.Equal(hsh, nb.id) {
// 		t.Fatalf("hash mismatch: want=%x have=%x", hsh, nb.id)
// 	}
// }
