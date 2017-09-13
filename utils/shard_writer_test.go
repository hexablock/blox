package utils

import (
	"fmt"
	"io"
	"os"
	"testing"
)

var (
	testfile = "../test-data/Crypto101.pdf"
)

func TestChunkedWriter(t *testing.T) {

	fh, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	stat, _ := fh.Stat()
	fsize := stat.Size()

	blockSize := uint64(1024 * 1024)
	// init chunker
	wr := NewShardWriter(blockSize, 0)
	tt := make(chan int64, 1)

	chunks := wr.Shards()
	go func() {
		var total int64
		for chunk := range chunks {
			//l := len(chunk.data)
			total += int64(len(chunk.Data))
			//fmt.Println(chunk.Index)
		}
		tt <- total
	}()

	_, err = io.Copy(wr, fh)
	if err != nil {
		t.Fatal(err)
	}

	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}

	total := <-tt

	if total != fsize {
		t.Fatal("size mismatch", total, fsize)
	}
}

// TODO: Improve
func TestChunkedWriter2(t *testing.T) {
	fh, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// stat, _ := fh.Stat()
	// fsize := stat.Size()
	blockSize := uint64(1024 * 1024)

	// init chunker
	wr := NewShardWriter(blockSize, 0)
	//tt := make(chan int64, 1)
	resp := make(chan error, 1)

	chunks := wr.Shards()
	go func() {
		//var total int64
		for chunk := range chunks {
			//l := len(chunk.data)
			//total += int64(len(chunk.Data))
			//fmt.Println(chunk.idx, l, total)
			if chunk.Offset != (blockSize * chunk.Index) {
				resp <- fmt.Errorf("invalid offset %d %d", chunk.Index, chunk.Offset)
				return
			}
		}
		//tt <- total
		resp <- nil
	}()

	buf := make([]byte, 2*blockSize)
	if _, err = fh.Read(buf); err != nil {
		t.Fatal(err)
	}

	if _, err = wr.Write(buf); err != nil {
		t.Fatal(err)
	}

	if wr.idx != 1 {
		t.Fatal("block count should be 1")
	}

	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}

	if err = <-resp; err != nil {
		t.Fatal(err)
	}
	//total := <-tt

	// if total != int64(2*blockSize) {
	// 	t.Fatal("size mismatch")
	// }

}
