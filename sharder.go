package blox

import (
	"errors"
	"io"
	"sync"

	"github.com/hexablock/blox/block"
)

// Shard is a piece of a given file.  It contains the data, offset in the file
// and index in the file
type shard struct {
	Data   []byte
	Index  uint64
	Offset uint64
	err    error
}

type result struct {
	id   []byte
	idx  uint64
	size uint64
	err  error
}

// StreamSharder takes a stream and shards it by the given block size.  It
// produces an index block of the stream
type StreamSharder struct {
	// Num of go-routines to write blocks
	numRoutines int

	// Index block for the input stream
	idx *block.IndexBlock

	// Block device used to store blocks
	dev BlockDevice
}

// NewStreamSharder creates a new sharder using the block device as storage.
// It launches numRoutines to write blocks in parallel
func NewStreamSharder(dev BlockDevice, numRoutines int) *StreamSharder {
	sh := &StreamSharder{
		dev:         dev,
		numRoutines: numRoutines,
		idx:         block.NewIndexBlock(nil, dev.Hasher()),
	}
	if sh.numRoutines < 1 {
		sh.numRoutines = 1
	}
	return sh
}

// SetBlockSize sets the block size for the sharder.  This should be called
// before Shard is called in order to take affect
func (sh *StreamSharder) SetBlockSize(blockSize uint64) {
	sh.idx.SetBlockSize(blockSize)
}

// Shard starts sharding a given stream.  It returns an IndexBlock or an error
func (sh *StreamSharder) Shard(rd io.ReadCloser) error {

	done := make(chan struct{})
	defer close(done)

	shards, errc := shardReader(done, rd, sh.idx.BlockSize())

	// Start a fixed number of goroutines to read
	c := make(chan result)

	var wg sync.WaitGroup
	wg.Add(sh.numRoutines)

	for i := 0; i < sh.numRoutines; i++ {
		go func() {
			sh.consume(done, shards, c)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(c)
	}()
	// End of pipeline.

	// Read results and update index
	for r := range c {

		if r.err != nil {
			return r.err
		}

		sh.idx.IndexBlock(r.idx, r.id, r.size)

	}

	// Check whether the file shard read failed
	if err := <-errc; err != nil {
		return err
	}

	return nil
}

func (sh *StreamSharder) newBlockFromShard(shrd *shard) (block.Block, error) {

	blk := block.NewDataBlock(nil, sh.dev.Hasher())
	wr, err := blk.Writer()
	if err != nil {
		return nil, err
	}

	// Write the data
	if _, err = wr.Write(shrd.Data); err != nil {
		wr.Close()
		return nil, err
	}

	err = wr.Close()
	return blk, err
}

func (sh *StreamSharder) consume(done <-chan struct{}, shards <-chan shard, c chan<- result) {
	for shrd := range shards {

		rslt := result{
			idx:  shrd.Index,
			size: uint64(len(shrd.Data)),
		}

		blk, err := sh.newBlockFromShard(&shrd)
		if err == nil {

			rslt.id, err = sh.dev.SetBlock(blk)
			if err != nil && err != block.ErrBlockExists {
				rslt.err = err
			}

		} else {
			rslt.err = err
		}

		select {
		case c <- rslt:
		case <-done:
			return
		}

	}
}

// generate shards from a ReadCloser
func shardReader(done <-chan struct{}, rd io.ReadCloser, blockSize uint64) (<-chan shard, <-chan error) {
	chunks := make(chan shard)
	errc := make(chan error, 1)

	go func(fh io.ReadCloser) {
		// Close output channel
		defer close(chunks)
		// Close reader
		defer fh.Close()

		var (
			i   uint64
			buf = make([]byte, blockSize)
			eof bool
		)

		for {

			n, err := io.ReadFull(fh, buf)
			if err != nil {
				if err == io.EOF {
					break
				} else if err == io.ErrUnexpectedEOF {
					eof = true
				} else {
					errc <- err
					return
				}

			}

			sh := shard{Data: make([]byte, n), Index: i, Offset: i * blockSize}
			copy(sh.Data, buf[:n])

			select {
			case chunks <- sh:
			case <-done:
				errc <- errors.New("read cancelled")
				return
			}

			if eof {
				break
			}

			i++
		}

		errc <- nil

	}(rd)

	return chunks, errc
}
