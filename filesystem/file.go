package filesystem

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/utils"
	"github.com/hexablock/log"
)

// BloxFile is a file on the blox file-system
type BloxFile struct {
	*filebase
	// IndexBlock for file read/write operations
	idx *block.IndexBlock
	// TreeBlock for directories
	tree *block.TreeBlock
	// Blocks available to read
	rblk chan block.Block
	// Current open block reader
	rfd io.ReadCloser
	// Bytes read
	rc int64
	// Writer that shards data as it's written
	w *utils.ShardWriter
	// Channel used to signal completion of block generation when writing
	done chan error
	// Capture runtime
	start time.Time
	end   time.Time
}

// setup io and return the effective buffer size checking it is greater than
// equal to the number of workers
func (bf *BloxFile) initIO(bsize int) int {
	bf.done = make(chan error, 1)
	if bf.numWorkers <= 0 {
		bf.numWorkers = 2
	}

	// use numWorkers as channel buffer size if the provided size is less than
	// the number of workers.  This is to ensure efficiency
	if bsize < bf.numWorkers {
		return bf.numWorkers
	}

	return bsize
}

// initialize file for writing
func (bf *BloxFile) initWriter(bufSize int) {
	bsz := bf.initIO(bufSize)
	bf.w = utils.NewShardWriter(bf.idx.BlockSize(), bsz)

	bf.start = time.Now()
	go bf.startWriters()
}

// initialize file for reading
func (bf *BloxFile) initReader(bufSize int) {
	bsz := bf.initIO(bufSize)
	bf.rblk = make(chan block.Block, bsz)

	bf.start = time.Now()
	go bf.fetchBlocks()
}

// Type returns the type of the BloxFile.  This is essentially the type of the root block for
// a file
func (bf *BloxFile) Type() block.BlockType {
	return bf.blk.Type()
}

// Size returns the size of the file.  If returns the FileSize from the IndexBlock or the
// DataBlock size if it is inline. It satisfies the file and file info interfaces
func (bf *BloxFile) Size() int64 {
	if bf.idx == nil {
		return int64(bf.blk.Size())
	}
	// Return file size from index
	return int64(bf.idx.FileSize())
}

// SetBlockSize sets the block size of the file.  It returns an error if the file has
// already been initialized with an existing block. Each call re-inits the underlying
// sharder
func (bf *BloxFile) SetBlockSize(size uint64) error {
	if bf.idx.BlockCount() == 0 {
		bf.idx.SetBlockSize(size)
		// Reset sharder to new block size
		bf.w = utils.NewShardWriter(size, 0)
		return nil
	}
	return fmt.Errorf("cannot reset index block size")
}

// BlockSize returns the block size of the file.  If the root is a single data block then
// the size of the data block is returned
func (bf *BloxFile) BlockSize() uint64 {
	if bf.idx != nil {
		return bf.idx.BlockSize()
	}
	// There is only 1 block ie. a DataBlock.  Use it's size as the block size
	return bf.blk.Size()
}

// Write writes the given data to the underlying sharder
func (bf *BloxFile) Write(p []byte) (int, error) {
	select {
	case err := <-bf.done:
		// Check for errors before performing any writes. These come from the writeBlocks
		// go-routine
		log.Printf("[ERROR] BloxFile.Write error='%v'", err)
		return 0, err
	default:
		return bf.w.Write(p)
	}
}

// fetchBlocks reads blocks from the underlying device and makes them available in the
// channel buffer
func (bf *BloxFile) fetchBlocks() {
	// Assume a DataBlock and handle it.
	if bf.idx == nil {
		bf.rblk <- bf.blk
		close(bf.rblk)
		return
	}

	// Process index block
	err := bf.idx.Iter(func(index uint64, id []byte) error {
		blk, er := bf.dev.GetBlock(id)
		if er == nil {
			bf.rblk <- blk
		}
		return er
	})

	close(bf.rblk)

	// In case of read, send to done channel only on error.
	if err != nil {
		bf.done <- err
	}

}

func (bf *BloxFile) Read(p []byte) (int, error) {

	if bf.rc == bf.Size() {
		return 0, io.EOF
	}

	if bf.rfd == nil {

		select {
		case b := <-bf.rblk:
			rd, err := b.Reader()
			if err != nil {
				return 0, err
			}
			bf.rfd = rd

		case err := <-bf.done:
			return 0, err

		}

	}

	n, err := bf.rfd.Read(p)
	bf.rc += int64(n)

	if err != nil {

		if err == io.EOF {
			bf.rfd.Close()
			bf.rfd = nil
			if bf.rc != bf.Size() {
				err = nil
			}
		}

	}

	return n, err
}

// startWriters start the specified number of block writers each in a its own go-routine
// and waits for completion. An error will result in the termination of call go routines
func (bf *BloxFile) startWriters() {
	log.Printf("[DEBUG] Starting BloxFile workers=%d", bf.numWorkers)

	var wg sync.WaitGroup
	wg.Add(bf.numWorkers)

	done := make(chan error, 1)
	wait := make(chan struct{}, 1)

	for i := 0; i < bf.numWorkers; i++ {
		go func() {

			if err := bf.writeBlocks(); err != nil {
				done <- err
			}

			wg.Done()

		}()

	}

	go func() {
		wg.Wait()
		wait <- struct{}{}
	}()

	var err error
	select {
	case <-wait:
	case err = <-done:
		// CLose the remainder go-routines
		bf.w.Close()
		// Wait for remainder to exit.
		log.Printf("[DEBUG] Waiting for go-routines to bail...")
		<-wait
	}

	bf.done <- err
}

// consume the channel of shards, generating blocks and setting them to the device.  On
// each successful block written, the index is also updated with the newly written id.
func (bf *BloxFile) writeBlocks() error {
	shards := bf.w.Shards()

	for shard := range shards {
		// Generate a new block from the shard
		blk, err := bf.newBlockFromShard(shard)
		if err != nil {
			//log.Printf("[ERROR] Failed to create block index=%d offset=%d error='%v'", shard.Index, shard.Offset, err)
			return err
		}

		// Set the block to the BlockDevice
		_, err = bf.dev.SetBlock(blk)
		// Update the index block also when the actual block exists as the block may be
		// shared.  The index starts at 1 so we add 1
		if err == nil || err == block.ErrBlockExists {
			i := shard.Index + 1
			bf.idx.AddBlock(i, blk)
			//log.Printf("[INFO] New block from shard index=%d id=%x size=%d", i, blk.ID(), blk.Size())
			continue
		}

		//log.Printf("[ERROR] Failed to persist block id=%x index=%d offset=%d error='%v'",
		//	blk.ID(), shard.Index, shard.Offset, err)
		return err
	}

	return nil
}

// Close closes the sharder and waits for all blocks to be consumed.  It then writes out
// the newly created IndexBlock to the device.
func (bf *BloxFile) closeWriter() error {
	err := bf.w.Close()
	if err != nil {
		return err
	}

	// Wait for all blocks to be processed
	if err = <-bf.done; err != nil {
		return err
	}

	idx := bf.idx
	if idx.BlockCount() == 0 {
		return fmt.Errorf("index does not contain blocks")
	}

	// Hash the index to to update the internal hash id after all blocks are written
	idx.Hash()

	// Set the index block
	_, err = bf.dev.SetBlock(idx)
	//log.Printf("[DEBUG] %d block=%x journal=%x", idx.BlockCount(), idx.ID(), id)
	if err == nil {
		bf.mtime = time.Now()
		//log.Printf("[DEBUG] Persisted file id=%x block-size=%d blocks=%d", id, idx.BlockSize(), idx.BlockCount())
	}

	return err
}

func (bf *BloxFile) closeReader() error {
	// Channel should already be closed.  Reset it
	bf.rblk = nil
	// This should be closed
	bf.rfd = nil
	// Reset bytes read
	bf.rc = 0

	return nil
}

// Runtime returns the runtime for a complete file write or read depending on usage.  The value
// will only be valid once Close is called.  If close is never called then the value will be zero
func (bf *BloxFile) Runtime() time.Duration {
	delta := bf.end.Sub(bf.start)
	if delta < 0 {
		return 0
	}
	return delta
}

func (bf *BloxFile) setEndTime() {
	bf.end = time.Now()
}

// Close closes the writer if this is a writeable file otherwise it closes the reader.
func (bf *BloxFile) Close() error {
	// Set the end time after we have closed the handle
	defer bf.setEndTime()

	// Check if writer needs closing
	if bf.w != nil {
		return bf.closeWriter()
	}
	// Close reader
	return bf.closeReader()
}

func (bf *BloxFile) newBlockFromShard(shard *utils.Shard) (block.Block, error) {

	blk := block.NewDataBlock(nil, bf.dev.Hasher())

	wr, err := blk.Writer()
	if err != nil {
		return nil, err
	}

	// Write the data
	if _, err = wr.Write(shard.Data); err != nil {
		wr.Close()
		return nil, err
	}

	err = wr.Close()
	return blk, err
}
