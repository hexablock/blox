package device

import (
	"hash"
	"io"
	"io/ioutil"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/log"
)

// maxIndexDataValSize is the max allowed size of a DataBlock to be stored
// inline in the journal entry 4KB
const maxIndexDataValSize = 4 * 1024

// Delegate implements a block device Delegate for write operations
type Delegate interface {
	// Called when a block is successfully set ie. created
	BlockSet(idx IndexEntry)

	// Called when a block is successfully removed
	BlockRemove(id []byte)
}

// RawDevice represents a block storage interface specifically for data blocks. It
// contains no smarts
type RawDevice interface {
	// Hasher i.e. hash function for id generation
	Hasher() func() hash.Hash

	// New block.  Upon close write the id
	NewBlock() block.Block

	// Write the block by the id
	SetBlock(block.Block) ([]byte, error)

	// Get a block by id
	GetBlock(id []byte) (block.Block, error)

	// Remove a block by id
	RemoveBlock(id []byte) error

	// Check if a block by the id exists
	Exists(id []byte) bool

	// IterIDs
	IterIDs(f func(id []byte) error) error

	// Count returns the total number of block on the device
	Count() int

	// Closes the device
	Close() error
}

// BlockIndex implements a BlockDevice index containing type and size.
// IndexBlock and TreeBlock are stored in their entirity only in the index.
// DataBlock is stored in the index if the size is smaller than
// maxIndexDataValSize.
type BlockIndex interface {
	Get(id []byte) (*IndexEntry, error)
	Exists(id []byte) bool

	// Iterate over all index entries in the store
	Iter(cb func(*IndexEntry) error) error

	// Set an block index entry to the index store
	Set(idx *IndexEntry) error
	Remove(id []byte) (*IndexEntry, error)
	Close() error

	// Stats returns statistics.  Also contains raw device stats
	Stats() *Stats
}

// Stats contains information regarding blocks for a given device
type Stats struct {
	DataBlocks   int
	IndexBlocks  int
	TreeBlocks   int
	MetaBlocks   int
	TotalBlocks  int
	BlocksOnDisk int
	UsedBytes    uint64
}

// BlockDevice holds and stores the actual blocks.  It contians an underlying block device
// used primarily to store data blocks.  It maintains an index of all blocks, that includes
// the type and size of the block indexed by its hash id. Index and Tree blocks are stored
// in the index/journal.
type BlockDevice struct {
	// Block index for the underlying RawDevice
	idx BlockIndex

	// Actual block store for data blocks
	raw RawDevice

	// Called at various phases based on action.  This is user supplied to take
	// custom actions
	delegate Delegate
}

// NewBlockDevice inits a new BlockDevice with the underlying raw BlockDevice.
func NewBlockDevice(idx BlockIndex, dev RawDevice) *BlockDevice {
	return &BlockDevice{
		idx: idx,
		raw: dev,
	}
}

// SetDelegate set the block device delegate.  It should be set before the
// device is used as it is not thread-safe
func (dev *BlockDevice) SetDelegate(delegate Delegate) {
	dev.delegate = delegate
}

// Reindex scans the raw device and adds indexes for earch block not found in
// the index
func (dev *BlockDevice) Reindex() {
	var i int
	dev.raw.IterIDs(func(id []byte) error {
		if !dev.idx.Exists(id) {

			blk, err := dev.raw.GetBlock(id)
			if err == nil {
				jent := &IndexEntry{id: blk.ID(), size: blk.Size(), typ: blk.Type()}
				err = dev.idx.Set(jent)
			}

			if err != nil {
				log.Println("[ERROR] Failed sync block index:", err)
			} else {
				i++
			}

		}

		return nil
	})

	log.Printf("[INFO] BlockDevice index synced blocks=%d", i)
}

// Hasher returns the underlying hasher used for hash id generation
func (dev *BlockDevice) Hasher() func() hash.Hash {
	return dev.raw.Hasher()
}

// GetBlock returns a block from the volume. Index and tree blocks will be returned in
// their entirity while a DataBlock will only contain the type and size.  The Reader
// must be used to access the block contents.
func (dev *BlockDevice) GetBlock(id []byte) (blk block.Block, err error) {
	// Check journal for the block
	jent, err := dev.idx.Get(id)
	if err != nil {
		return nil, err
	}

	// Initialize a new in-memory block
	if blk, err = block.New(jent.Type(), nil, dev.raw.Hasher()); err != nil {
		return
	}

	var wr io.WriteCloser

	switch jent.Type() {
	case block.BlockTypeData:
		// Get the remainder of the data if there is any.  This would be an inline data block.
		// only
		if jent.size < maxIndexDataValSize {
			// Create block from inline journal data.  It does not contain the size.
			if wr, err = blk.Writer(); err == nil {
				defer wr.Close()
				_, err = wr.Write(jent.data)
			}
		} else {
			blk, err = dev.raw.GetBlock(jent.id)
		}

	case block.BlockTypeIndex, block.BlockTypeTree:
		if wr, err = blk.Writer(); err == nil {
			defer wr.Close()
			_, err = wr.Write(jent.data)
		}

	default:
		err = block.ErrInvalidBlockType
	}

	return
}

// BlockExists returns true if the id exists in the journal
func (dev *BlockDevice) BlockExists(id []byte) (bool, error) {
	return dev.idx.Exists(id), nil
}

// SetBlock stores the block in the volume. For DataBlocks the ID is expected to be
// present.
func (dev *BlockDevice) SetBlock(blk block.Block) ([]byte, error) {

	typ := blk.Type()
	jent := &IndexEntry{id: blk.ID(), size: blk.Size(), typ: typ}
	if jent.id == nil || len(jent.id) == 0 {
		return nil, block.ErrInvalidBlock
	}

	//log.Printf("BlockDevice.SetBlock id=%x type=%s size=%d", jent.id, typ, jent.size)

	switch typ {
	case block.BlockTypeData:
		if jent.size < maxIndexDataValSize {
			bd, err := blockReadAll(blk)
			if err != nil {
				return nil, err
			}
			jent.data = bd
			break
		}

		id, err := dev.raw.SetBlock(blk)
		if err != nil && err != block.ErrBlockExists {
			return nil, err
		}
		// use the device returned id
		jent.id = id

	case block.BlockTypeTree:
		bd, err := blockReadAll(blk)
		if err != nil {
			return nil, err
		}
		jent.data = bd

	case block.BlockTypeIndex:
		bd, err := blockReadAll(blk)
		if err != nil {
			return nil, err
		}
		jent.data = bd

	default:
		return nil, block.ErrInvalidBlockType
	}

	// Update the index as needed
	err := dev.setIndex(jent)

	log.Printf("[DEBUG] BlockDevice.SetBlock id=%x type=%s size=%d error='%v'",
		blk.ID(), blk.Type(), blk.Size(), err)

	return jent.id, err
}

// set/update the block index and call the delegate on success
func (dev *BlockDevice) setIndex(jent *IndexEntry) error {
	err := dev.idx.Set(jent)
	if err == nil && dev.delegate != nil {
		// Call delegate
		dev.delegate.BlockSet(*jent)
	}
	return err
}

// RemoveBlock removes a block from the volume as well as journal by the given hash id
func (dev *BlockDevice) RemoveBlock(id []byte) error {
	jent, err := dev.idx.Remove(id)
	if err == nil {
		// Inline block
		switch jent.Type() {
		case block.BlockTypeData:
			if jent.size <= maxIndexDataValSize {
				if dev.delegate != nil {
					// Call delegate
					dev.delegate.BlockRemove(id)
				}
				return nil
			}

		case block.BlockTypeIndex, block.BlockTypeTree:
			if dev.delegate != nil {
				// Call delegate
				dev.delegate.BlockRemove(id)
			}
			return nil
		}

	} else if err != block.ErrBlockNotFound {
		return err
	}

	// Remove block from device.
	//
	// TODO: Defer this to compaction
	//

	if err = dev.raw.RemoveBlock(id); err == nil && dev.delegate != nil {
		// Call delegate
		dev.delegate.BlockRemove(id)
	}

	return err
}

// Close stops all operations on the device and closes it
func (dev *BlockDevice) Close() error {
	return dev.raw.Close()
}

// Stats returns stats about the device
func (dev *BlockDevice) Stats() *Stats {
	stats := dev.idx.Stats()
	stats.BlocksOnDisk = dev.raw.Count()

	return stats
}

func blockReadAll(blk block.Block) ([]byte, error) {
	rd, err := blk.Reader()
	if err != nil {
		return nil, err
	}

	bd, err := ioutil.ReadAll(rd)
	rd.Close()

	return bd, err
}
