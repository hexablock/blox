package device

import (
	"io"
	"io/ioutil"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

// maxJournalDataValSize is the max allowed size of a DataBlock to be stored
// inline in the journal entry 4KB
const maxJournalDataValSize = 4 * 1024

// RawDevice represents a block storage interface specifically for data blocks. It
// contains no smarts
type RawDevice interface {
	// Hasher i.e. hash function for id generation
	Hasher() hexatype.Hasher
	// Upon close write the id
	NewBlock() block.Block
	// Write the block by the id
	SetBlock(block.Block) ([]byte, error)
	// Get a block by id
	GetBlock(id []byte) (block.Block, error)
	// Remove a block by id
	RemoveBlock(id []byte) error
	// Check if a block by the id exists
	Exists(id []byte) bool
	// Closes the device
	Close() error
}

// Journal implements a BlockDevice journal to hold an index containing type and size.
// IndexBlock and TreeBlock are stored only in the journal.  DataBlock is stored in the
// journal if the size is smaller than the allowed size.
type Journal interface {
	Get(id []byte) (*JournalEntry, error)
	Exists(id []byte) bool
	Iter(cb func(*JournalEntry) error) error
	Set(jent *JournalEntry) error
	Remove(id []byte) (*JournalEntry, error)
	Close() error
}

// BlockDevice holds and stores the actual blocks.  It contians an underlying block device
// used primarily to store data blocks.  It maintains an index of all blocks, that includes
// the type and size of the block indexed by its hash id. Index and Tree blocks are stored
// in the index/journal.
type BlockDevice struct {
	// Block journal/index for the underlying RawDevice
	j Journal
	// Actual block store for data blocks
	dev RawDevice
	// hasher
	hasher hexatype.Hasher
}

// NewBlockDevice inits a new BlockDevice with the BlockDevice.
func NewBlockDevice(journal Journal, dev RawDevice) *BlockDevice {
	return &BlockDevice{
		j:      journal,
		dev:    dev,
		hasher: dev.Hasher(),
	}
}

// Hasher returns the underlying hasher used for hash id generation
func (dev *BlockDevice) Hasher() hexatype.Hasher {
	return dev.hasher
}

// GetBlock returns a block from the volume. Index and tree blocks will be returned in
// their entirity while a DataBlock will only contain the type and size.  The Reader
// must be used to access the block contents.
func (dev *BlockDevice) GetBlock(id []byte) (blk block.Block, err error) {
	// Check journal for the block
	jent, err := dev.j.Get(id)
	if err != nil {
		return nil, err
	}

	//fmt.Printf("BlockDevice.Journal.Get type=%s size=%d\n", jent.Type(), jent.size)

	// Initialize a new in-memory block
	if blk, err = block.New(jent.Type(), nil, dev.hasher); err != nil {
		return
	}

	var wr io.WriteCloser

	switch jent.Type() {
	case block.BlockTypeData:
		// Get the remainder of the data if there is any.  This would be an inline data block.
		// only
		if jent.size <= maxJournalDataValSize {
			// Create block from inline journal data.  It does not contain the size.
			if wr, err = blk.Writer(); err == nil {
				defer wr.Close()
				_, err = wr.Write(jent.data)
			}
		} else {
			blk, err = dev.dev.GetBlock(jent.id)
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

// SetBlock stores the block in the volume. For DataBlocks the ID is expected to be
// present.
func (dev *BlockDevice) SetBlock(blk block.Block) ([]byte, error) {

	typ := blk.Type()
	jent := &JournalEntry{id: blk.ID(), size: blk.Size(), typ: typ}
	if jent.id == nil || len(jent.id) == 0 {
		return nil, block.ErrInvalidBlock
	}

	//log.Printf("BlockDevice.SetBlock id=%x type=%s size=%d", jent.id, typ, jent.size)

	switch typ {
	case block.BlockTypeData:
		if jent.size < maxJournalDataValSize {
			bd, err := blockReadAll(blk)
			if err != nil {
				return nil, err
			}
			jent.data = bd
			break
		}

		id, err := dev.dev.SetBlock(blk)
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

	// Update the journal as needed
	err := dev.j.Set(jent)

	log.Printf("[DEBUG] BlockDevice.SetBlock id=%x type=%s size=%d error='%v'", blk.ID(), blk.Type(), blk.Size(), err)

	return jent.id, err
}

// BlockExists returns true if the id exists in the journal
func (dev *BlockDevice) BlockExists(id []byte) bool {
	return dev.j.Exists(id)
}

// RemoveBlock removes a block from the volume as well as journal by the given hash id
func (dev *BlockDevice) RemoveBlock(id []byte) error {
	jent, err := dev.j.Remove(id)
	if err == nil {
		// Inline block
		switch jent.Type() {
		case block.BlockTypeData:
			if jent.size <= maxJournalDataValSize {
				return nil
			}
		case block.BlockTypeIndex, block.BlockTypeTree:
			return nil
		}

	} else if err != block.ErrBlockNotFound {
		return err
	}

	// Remove block from device.
	//
	// TODO: Defer this to compaction
	//

	return dev.dev.RemoveBlock(id)
}

// Close stops all operations on the device and closes it
func (dev *BlockDevice) Close() error {
	return dev.dev.Close()
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
