package device

import (
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

// Max allowed size of a DataBlock to be stored inline in the journal entry 4KB
const maxJournalDataValSize = 4 * 1024

// RawDevice represents a block storage interface specifically for data blocks. It contains
// no smarts
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
	Get(id []byte) ([]byte, error)
	Exists(id []byte) bool
	Iter(cb func(key []byte, value []byte) error) error
	Set(id, val []byte) error
	Remove(id []byte) (inline bool, err error)
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
	val, err := dev.j.Get(id)
	if err != nil {
		return nil, err
	}

	// Initialize a new in-memory block
	typ := block.BlockType(val[0])
	if blk, err = block.New(typ, nil, dev.hasher); err != nil {
		return
	}
	// At this point we only have the type

	var wr io.WriteCloser

	switch typ {
	case block.BlockTypeData:
		// Only set the size for a data block
		sz := binary.BigEndian.Uint64(val[1:9])
		blk.SetSize(sz)
		// Get the remainder of the data if there is any.  This would be an inline data block.
		// only
		if len(val) > 9 {
			if wr, err = blk.Writer(); err == nil {
				defer wr.Close()
				_, err = wr.Write(val[9:])
			}
		} else {
			blk, err = dev.dev.GetBlock(id)
		}

	case block.BlockTypeIndex, block.BlockTypeTree:
		if wr, err = blk.Writer(); err == nil {
			defer wr.Close()
			_, err = wr.Write(val[1:])
		}

	default:
		err = block.ErrInvalidBlockType
	}

	return
}

// SetBlock stores the block in the volume. For DataBlocks the ID is expected to be
// present.
func (dev *BlockDevice) SetBlock(blk block.Block) (id []byte, err error) {
	typ := blk.Type()
	val := []byte{byte(typ)}
	//log.Printf("[DEBUG] BlockDevice.SetBlock id=%x type=%s", blk.ID(), blk.Type())

	switch typ {
	case block.BlockTypeData:

		sz := make([]byte, 8)
		binary.BigEndian.PutUint64(sz, blk.Size())
		val = append(val, sz...)
		//log.Printf("BlockDevice.SetBlock setting id=%x size=%d", blk.ID(), blk.Size())
		//
		// TODO: Enabling inlining causes the network transport to fail. Investigate
		//

		// if blk.Size() < maxJournalDataValSize {
		// 	// Write data inline to the journal
		// 	var rd io.ReadCloser
		// 	if rd, err = blk.Reader(); err != nil {
		// 		return
		// 	}
		// 	var inline []byte
		// 	if inline, err = ioutil.ReadAll(rd); err != nil {
		// 		return
		// 	}
		//
		// 	if err = rd.Close(); err != nil {
		// 		return
		// 	}
		//
		// 	id = blk.ID()
		// 	val = append(val, inline...)
		//
		// } else {

		id, err = dev.dev.SetBlock(blk)
		if err != nil {
			if err != block.ErrBlockExists {
				return nil, err
			}
			// If we already have the block continue on to update the journal
			//err = nil
		}
		//log.Printf("BlockDevice.SetBlock wrote id=%x type=%s size=%d", blk.ID(), blk.Type(), blk.Size())

	case block.BlockTypeTree:
		// Write the index block directly to the journal without unmarshalling.
		var bd []byte
		if bd, err = blockReadAll(blk); err != nil {
			return
		}
		// Append actual data
		val = append(val, bd...)
		// Calculate the hash for non-data blocks
		id = dev.hash(val)

		log.Printf("[INFO] TreeBlock set id=%x size=%d", id, blk.Size())

	case block.BlockTypeIndex:
		// Write the index block directly to the journal without unmarshalling
		var bd []byte
		if bd, err = blockReadAll(blk); err != nil {
			return
		}
		// Append actual data
		val = append(val, bd...)
		// Calculate the hash for non-data blocks
		id = dev.hash(val)

		log.Printf("[INFO] IndexBlock set id=%x size=%d", id, blk.Size())

	default:
		return nil, block.ErrInvalidBlockType
	}

	// Update the journal as needed
	err = dev.j.Set(id, val)

	return
}

func (dev *BlockDevice) hash(val []byte) []byte {
	h := dev.hasher.New()
	h.Write(val)
	sh := h.Sum(nil)
	return sh[:]
}

// BlockExists returns true if the id exists in the journal
func (dev *BlockDevice) BlockExists(id []byte) bool {
	return dev.j.Exists(id)
}

// RemoveBlock removes a block from the volume as well as journal by the given hash id
func (dev *BlockDevice) RemoveBlock(id []byte) error {
	inline, err := dev.j.Remove(id)
	if err == nil {
		if inline {
			return nil
		}
	} else if err != block.ErrBlockNotFound {
		return err
	}
	//
	// TODO: Defer this to compaction
	//
	// Remove block from device.
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
