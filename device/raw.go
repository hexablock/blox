package device

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/utils"
	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

// DefaultFilePerms are the default file permissions used when creating files on disk.
const DefaultFilePerms = 0444

// FileRawDevice implements a file based block device.  Blocks are stored in files
// 1 file per block in the data dir.
type FileRawDevice struct {
	datadir        string
	defaultSetPerm os.FileMode
	hasher         hexatype.Hasher
}

// NewFileRawDevice instantiates a new FileRawDevice setting the defaults permissions, flush interval
// provided data directory.
func NewFileRawDevice(datadir string, hasher hexatype.Hasher) (*FileRawDevice, error) {
	dabs, err := filepath.Abs(datadir)
	if err == nil {

		var stat os.FileInfo
		if stat, err = os.Stat(dabs); err == nil {
			if !stat.IsDir() {
				return nil, errors.New("path must be a directory")
			}
		}

		return &FileRawDevice{dabs, DefaultFilePerms, hasher}, nil
	}

	return nil, err
}

// Hasher returns the underlying hash function generator used to generate hash id
func (st *FileRawDevice) Hasher() hexatype.Hasher {
	return st.hasher
}

// returns the absolute path to the given block from the data directory
func (st *FileRawDevice) abspath(id []byte) string {
	return filepath.Join(st.datadir, hex.EncodeToString(id))
}

// NewBlock returns a new Block backed by the store.  It initially sets the block uri to the
// data directory for the store. On write closing it updates with the path including the
// hash id
func (st *FileRawDevice) NewBlock() block.Block {
	uri := block.NewURI("file://" + st.datadir)
	return block.NewFileDataBlock(uri, st.hasher)
}

// RemoveBlock removes a Block from the in-mem buffer as well as stable store.
func (st *FileRawDevice) RemoveBlock(id []byte) error {
	p := st.abspath(id)
	return os.Remove(p)
}

// GetBlock returns a block with the given id if it exists.  It loads the type
// and size from the file then closes the file.
func (st *FileRawDevice) GetBlock(id []byte) (block.Block, error) {
	ap := st.abspath(id)
	uri := block.NewURI("file://" + ap)
	return block.LoadFileDataBlock(uri, st.hasher)
}

// Exists stats the block file and returns whether it exists
func (st *FileRawDevice) Exists(id []byte) bool {
	ap := st.abspath(id)
	if _, err := os.Stat(ap); err == nil {
		return true
	}
	return false
}

// SetBlock writes the block to the store. It gets a reader from the provided
// Block, instantiates a new Block in the store and copies the data to the new
// Block.  It returns the id i.e. hash of the newly written block.  It returns a
// ErrBlockExists if the block exists along with the id.
func (st *FileRawDevice) SetBlock(blk block.Block) ([]byte, error) {
	if st.Exists(blk.ID()) {
		return blk.ID(), block.ErrBlockExists
	}
	// Get source block reader
	src, err := blk.Reader()
	if err != nil {
		log.Printf("[ERROR] FileRawDevice.SetBlock id=%x error='%v'", blk.ID(), err)
		return nil, err
	}

	// New Block
	uri := block.NewURI("file://" + st.datadir)
	dstBlk := block.NewFileDataBlock(uri, st.hasher)
	// Get dest. writer
	dst, err := dstBlk.Writer()
	if err != nil {
		src.Close()
		log.Printf("[ERROR] FileRawDevice.SetBlock id=%x error='%v'", blk.ID(), err)
		return nil, err
	}

	if err = utils.CopyNAndCheck(dst, src, int64(blk.Size())); err != nil {
		src.Close()
		dst.Close()
		log.Printf("[ERROR] FileRawDevice.SetBlock id=%x error='%v'", blk.ID(), err)
		return nil, err
	}
	src.Close()
	err = dst.Close()

	log.Printf("[DEBUG] FileRawDevice.SetBlock id=%x size=%d error='%v'", dstBlk.ID(), dstBlk.Size(), err)

	return dstBlk.ID(), err
}

// ReleaseBlock marks a block to be released (eventually removed) from the store.
// func (st *FileRawDevice) ReleaseBlock(id []byte) error {
// 	sid := hex.EncodeToString(id)
// 	ap := st.abspath(sid)
//
// 	if _, err := os.Stat(ap); err != nil {
// 		return err
// 	}
//
// 	//log.Printf("Releasing block/%x path='%s'", id, ap)
// 	return errors.New("TBI")
// }

// IterIDs iterates over all block ids
// func (st *FileRawDevice) IterIDs(f func(id []byte) error) error {
// 	// Traverse in-mem blocks
// 	i := 0
// 	st.mu.RLock()
// 	inMem := make([][]byte, len(st.buf))
// 	for k := range st.buf {
// 		inMem[i] = []byte(k)
// 		i++
// 	}
// 	st.mu.RUnlock()
//
// 	for _, v := range inMem {
// 		if err := f(v); err != nil {
// 			return err
// 		}
// 	}
//
// 	// Traverse all block files
// 	files, err := ioutil.ReadDir(st.datadir)
// 	if err != nil {
// 		return err
// 	}
//
// 	for _, fl := range files {
// 		if fl.IsDir() {
// 			continue
// 		}
//
// 		id, er := hex.DecodeString(fl.Name())
// 		if er != nil {
// 			continue
// 		}
//
// 		if er := f(id); er != nil {
// 			err = er
// 			break
// 		}
// 	}
//
// 	return err
// }

// Iter iterates over blocks in theh store.  If an error is returned by the callback
// iteration is immediately terminated returning the error.
// func (st *FileRawDevice) Iter(f func(block *structs.Block) error) error {
// 	// read in-mem blocks
// 	st.mu.RLock()
// 	for _, v := range st.buf {
// 		if err := f(v); err != nil {
// 			return err
// 		}
// 	}
// 	st.mu.RUnlock()
//
// 	files, err := ioutil.ReadDir(st.datadir)
// 	if err != nil {
// 		return err
// 	}
//
// 	for _, fl := range files {
// 		if fl.IsDir() {
// 			continue
// 		}
//
// 		if _, er := hex.DecodeString(fl.Name()); er != nil {
// 			continue
// 		}
//
// 		fp := st.abspath(fl.Name())
// 		blk, er := st.readBlockFromFile(fp)
// 		if er != nil {
// 			err = er
// 			break
// 		}
// 		if er := f(blk); er != nil {
// 			err = er
// 			break
// 		}
// 	}
//
// 	return err
// }
//

// Close closes the device performing necessary cleanup and shutdown
func (st *FileRawDevice) Close() error {
	return nil
}
