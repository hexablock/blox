package block

import (
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hexablock/hexatype"
)

// FileDataBlock is a block with a file as its store.
type FileDataBlock struct {
	*baseBlock
	th *os.File // temp file handle for writer
}

// NewFileDataBlock instantiates a new Block for the given type
func NewFileDataBlock(uri *URI, hasher hexatype.Hasher) *FileDataBlock {
	return &FileDataBlock{
		baseBlock: &baseBlock{typ: BlockTypeData, uri: uri, hasher: hasher}}
}

// LoadFileDataBlock loads a FileDataBlock from a file on disk.  It does not actually
// open the file
func LoadFileDataBlock(uri *URI, hasher hexatype.Hasher) (*FileDataBlock, error) {
	id, err := hex.DecodeString(filepath.Base(uri.Path))
	if err != nil {
		return nil, err
	}

	fp := uri.Path
	stat, err := os.Stat(fp)
	if err != nil {
		return nil, ErrBlockNotFound
	}

	blk := &FileDataBlock{
		baseBlock: &baseBlock{
			hasher: hasher,
			id:     id,
			typ:    BlockTypeData,
			uri:    uri,
			size:   uint64(stat.Size() - 1), // deduct 1-byte type
		},
	}

	return blk, err
}

// Reader reads data from block.  It first burns the 1-byte type then returns a ReadCloser to the
// actual data
func (block *FileDataBlock) Reader() (io.ReadCloser, error) {
	fh, err := os.Open(block.uri.Path)
	if err != nil {
		return nil, err
	}

	// burn the type from ther reader
	if _, err = ReadBlockType(fh); err != nil {
		fh.Close()
		return nil, err
	}

	return fh, nil
}

// Writer returns a new writer closer to write data to the block.  It initializes a hashing
// writer, writing the type first before returning the writer.
func (block *FileDataBlock) Writer() (io.WriteCloser, error) {
	// Write block to a tmp file first as we need the hash which is calculated after the
	// complete write
	fh, err := ioutil.TempFile("", "block")
	if err != nil {
		return nil, err
	}
	block.th = fh
	tmpfile := fh.Name()

	block.hw = NewHasherWriter(block.hasher.New(), fh)
	if err = WriteBlockType(block.hw, block.typ); err != nil {
		fh.Close()
		os.Remove(tmpfile)
		block.th = nil
	}

	// Return the block as the writer
	return block, err
}

// Write writes and hashes the data by writing it to the underlying writer.  It also updates
// the block size
func (block *FileDataBlock) Write(b []byte) (int, error) {
	n, err := block.hw.Write(b)
	block.size += uint64(n)

	return n, err
}

// Close closes the Writer, writes the hash id to the block and resets the writer.
func (block *FileDataBlock) Close() error {
	// Close temp file.
	err := block.th.Close()
	if err == nil {
		// Write block id hash to cache
		block.id = block.hw.Hash()
		// Calculate old and new names
		oldname := block.th.Name()
		newname := filepath.Join(block.uri.Path, hex.EncodeToString(block.id))
		// Only link if it doesn't exist
		if _, err = os.Stat(newname); err != nil {
			// Link file in place
			if err = os.Link(oldname, newname); err == nil {
				// Update internal path from directory to absolute path to block.
				block.uri.Path = newname
				//log.Printf("[INFO] FileDataBlock write id=%x", block.id)
			}
		} else {
			err = ErrBlockExists
		}
		// Remove tmpfile
		os.Remove(oldname)

	}

	block.th = nil
	block.hw = nil
	return err
}

// Hash returns the hash for the DataBlock
func (block *FileDataBlock) Hash() []byte {
	// TODO: actually compute the hash of the whole block from underlying file
	return nil
}
