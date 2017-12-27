package device

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hexablock/blox/block"
)

// IndexEntry represents a single entry for a block
type IndexEntry struct {
	id   []byte
	typ  block.BlockType
	size uint64
	data []byte
}

// MarshalBinary marshals the entry into a 1-8-hash-null-data - type, size, id,
// followed by a null byte then the data
func (je *IndexEntry) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 9)
	buf[0] = byte(je.typ)
	binary.BigEndian.PutUint64(buf[1:], je.size)
	buf = append(buf, je.id...)
	// append marker
	buf = append(buf, byte('|'), byte('|'))
	//buf = append(buf, '\x00')
	return append(buf, je.data...), nil
}

// UnmarshalBinary unmarshals a byte slice into an IndexEntry.  It returns an
// error if there is not enough data
func (je *IndexEntry) UnmarshalBinary(b []byte) error {
	// Min sha1 check
	if len(b) < 29 {
		return fmt.Errorf("not enough data")
	}
	je.typ = block.BlockType(b[0])
	je.size = binary.BigEndian.Uint64(b[1:9])

	i := bytes.Index(b[9:], []byte("||"))
	//i := bytes.IndexByte(b[9:], '\x00')
	if i < 0 {
		return fmt.Errorf("id not found")
	}

	je.id = make([]byte, i)
	i += 9
	copy(je.id, b[9:i])

	// marker increment
	i += 2
	l := len(b[i:])
	if l > 0 {
		je.data = make([]byte, l)
		copy(je.data, b[i:])
	}
	return nil
}

// ID returns the id for the entry
func (je *IndexEntry) ID() []byte {
	return je.id
}

// Type returns the type of block
func (je *IndexEntry) Type() block.BlockType {
	return je.typ
}

// Size returns the actual consumed size of the block
func (je *IndexEntry) Size() uint64 {
	return je.size
}

// Data returns the raw block data as returned from the block Writer
func (je *IndexEntry) Data() []byte {
	return je.data
}

// InmemIndex implements an in-memory Index interface
type InmemIndex struct {
	mu sync.RWMutex
	m  map[string]*IndexEntry

	// Sum of bytes used by each block
	usedBytes uint64
}

// NewInmemIndex inits a new in-memory journal.
func NewInmemIndex() *InmemIndex {
	return &InmemIndex{m: make(map[string]*IndexEntry)}
}

// Stats returns index stats
func (j *InmemIndex) Stats() *Stats {
	j.mu.RLock()
	defer j.mu.RUnlock()

	stat := &Stats{
		TotalBlocks: len(j.m),
		UsedBytes:   j.usedBytes,
	}

	for _, je := range j.m {
		typ := je.Type()
		switch typ {
		case block.BlockTypeData:
			stat.DataBlocks++
		case block.BlockTypeIndex:
			stat.IndexBlocks++
		case block.BlockTypeTree:
			stat.TreeBlocks++
		case block.BlockTypeMeta:
			stat.MetaBlocks++
		}
	}

	return stat
}

// Get retreives the value for the given id.  It returns a ErrNotFoundError if the
// id is not found
func (j *InmemIndex) Get(id []byte) (*IndexEntry, error) {
	j.mu.RLock()
	val, ok := j.m[string(id)]
	if ok {
		j.mu.RUnlock()
		return val, nil
	}
	j.mu.RUnlock()
	return nil, block.ErrBlockNotFound
}

// Set sets the id to the value in the journal.  It returns an error if the block
// exists.
func (j *InmemIndex) Set(entry *IndexEntry) error {
	k := string(entry.id)
	j.mu.RLock()
	if _, ok := j.m[k]; ok {
		j.mu.RUnlock()
		return block.ErrBlockExists
	}
	j.mu.RUnlock()

	j.mu.Lock()
	j.m[k] = entry
	j.usedBytes += entry.size
	j.mu.Unlock()
	return nil
}

// Remove removes the block from the journal and return true if the block was inline
// and an error if it doesn't exist
func (j *InmemIndex) Remove(id []byte) (*IndexEntry, error) {
	is := string(id)

	j.mu.Lock()
	if val, ok := j.m[is]; ok {
		j.usedBytes -= val.size
		delete(j.m, is)
		j.mu.Unlock()

		return val, nil
	}
	j.mu.Unlock()

	return nil, block.ErrBlockNotFound
}

// Exists returns true if the journal contains the id
func (j *InmemIndex) Exists(id []byte) bool {
	j.mu.RLock()
	_, ok := j.m[string(id)]
	j.mu.RUnlock()
	return ok
}

// Iter obtains a read-lock and interates over each key-value pair issuing the
// callback for each
func (j *InmemIndex) Iter(cb func(*IndexEntry) error) error {
	var err error

	j.mu.RLock()
	for _, val := range j.m {
		//key := []byte(k)
		if err = cb(val); err != nil {
			break
		}
	}
	j.mu.RUnlock()

	return err
}

// Close is a no-op to satifsy the journal interface
func (j *InmemIndex) Close() error {
	return nil
}
