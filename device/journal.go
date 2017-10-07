package device

import (
	"sync"

	"github.com/hexablock/blox/block"
)

// JournalEntry represents a single entry for a block
type JournalEntry struct {
	id   []byte
	typ  block.BlockType
	size uint64
	data []byte
}

// ID returns the id for the entry
func (je *JournalEntry) ID() []byte {
	return je.id
}

// Type returns the type of block
func (je *JournalEntry) Type() block.BlockType {
	return je.typ
}

// Size returns the actual consumed size of the block
func (je *JournalEntry) Size() uint64 {
	return je.size
}

// Data returns the raw block data as returned from the block Writer
func (je *JournalEntry) Data() []byte {
	return je.data
}

// InmemJournal implements an in-memory Journal interface
type InmemJournal struct {
	mu sync.RWMutex
	m  map[string]*JournalEntry
}

// NewInmemJournal inits a new in-memory journal.
func NewInmemJournal() *InmemJournal {
	return &InmemJournal{m: make(map[string]*JournalEntry)}
}

// Stats returns the journal stats
func (j *InmemJournal) Stats() *Stats {
	j.mu.RLock()
	defer j.mu.RUnlock()

	stat := &Stats{TotalBlocks: len(j.m)}

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
func (j *InmemJournal) Get(id []byte) (*JournalEntry, error) {
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
func (j *InmemJournal) Set(entry *JournalEntry) error {
	k := string(entry.id)
	j.mu.RLock()
	if _, ok := j.m[k]; ok {
		j.mu.RUnlock()
		return block.ErrBlockExists
	}
	j.mu.RUnlock()

	j.mu.Lock()
	j.m[k] = entry
	j.mu.Unlock()
	return nil
}

// Remove removes the block from the journal and return true if the block was inline
// and an error if it doesn't exist
func (j *InmemJournal) Remove(id []byte) (*JournalEntry, error) {
	is := string(id)

	j.mu.Lock()
	if val, ok := j.m[is]; ok {
		delete(j.m, is)
		j.mu.Unlock()

		return val, nil
	}
	j.mu.Unlock()

	return nil, block.ErrBlockNotFound
}

// Exists returns true if the journal contains the id
func (j *InmemJournal) Exists(id []byte) bool {
	j.mu.RLock()
	_, ok := j.m[string(id)]
	j.mu.RUnlock()
	return ok
}

// Iter obtains a read-lock and interates over each key-value pair issuing the
// callback for each
func (j *InmemJournal) Iter(cb func(*JournalEntry) error) error {
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
func (j *InmemJournal) Close() error {
	return nil
}
