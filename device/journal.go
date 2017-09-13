package device

import (
	"sync"

	"github.com/hexablock/blox/block"
)

// InmemJournal implements an in-memory Journal interface
type InmemJournal struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewInmemJournal() *InmemJournal {
	return &InmemJournal{m: make(map[string][]byte)}
}

// Get retreives the value for the given id.  It returns a ErrNotFoundError if the id is not
// found
func (j *InmemJournal) Get(id []byte) ([]byte, error) {
	j.mu.RLock()
	val, ok := j.m[string(id)]
	if ok {
		j.mu.RUnlock()
		return val, nil
	}
	j.mu.RUnlock()
	return nil, block.ErrBlockNotFound
}

// Set sets the id to the value in the journal.  It returns an error if the block exists.
func (j *InmemJournal) Set(id, val []byte) error {
	j.mu.RLock()
	if _, ok := j.m[string(id)]; ok {
		j.mu.RUnlock()
		return block.ErrBlockExists
	}
	j.mu.RUnlock()

	j.mu.Lock()
	j.m[string(id)] = val
	j.mu.Unlock()
	return nil
}

// Remove removes the block from the journal and return true if the block was inline and
// an error if it doesn't exist
func (j *InmemJournal) Remove(id []byte) (inline bool, err error) {
	is := string(id)

	j.mu.Lock()
	if val, ok := j.m[is]; ok {
		delete(j.m, is)
		j.mu.Unlock()

		if len(val) > 9 {
			inline = true
		}

		return
	}
	j.mu.Unlock()

	return false, block.ErrBlockNotFound
}

// Exists returns true if the journal contains the id
func (j *InmemJournal) Exists(id []byte) bool {
	j.mu.RLock()
	_, ok := j.m[string(id)]
	j.mu.RUnlock()
	return ok
}

// Close is a no-op to satifsy the journal interface
func (j *InmemJournal) Close() error {
	return nil
}
