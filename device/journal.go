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
func (j *InmemJournal) Remove(id []byte) ([]byte, error) {
	is := string(id)

	j.mu.Lock()
	if val, ok := j.m[is]; ok {
		delete(j.m, is)
		j.mu.Unlock()

		// if len(val) > 9 {
		// 	inline = true
		// }

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
func (j *InmemJournal) Iter(cb func(key, value []byte) error) error {
	var err error

	j.mu.RLock()
	for k, val := range j.m {
		key := []byte(k)
		if err = cb(key, val); err != nil {
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
