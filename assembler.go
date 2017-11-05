package blox

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/hexablock/blox/block"
)

// Assembler assembles all blocks in a root block
type Assembler struct {
	numRoutines int

	dev BlockDevice

	idx *block.IndexBlock

	runtime time.Duration
}

// NewAssembler inits a new assembler backed by the BlockDevice.  It launches
// numRoutines assemblers to retreive and process blocks.
func NewAssembler(dev BlockDevice, numRoutines int) *Assembler {
	return &Assembler{
		numRoutines: numRoutines,
		dev:         dev,
	}
}

// Runtime returns the time taken to assemble the root block
func (asm *Assembler) Runtime() time.Duration {
	return asm.runtime
}

// SetRoot retreives the block associated to the id.  This is used to retreive
// complete indexes ar trees
func (asm *Assembler) SetRoot(id []byte) (*block.IndexBlock, error) {
	root, err := asm.dev.GetBlock(id)
	if err != nil {
		return nil, err
	}
	idx, ok := root.(*block.IndexBlock)
	if !ok {
		return nil, fmt.Errorf("not an index block")
	}

	asm.idx = idx
	return idx, nil
}

// Assemble begins to retreive all blocks in the root and write then to the
// writer.  Sequence order is maintained
func (asm *Assembler) Assemble(wr io.Writer) error {

	start := time.Now()
	defer func(s time.Time) {
		asm.runtime = time.Since(s)
	}(start)

	done := make(chan struct{})
	defer close(done)

	blkIDs, errc := asm.assembleFromIndexBlock(done)

	c := make(chan result)

	var wg sync.WaitGroup
	wg.Add(asm.numRoutines)

	for i := 0; i < asm.numRoutines; i++ {
		go func() {
			asm.assemble(done, blkIDs, c)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	s := uint64(0)
	q := make(map[uint64][]byte)
	for rslt := range c {
		if rslt.err != nil {
			return rslt.err
		}

		data := rslt.id

		// Sequence data
		if rslt.idx == s {
			n, er := wr.Write(data)
			if er != nil {
				return er
			}
			if n != len(data) {
				return io.ErrShortWrite
			}

			//log.Printf("TODO: WRITE Assemble block index=%d size=%d", rslt.idx, len(data))
			s++
		} else {
			q[rslt.idx] = data
		}

		for {
			val, ok := q[s]
			if !ok {
				break
			}

			n, er := wr.Write(val)
			if er != nil {
				return er
			}
			if n != len(val) {
				return io.ErrShortWrite
			}

			//log.Printf("TODO: WRITE Assemble block index=%d size=%d", s, len(val))
			delete(q, s)
			s++
		}
	}

	//log.Println("WAITING ON ERROR")
	err := <-errc

	return err
}

func (asm *Assembler) assemble(done <-chan struct{}, bids <-chan shard, out chan<- result) {
	for bid := range bids {
		rslt := result{idx: bid.Index}

		blk, err := asm.dev.GetBlock(bid.Data)
		if err == nil {

			var rd io.ReadCloser
			if rd, err = blk.Reader(); err == nil {
				rslt.id, rslt.err = ioutil.ReadAll(rd)
				rd.Close()
			}
		}

		if err != nil {
			rslt.err = err
		}

		select {
		case out <- rslt:
		case <-done:
			return
		}

	}

	//log.Println("ASSEMBLER DONE")
}

func (asm *Assembler) assembleFromIndexBlock(done <-chan struct{}) (<-chan shard, <-chan error) {
	blocks := make(chan shard)
	errc := make(chan error, 1)

	go func(idx *block.IndexBlock) {
		defer close(blocks)

		errc <- idx.Iter(func(index uint64, id []byte) error {
			sh := shard{Index: index, Data: id}

			select {
			case blocks <- sh:
			case <-done:
				return errors.New("assemble cancelled")
			}

			return nil
		})

		//log.Print("GEN DONE")

	}(asm.idx)

	return blocks, errc
}
