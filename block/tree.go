package block

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hexablock/hexatype"
)

// TreeNode contains child entries pointing to other blocks
type TreeNode struct {
	Name    string // name of the file or directory
	Address []byte // hash address to its index block
	Type    BlockType
	Mode    os.FileMode
}

// MarshalBinary marshals the TreeNode into bytes.  It writes mode, space, block
// type, space, hash address, space, and finally the name as a string and returns the
// byte representation of the string
func (node TreeNode) MarshalBinary() []byte {
	str := fmt.Sprintf("%d %s %x %s", node.Mode, node.Type, node.Address, node.Name)
	return []byte(str)
}

// UnmarshalBinary unmarshals the given bytes into a TreeNode.  it returns an error if the
// format is not as expected
func (node *TreeNode) UnmarshalBinary(b []byte) error {
	str := string(b)
	parts := strings.Split(str, " ")

	if len(parts) < 4 {
		return fmt.Errorf("invalid tree node data")
	}

	m, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return err
	}
	node.Mode = os.FileMode(m)
	node.Type, err = ParseBlockType(parts[1])
	if err != nil {
		return err
	}

	node.Address, err = hex.DecodeString(parts[2])
	if err != nil {
		return err
	}

	node.Name = strings.Join(parts[3:], " ")
	return nil

}

// TreeBlock is a block containing other types of blocks as it's children
type TreeBlock struct {
	*baseBlock
	// Child nodes indexed by name
	mu    sync.RWMutex
	nodes map[string]*TreeNode
	// Read buffer initialized when calling Reader()
	rbuf *bytes.Buffer
}

// NewTreeBlock inits a new TreeBlock with the uri and hasher. The uri may be nil.
func NewTreeBlock(uri *URI, hasher hexatype.Hasher) *TreeBlock {
	return &TreeBlock{
		nodes:     make(map[string]*TreeNode),
		baseBlock: &baseBlock{uri: uri, typ: BlockTypeTree, hasher: hasher},
	}
}

// NodeCount returns the total number of child nodes to the TreeBlock
func (block *TreeBlock) NodeCount() int {
	block.mu.RLock()
	defer block.mu.RUnlock()
	return len(block.nodes)
}

// Iter iterates over each child TreeNode sorted by the name
func (block *TreeBlock) Iter(cb func(*TreeNode) error) error {
	block.mu.RLock()

	skeys := block.sortedKeys()
	var err error
	for _, k := range skeys {
		if err = cb(block.nodes[k]); err != nil {
			break
		}
	}

	block.mu.RUnlock()

	return err
}

// AddNodes adds TreeNodes to the TreeBlock.  It also updates the size of the block by
// computing the byte slice size
func (block *TreeBlock) AddNodes(nodes ...*TreeNode) {
	if len(nodes) == 0 {
		return
	}

	block.mu.Lock()
	for _, tn := range nodes {
		block.nodes[tn.Name] = tn
	}

	b := block.MarshalBinary()
	h := block.hasher.New()
	h.Write(b)
	sh := h.Sum(nil)
	// Update the block id based on the latest hash calculation
	block.id = sh[:]

	block.size = uint64(len(b[1:]))
	block.mu.Unlock()

}

// UnmarshalBinary unmarshals the byte slice to a tree block
func (block *TreeBlock) UnmarshalBinary(b []byte) error {
	block.typ = BlockType(b[0])
	block.size = uint64(len(b[1:]))

	//fmt.Printf("%s\n", b[1:])
	list := bytes.Split(b[1:], []byte("\n"))
	//fmt.Println(len(list))
	for _, l := range list {
		tn := &TreeNode{}
		if err := tn.UnmarshalBinary(l); err != nil {
			return err
		}
		block.nodes[tn.Name] = tn
	}

	return nil
}

// MarshalBinary marshals the TreeNodes sorted by name.  It writes a 1-byte type followed
// by each node 1 per line.
func (block *TreeBlock) MarshalBinary() []byte {
	keys := block.sortedKeys()
	list := make([][]byte, 0, len(keys))

	for _, k := range keys {
		b := block.nodes[k].MarshalBinary()
		list = append(list, b)
	}

	return append([]byte{byte(block.typ)}, bytes.Join(list, []byte("\n"))...)
}

// Reader inits the internal buffer for reading and writes the bytes to it.  It returns a
// io.ReadCloser
func (block *TreeBlock) Reader() (io.ReadCloser, error) {
	b := block.MarshalBinary()
	block.rbuf = bytes.NewBuffer(b[1:])
	return block, nil
}

func (block *TreeBlock) Read(b []byte) (int, error) {
	return block.rbuf.Read(b)
}

// Writer returns a new writer to allow writing raw bytes to the the TreeBlock.  Data is
// actually written to the structure once the writer is closed.
func (block *TreeBlock) Writer() (io.WriteCloser, error) {
	block.hw = NewHasherWriter(block.hasher.New(), bytes.NewBuffer(nil))
	err := WriteBlockType(block.hw, block.typ)
	return block, err
}

func (block *TreeBlock) Write(p []byte) (int, error) {
	return block.hw.Write(p)
}

// Hash returns the hash id of the block given the hash function
func (block *TreeBlock) Hash() []byte {
	h := block.hasher.New()
	h.Write(block.MarshalBinary())
	sh := h.Sum(nil)
	// Update the block id based on the latest hash calculation
	block.id = sh[:]
	return block.id
}

func (block *TreeBlock) sortedKeys() []string {
	out := make([]string, 0, len(block.nodes))
	for k := range block.nodes {
		out = append(out, k)
	}

	sort.Strings(out)
	return out
}

// Close closed the reader and writer
func (block *TreeBlock) Close() error {
	// Clear read buffer
	block.rbuf = nil

	// Check write buffer
	if block.hw == nil {
		return nil
	}

	block.id = block.hw.Hash()

	buf := block.hw.uw.(*bytes.Buffer)
	b := buf.Bytes()
	block.hw = nil

	return block.UnmarshalBinary(b)
}
