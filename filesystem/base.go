package filesystem

import (
	"encoding/hex"
	"os"
	"time"

	"github.com/hexablock/blox/block"
)

type filebase struct {
	// File system name. This is only populated if available from the tree block
	name string
	// POSIX mode useful when writing
	mode os.FileMode
	// Flag/s for when file is opened
	flag int
	// File modification time
	mtime time.Time
	// Source block
	blk block.Block
	// Device holding the root block
	dev BlockDevice
}

func (fb *filebase) Flags() int {
	return fb.flag
}

// IsDir return true if the underlying block is a tree block.
func (fb *filebase) IsDir() bool {
	return fb.blk.Type() == block.BlockTypeTree
}

func (fb *filebase) Sys() interface{} {
	return fb.blk
}

// Name returns the hex encoded id of the file if the name is not populated
func (fb *filebase) Name() string {
	if fb.name == "" {
		return hex.EncodeToString(fb.blk.ID())
	}
	return fb.name
}

// ModTime returns the current time.  It currently exists to satisfy the os.FileInfo
// interface
func (fb *filebase) ModTime() time.Time {
	return fb.mtime
}

// Mode returns the mode of the file.  It currentlys exists to satisfy the os.FileInfo
// interface
func (fb *filebase) Mode() os.FileMode {
	return fb.mode
}
