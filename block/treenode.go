package block

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TreeNode contains child entries pointing to other blocks
type TreeNode struct {
	Name    string // name of the file or directory
	Address []byte // hash address to its index block
	Type    BlockType
	Mode    os.FileMode
}

//NewFileTreeNode inits a new TreeNode for a file
func NewFileTreeNode(name string, addr []byte) *TreeNode {
	return &TreeNode{
		Name:    name,
		Address: addr,
		Type:    BlockTypeIndex,
		Mode:    os.ModePerm,
	}
}

//NewDirTreeNode inits a new TreeNode for a directory
func NewDirTreeNode(name string, addr []byte) *TreeNode {
	return &TreeNode{
		Name:    name,
		Address: addr,
		Type:    BlockTypeTree,
		Mode:    os.ModePerm | os.ModeDir,
	}
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
