package filesystem

import (
	"os"

	"github.com/hexablock/blox/block"
)

// Readdir returns a list of BloxFiles in the directory satisfying the FileInfo interface
func (bf *BloxFile) Readdir(n int) ([]os.FileInfo, error) {
	var (
		out = make([]os.FileInfo, bf.tree.NodeCount())
		i   int
	)

	err := bf.tree.Iter(func(node *block.TreeNode) error {

		nbf, er := bloxFileFromHash(bf.dev, node.Address)
		if er != nil {
			return er
		}
		nbf.name = node.Name
		nbf.mode = node.Mode

		out[i] = nbf
		i++

		return nil
	})

	return out, err
}

// Readdirnames returns a list of file and directory names in the directory
func (bf *BloxFile) Readdirnames(n int) ([]string, error) {

	out := make([]string, bf.tree.NodeCount())
	var i int
	err := bf.tree.Iter(func(node *block.TreeNode) error {
		out[i] = node.Name
		i++

		return nil
	})

	return out, err

}
