package filesystem

import "github.com/hexablock/blox/block"

// retrieve the block from the device and create a BloxFile
func bloxFileFromHash(dev BlockDevice, sh []byte) (*BloxFile, error) {

	blk, err := dev.GetBlock(sh)
	if err != nil {
		return nil, err
	}

	return bloxFileFromBlock(dev, blk)
}

func bloxFileFromBlock(dev BlockDevice, blk block.Block) (*BloxFile, error) {
	fb := &filebase{dev: dev, blk: blk, numWorkers: 1}
	bf := &BloxFile{filebase: fb}

	switch blk.Type() {
	case block.BlockTypeData:

	case block.BlockTypeIndex:
		bf.idx = blk.(*block.IndexBlock)

	case block.BlockTypeTree:
		bf.tree = blk.(*block.TreeBlock)

	default:
		return nil, block.ErrInvalidBlockType
	}
	return bf, nil
}
