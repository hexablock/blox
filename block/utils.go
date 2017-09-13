package block

import (
	"io"
)

// WriteBlockType writes the block type to the writer ensuring a successful write
func WriteBlockType(wr io.Writer, typ BlockType) error {
	n, err := wr.Write([]byte{byte(typ)})
	if err == nil {
		if n != 1 {
			err = ErrWriteBlockType
		}
	}
	return err
}

// ReadBlockType reads the BlockType from the reader ensuring a successful read
func ReadBlockType(r io.Reader) (BlockType, error) {
	var bt BlockType
	// read block type
	typ := make([]byte, 1)
	n, err := r.Read(typ)
	if err == nil {
		if n != 1 {
			err = ErrReadBlockType
		} else {
			bt = BlockType(typ[0])
		}
	}

	return bt, err
}

func ParseBlockType(typ string) (btyp BlockType, err error) {

	switch typ {
	case "data":
		btyp = BlockTypeData
	case "index":
		btyp = BlockTypeIndex
	case "tree":
		btyp = BlockTypeTree
	default:
		err = ErrInvalidBlockType
	}

	return
}
