package block

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidBlock is used when the block is of invalid format
	ErrInvalidBlock = errors.New("invalid block")
	// ErrBlockNotFound is used when the block doesn't exist
	ErrBlockNotFound = errors.New("block not found")
	// ErrBlockExists is used when a block already exists
	ErrBlockExists = errors.New("block exists")
	// ErrInvalidBlockType is used if an unsupported block type is encountered
	ErrInvalidBlockType = errors.New("invalid block type")
	// ErrReadBlockType is an error when the type cannot be read
	ErrReadBlockType = errors.New("failed to read BlockType")
	// ErrWriteBlockType is an error when the type cannot be written
	ErrWriteBlockType    = errors.New("failed to write BlockType")
	ErrUnsupportedScheme = errors.New("unsupported scheme")

	errReaderWriterOpen = errors.New("reader/writer already open")
	errIncompleteWrite  = errors.New("incomplete write")
	errIncompleteRead   = errors.New("incomplete read")
)

// ParseError parses a error string to an actual error
func ParseError(e string) error {

	switch e {
	case ErrBlockNotFound.Error():
		return ErrBlockNotFound

	case ErrBlockExists.Error():
		return ErrBlockExists

	case ErrInvalidBlock.Error():
		return ErrInvalidBlock

	case ErrInvalidBlockType.Error():
		return ErrInvalidBlockType

	case ErrReadBlockType.Error():
		return ErrReadBlockType

	case ErrWriteBlockType.Error():
		return ErrWriteBlockType
	}

	return fmt.Errorf("%s", e)
}
