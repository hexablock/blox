package block

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidBlock     = errors.New("invalid block")
	ErrBlockNotFound    = errors.New("block not found")
	ErrBlockExists      = errors.New("block exists")
	ErrInvalidBlockType = errors.New("invalid block type")
	ErrReadBlockType    = errors.New("failed to read BlockType")
	ErrWriteBlockType   = errors.New("failed to write BlockType")

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
