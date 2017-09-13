package utils

import (
	"errors"
	"io"
)

var (
	errIncompleteWrite = errors.New("incomplete write")
	errIncompleteRead  = errors.New("incomplete read")
)

// CopyNAndCheck copy bytes to buffer and check all data was written
func CopyNAndCheck(dst io.Writer, src io.Reader, datasize int64) error {
	// Copy data
	n, err := io.CopyN(dst, src, datasize)
	if err == nil {
		if n != datasize {
			err = errIncompleteWrite
		}
	}
	return err
}
