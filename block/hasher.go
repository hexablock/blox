package block

import (
	"hash"
	"io"
)

// HasherReader hashes all data that is read from the underlying reader
type HasherReader struct {
	r      io.Reader // Supplied underlying reader
	hasher hash.Hash // Hash function to use
	cnt    uint64    // Total bytes read
}

// NewHasherReader implments a reader that hashes the data
func NewHasherReader(hasher hash.Hash, r io.Reader) *HasherReader {
	hr := &HasherReader{r: r, hasher: hasher}
	return hr
}

// Read reads from the underlying reader, writes the data to the hasher and
// updates the bytes read count.
func (w *HasherReader) Read(b []byte) (int, error) {
	n, err := w.r.Read(b)
	// Update total read bytes
	w.cnt += uint64(n)
	// On error n will be 0, so this is still safe
	w.hasher.Write(b[:n])
	return n, err
}

// Hash returns the hash of the data written so far
func (w *HasherReader) Hash() []byte {
	sh := w.hasher.Sum(nil)
	return sh[:]
}

// DataSize returns the total bytes read
func (w *HasherReader) DataSize() uint64 {
	return w.cnt
}

// HasherWriter writes and hashes the data
type HasherWriter struct {
	mw     io.Writer // MultiWriter containing the hash writer and the underlying writer
	uw     io.Writer // Underlying supplied writer
	hasher hash.Hash // Hash writer
	cnt    uint64    // Total bytes written
}

// NewHasherWriter instantiates a new HasherWriter with given underlying writer.
func NewHasherWriter(hasher hash.Hash, w io.Writer) *HasherWriter {
	hw := &HasherWriter{uw: w, hasher: hasher}
	hw.mw = io.MultiWriter(hw.hasher, w)
	return hw
}

func (w *HasherWriter) Write(p []byte) (int, error) {
	n, err := w.mw.Write(p)
	w.cnt += uint64(n)
	return n, err
}

// Hash returns the hash of the data written so far.
func (w *HasherWriter) Hash() []byte {
	sh := w.hasher.Sum(nil)
	return sh[:]
}

// DataSize returns total bytes written
func (w *HasherWriter) DataSize() uint64 {
	return w.cnt
}

// Close closes the underlying writer
// func (w *hasherWriter) Close() error {
// 	return w.w.Close()
// }
