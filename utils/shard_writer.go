package utils

import (
	"bytes"
)

// Shard is a piece of a given file.  It contains the data, offset in the file and index
// in the file
type Shard struct {
	Data   []byte
	Index  uint64
	Offset uint64
}

// ShardWriter implements a writer closer that shards the written by the given block size.
// As each shard is read it made available via a channel containing the data, index and
// offset. This can then be used to consume each shard concurrently.
type ShardWriter struct {
	// block size
	bs uint64
	// temporary buffer to store a partial chunk
	buf *bytes.Buffer
	// counter for block size splitting
	c int
	// current block index
	idx uint64
	// Generated chunks to be converted to blocks
	out chan *Shard
}

// NewShardWriter inits a new ShardWriter with the given chunksize and a shard buffer size.
// The shard buffer size is the buffered channel size where shards are available.  A new
// shard is produced each time chunkSize bytes have been read.  The last chunk may be less
// than the block size
func NewShardWriter(chunkSize uint64, shardBufSize int) *ShardWriter {

	wr := &ShardWriter{
		bs:  chunkSize,
		buf: bytes.NewBuffer(nil),
	}

	if shardBufSize < 1 {
		wr.out = make(chan *Shard, 8)
	} else {
		wr.out = make(chan *Shard, shardBufSize)
	}

	return wr
}

// Shards returns a channel containing generated shards available for concurrent
// consumption.  This is so the consumer can start the number of go-routines per their
// choosing
func (wr *ShardWriter) Shards() <-chan *Shard {
	return wr.out
}

func (wr *ShardWriter) reset() {
	wr.idx++
	wr.buf.Reset()
	wr.c = 0
}

func (wr *ShardWriter) write(p []byte) (int, error) {
	n, err := wr.buf.Write(p)
	wr.c += n
	return n, err
}

func (wr *ShardWriter) Write(p []byte) (n int, err error) {
	//l := len(p)
	nsz := uint64(wr.c + len(p))

	if nsz > wr.bs {
		rem := wr.bs - uint64(wr.c)
		// xs size
		//xs := nsz - wr.bs
		//xssz := l - int(xs)
		//log.Println(">>", nsz, xs, xssz)
		// write regular
		// n, err = wr.buf.Write(p[:(l - xssz)])
		// wr.c += n
		//n, err = wr.write(p[:(l - xssz)])
		n, err = wr.write(p[:rem])
		//log.Printf("> buff=%d input=%d bs=%d n=%d error='%v'", wr.c, len(p), wr.bs, n, err)
		if err != nil {
			return
		}

		wr.genblock()
		wr.reset()

		// Write excess
		var c int
		// c, err = wr.buf.Write(p[xssz:])
		// n += c
		// wr.c += c
		//c, err = wr.write(p[xssz:])
		c, err = wr.write(p[rem:])
		n += c
		//log.Printf("> xs buff=%d input=%d bs=%d n=%d error='%v'", wr.c, len(p), wr.bs, n, err)

	} else if nsz < wr.bs {

		//n, err = wr.buf.Write(p)
		//wr.c += n
		//log.Printf("< buff=%d input=%d bs=%d written=%d error='%v'", wr.c, len(p), wr.bs, n, err)
		n, err = wr.write(p)

	} else {

		// n, err = wr.buf.Write(p)
		// wr.c += n
		n, err = wr.write(p)
		//log.Printf("= buff=%d input=%d bs=%d written=%d error='%v'", wr.c, len(p), wr.bs, n, err)

		if err == nil {
			wr.genblock()
			wr.reset()
		}

	}

	return
}

// create a new shard and submit to the block generator
func (wr *ShardWriter) genblock() {
	buf := wr.buf.Bytes()
	bufLen := len(buf)
	if bufLen == 0 {
		return
	}

	data := make([]byte, bufLen)
	copy(data, buf)
	wr.out <- &Shard{Index: wr.idx, Data: data, Offset: wr.idx * wr.bs}
}

// Close closes the writer writing the remainder of the data in the buffer as a shard. It
// zeros out the internal state closing the shard channel as well.  Once the ShardWriter
// is closed it cannot be used again and a new one must be instantiated.
func (wr *ShardWriter) Close() error {
	// Write out whatever remains in the buffer
	wr.genblock()

	wr.buf = nil
	wr.c = 0
	wr.idx = 0

	close(wr.out)
	return nil
}
