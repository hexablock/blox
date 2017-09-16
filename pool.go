package blox

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hexablock/log"
)

type outPool struct {
	olock    sync.RWMutex
	outbound map[string][]*protoConn
	// Dial timeout
	dialTimeout time.Duration
	// Time before connection is considered idle and shoule be reaped.
	maxConnIdle time.Duration
	// signal a shutdown of the pool
	stop int32
}

func newOutPool(dialTimeout, maxIdle time.Duration) *outPool {
	return &outPool{
		outbound:    make(map[string][]*protoConn),
		dialTimeout: dialTimeout,
		maxConnIdle: maxIdle,
	}
}

// Reap closes all connections that have been considered idle
func (pool *outPool) reap() {
	pool.olock.Lock()
	for host, conns := range pool.outbound {

		max := len(conns)
		for i := 0; i < max; i++ {
			if time.Since(conns[i].lastused) > pool.maxConnIdle {
				conns[i].Close()
				conns[i], conns[max-1] = conns[max-1], nil
				max--
				i--
			}
		}
		// Trim any idle conns
		pool.outbound[host] = conns[:max]

	}
	pool.olock.Unlock()
}

// Gets an outbound connection to a host
func (pool *outPool) getConn(host string) (*protoConn, error) {
	if atomic.LoadInt32(&pool.stop) == 1 {
		return nil, errTransportShutdown
	}

	// Check if we have a conn cached
	var out *protoConn
	// Get a connection from the pool if we have one
	pool.olock.Lock()
	list, ok := pool.outbound[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		pool.outbound[host] = list
	}
	pool.olock.Unlock()

	if out != nil {
		// Verify that the socket is valid. Might be closed.
		if _, err := out.Read(nil); err == nil {
			return out, nil
		}
		out.Close()
		out = nil
	}

	// Try to establish a new connection
	conn, err := net.DialTimeout("tcp", host, pool.dialTimeout)
	if err != nil {
		return nil, err
	}
	setupTCPConn(conn)

	// Wrap the sock
	out = &protoConn{
		host:     host,
		Conn:     conn,
		lastused: time.Now(),
	}

	return out, nil
}

// Returns an outbound TCP connection to the pool
func (pool *outPool) returnConn(o *protoConn) {
	if atomic.LoadInt32(&pool.stop) == 1 {
		o.Close()
		return
	}
	// Update the last used time
	o.lastused = time.Now()

	// Return back to the pool
	pool.olock.Lock()
	list, _ := pool.outbound[o.host]
	pool.outbound[o.host] = append(list, o)
	pool.olock.Unlock()
}

// shutdown closes all the connections in the pool and zeros out the connection map.  It
// will also reject any new connections from being established
func (pool *outPool) shutdown() {
	atomic.StoreInt32(&pool.stop, 1)

	pool.olock.Lock()
	for _, conns := range pool.outbound {
		max := len(conns)
		for i := 0; i < max; i++ {
			conns[i].Close()
		}
	}
	pool.outbound = nil
	pool.olock.Unlock()
}

type inPool struct {
	ilock   sync.RWMutex
	inbound map[*protoConn]struct{}
}

func newInPool() *inPool {
	return &inPool{inbound: make(map[*protoConn]struct{})}
}

func (pool *inPool) register(c net.Conn) *protoConn {
	conn := &protoConn{
		Conn:     c,
		lastused: time.Now(),
	}

	pool.ilock.Lock()
	pool.inbound[conn] = struct{}{}
	pool.ilock.Unlock()

	return conn
}

// Release closes the inbound connection and de-registered it from the pool
func (pool *inPool) release(conn *protoConn) error {
	err := conn.Close()
	log.Printf("[DEBUG] Disconnected: %s", conn.RemoteAddr().String())

	pool.ilock.Lock()
	if _, ok := pool.inbound[conn]; ok {
		delete(pool.inbound, conn)
	}
	pool.ilock.Unlock()

	return err
}
