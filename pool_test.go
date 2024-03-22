package connpool

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type dummyConn struct {
	m         sync.Mutex
	closed    bool
	available bool
}

func (c *dummyConn) Status() ConnStatus {
	c.m.Lock()
	defer c.m.Unlock()
	return ConnStatus{
		Closed:    c.closed,
		Available: c.available,
	}
}
func (c *dummyConn) Close() error {
	c.m.Lock()
	c.closed = true
	c.m.Unlock()
	return nil
}

func Test_Pool(t *testing.T) {
	r := require.New(t)
	p := NewPool(Opts{
		Dial:             func(ctx context.Context) (Conn, error) { return &dummyConn{available: true}, nil },
		MaxStream:        1,
		MaxIdleBusyRatio: 1,
	})

	conns := make([]Conn, 0)
	for i := 0; i < 100; i++ {
		conn, _, err := p.Get(context.Background())
		r.NoError(err)
		conns = append(conns, conn)
	}
	r.Equal(100, p.Status().Busy)

	for _, conn := range conns {
		p.Release(conn)
	}
	r.Equal(0, p.Status().Busy)
	r.Equal(1, p.Status().Idle)
}

func Test_Pool_Reuse_Efficiency(t *testing.T) {
	r := require.New(t)
	p := NewPool(Opts{
		Dial:             func(ctx context.Context) (Conn, error) { return &dummyConn{available: true}, nil },
		MaxStream:        1000,
		MaxIdleBusyRatio: 1,
	})

	// Init 1000 busy conn
	conns := make([]Conn, 0)
	p.m.Lock()
	for i := 0; i < 1000; i++ {
		conn := &dummyConn{}
		conns = append(conns, conn)
		p.busyConns[conn] = &streamStatus{curStream: 1}
	}
	p.m.Unlock()

	for i := 0; i < 100000; i++ {
		randIdx := rand.IntN(len(conns))
		conn := conns[randIdx]
		p.Release(conn)
		nc, _, err := p.Get(context.Background())
		r.NoError(err)
		conns[randIdx] = nc
	}
	r.Less(p.Status().Busy, 2) // highly unlikely greater than 2, should be 1.
	r.Less(p.Status().Idle, 2)
}
