package bskpool

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"sync"
	"time"
)

// PoolConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type PoolConn struct {
	Conn     *beanstalk.Conn
	mu       sync.RWMutex
	c        *channelPool
	unusable bool
}

// Close() puts the given connects back to the pool instead of closing it.
func (p *PoolConn) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		if p.Conn != nil {
			return p.Conn.Close()
		}
		return nil
	}
	return p.c.put(p.Conn)
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *PoolConn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn *beanstalk.Conn) *PoolConn {
	p := &PoolConn{c: c}
	p.Conn = conn
	return p
}

func (p *PoolConn) Status() (map[string]string, error) {
	if nil == p.Conn {
		return map[string]string{}, failConnErr()
	}
	return p.Conn.Stats()
}

func (p *PoolConn) Put(tubeName, msg string) (uint64, error) {
	return p.putJob(tubeName, msg, 0)
}

func (p *PoolConn) PutDelay(tubeName, msg string) (uint64, error) {
	return p.putJob(tubeName, msg, 30)
}

func (p *PoolConn) Reserve(tubeName string) (uint64, []byte, error) {
	if nil == p.Conn {
		return 0, []byte{}, failConnErr()
	}
	tube := beanstalk.NewTubeSet(p.Conn, tubeName)
	return tube.Reserve(time.Second * 3)
}

func (p *PoolConn) Release(jobId uint64) error {
	if nil == p.Conn {
		return failConnErr()
	}
	return p.Conn.Release(jobId, 1, time.Second*30)
}

//Reserve状态的job，只有当前实例可以delete
func (p *PoolConn) Delete(id uint64) error {
	if nil == p.Conn {
		return failConnErr()
	}
	return p.Conn.Delete(id)
}

func (p *PoolConn) putJob(tubeName, msg string, delay time.Duration) (uint64, error) {
	if nil == p.Conn {
		return 0, failConnErr()
	}
	tube := &beanstalk.Tube{Conn: p.Conn, Name: tubeName}
	return tube.Put([]byte(msg), 1, delay*time.Second, 30*time.Second)
}

func failConnErr() error {
	return errors.New("connection beanstalkd failed.")
}
