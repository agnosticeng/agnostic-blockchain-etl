package run

import (
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/hashicorp/go-multierror"
)

type ConnPoolConfig struct {
	DSN             string
	MaxConnLifetime time.Duration
}

type ConnPool struct {
	conf    ConnPoolConfig
	lock    sync.Mutex
	counter int
	conns   map[int]*Conn
}

type Conn struct {
	pool      *ConnPool
	id        int
	createdAt time.Time
	leased    bool
	driver.Conn
}

func (conn *Conn) Release() {
	conn.pool.release(conn)
}

func NewConnPool(conf ConnPoolConfig) *ConnPool {
	if conf.MaxConnLifetime == 0 {
		conf.MaxConnLifetime = time.Hour
	}

	return &ConnPool{
		conf:  conf,
		conns: make(map[int]*Conn),
	}
}

func (pool *ConnPool) Acquire() (*Conn, error) {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	for {
		var conn = pool.findFreeConn()

		if conn == nil {
			break
		}

		if time.Since(conn.createdAt) >= pool.conf.MaxConnLifetime {
			conn.leased = true
			return conn, nil
		}

		if err := conn.Close(); err != nil {
			return nil, err
		}

		delete(pool.conns, conn.id)
	}

	chopts, err := clickhouse.ParseDSN(pool.conf.DSN)

	if err != nil {
		return nil, err
	}

	chopts.MaxOpenConns = 1
	chopts.ConnMaxLifetime = pool.conf.MaxConnLifetime * 2
	chconn, err := clickhouse.Open(chopts)

	if err != nil {
		return nil, err
	}

	var conn = &Conn{
		Conn:      chconn,
		pool:      pool,
		id:        pool.counter,
		createdAt: time.Now(),
		leased:    true,
	}

	pool.conns[pool.counter] = conn
	pool.counter++
	return conn, nil
}

func (pool *ConnPool) release(conn *Conn) {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	conn.leased = false
}

func (pool *ConnPool) findFreeConn() *Conn {
	for _, v := range pool.conns {
		if !v.leased {
			return v
		}
	}

	return nil
}

func (pool *ConnPool) Close() error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	var res *multierror.Error

	for _, conn := range pool.conns {
		if err := conn.Close(); err != nil {
			res = multierror.Append(res, err)
		}
	}

	return res.ErrorOrNil()
}
