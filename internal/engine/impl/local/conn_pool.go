package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/engine"
	"github.com/hashicorp/go-multierror"
)

type ConnPoolConfig struct {
	Dsn             string
	MaxConnLifetime time.Duration
	Settings        map[string]any
}

type ConnPool struct {
	conf    ConnPoolConfig
	lock    sync.Mutex
	counter int
	conns   map[int]*Conn
}

func NewConnPool(conf ConnPoolConfig) *ConnPool {
	if conf.MaxConnLifetime == 0 {
		conf.MaxConnLifetime = time.Hour
	}

	if len(conf.Dsn) == 0 {
		conf.Dsn = "tcp://127.0.0.1:9000"
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

		if err := conn.chConn.Close(); err != nil {
			return nil, err
		}

		delete(pool.conns, conn.id)
	}

	chopts, err := clickhouse.ParseDSN(pool.conf.Dsn)

	if err != nil {
		return nil, err
	}

	chopts.MaxOpenConns = 1
	chopts.ConnMaxLifetime = pool.conf.MaxConnLifetime * 2
	chopts.Settings = clickhouse.Settings(ch.NormalizeSettings(pool.conf.Settings))
	chconn, err := clickhouse.Open(chopts)

	if err != nil {
		return nil, err
	}

	var conn = &Conn{
		chConn:    chconn,
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
		if err := conn.chConn.Close(); err != nil {
			res = multierror.Append(res, err)
		}
	}

	return res.ErrorOrNil()
}

type Conn struct {
	pool      *ConnPool
	id        int
	createdAt time.Time
	leased    bool
	chConn    driver.Conn
}

func (conn *Conn) Ping(ctx context.Context) error {
	return conn.chConn.Ping(ctx)
}

func (conn *Conn) Exec(ctx context.Context, query string, args ...any) (*engine.QueryMetadata, error) {
	var (
		md  engine.QueryMetadata
		err = conn.chConn.Exec(
			clickhouse.Context(
				ctx,
				clickhouse.WithProgress(progressHandler(&md)),
				clickhouse.WithLogs(logHandler(&md)),
			),
			query,
			args...,
		)
	)

	return &md, err
}

func (conn *Conn) Select(ctx context.Context, res any, query string, args ...any) (*engine.QueryMetadata, error) {
	var (
		md  engine.QueryMetadata
		err = conn.chConn.Select(
			clickhouse.Context(
				ctx,
				clickhouse.WithProgress(progressHandler(&md)),
				clickhouse.WithLogs(logHandler(&md)),
			),
			res,
			query,
			args...,
		)
	)

	return &md, err
}

func (conn *Conn) Release() {
	conn.pool.release(conn)
}

func progressHandler(md *engine.QueryMetadata) func(*proto.Progress) {
	return func(p *proto.Progress) {
		if p == nil {
			return
		}

		md.Rows += p.Rows
		md.Bytes += p.Bytes
		md.TotalRows += p.TotalRows
		md.WroteRows += p.WroteRows
		md.WroteBytes += p.WroteBytes
		md.Elapsed += p.Elapsed
	}
}

func logHandler(md *engine.QueryMetadata) func(*clickhouse.Log) {
	return func(l *clickhouse.Log) {
		md.Logs = append(md.Logs, &engine.Log{
			Time:     l.Time,
			Hostname: l.Hostname,
			QueryID:  l.QueryID,
			ThreadID: l.ThreadID,
			Priority: l.Priority,
			Source:   l.Source,
			Text:     l.Text,
		})
	}
}

func profileInfoHandler(_ *engine.QueryMetadata) func(*clickhouse.ProfileInfo) {
	return func(pi *clickhouse.ProfileInfo) {
		fmt.Println("PROFILE INFO", pi.String())
	}
}

func profileEventsHandler(_ *engine.QueryMetadata) func([]clickhouse.ProfileEvent) {
	return func(events []clickhouse.ProfileEvent) {
		for _, event := range events {
			fmt.Println("PROFILE EVENT", event.Name, event.Type, event.Value)
		}
	}
}
