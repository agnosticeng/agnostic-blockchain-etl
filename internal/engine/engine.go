package engine

import (
	"context"
	"time"
)

type Engine interface {
	Start() error
	Stop()
	Wait() error
	AcquireConn() (Conn, error)
}

type QueryMetadata struct {
	Rows       uint64
	Bytes      uint64
	TotalRows  uint64
	WroteRows  uint64
	WroteBytes uint64
	Elapsed    time.Duration
	Logs       []*Log
}

type Log struct {
	Time     time.Time
	Hostname string
	QueryID  string
	ThreadID uint64
	Priority int8
	Source   string
	Text     string
}

type Conn interface {
	Ping(ctx context.Context) error
	Exec(ctx context.Context, query string, args ...any) (*QueryMetadata, error)
	Select(ctx context.Context, res any, query string, args ...any) (*QueryMetadata, error)
	Release()
}
