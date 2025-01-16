package remote

import (
	"context"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/engine"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/engine/impl/local"
)

type RemoteEngineConfig struct {
	local.ConnPoolConfig
}

type RemoteEngine struct {
	conf     RemoteEngineConfig
	pool     *local.ConnPool
	stopChan chan interface{}
}

func NewRemoteEngine(ctx context.Context, conf RemoteEngineConfig) (*RemoteEngine, error) {
	return &RemoteEngine{
		conf:     conf,
		pool:     local.NewConnPool(conf.ConnPoolConfig),
		stopChan: make(chan interface{}, 1),
	}, nil
}

func (eng *RemoteEngine) Start() error {
	return nil
}

func (eng *RemoteEngine) Stop() {
	eng.pool.Close()
	close(eng.stopChan)
}

func (eng *RemoteEngine) Wait() error {
	<-eng.stopChan
	return nil
}

func (eng *RemoteEngine) AcquireConn() (engine.Conn, error) {
	return eng.pool.Acquire()
}
