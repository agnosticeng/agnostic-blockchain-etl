package ch

import (
	"context"
	"time"

	slogctx "github.com/veqryn/slog-context"
)

type StartupProbeConfig struct {
	MaxDelay     time.Duration
	PollInterval time.Duration
}

func RunStartupProbe(ctx context.Context, pool *ConnPool, conf StartupProbeConfig) error {
	var logger = slogctx.FromCtx(ctx)

	if conf.MaxDelay == 0 {
		conf.MaxDelay = time.Second * 10
	}

	if conf.PollInterval == 0 {
		conf.PollInterval = time.Second
	}

	var tctx, cancel = context.WithTimeout(ctx, conf.MaxDelay)
	defer cancel()

	for {
		logger.Debug("probing clickhouse target")

		var err = func() error {
			conn, err := pool.Acquire()

			if err != nil {
				return err
			}

			defer conn.Release()

			if err := conn.Ping(tctx); err != nil {
				return err
			}

			return nil
		}()

		if err == nil {
			logger.Info("successfully probed clickhouse target")
			return nil
		}

		logger.Debug("failed to probe clickhouse target", "error", err.Error())

		select {
		case <-tctx.Done():
			return tctx.Err()
		case <-time.After(conf.PollInterval):
		}
	}
}
