package pipeline

import (
	"context"
	"time"

	slogctx "github.com/veqryn/slog-context"
)

type FinalizerConfig struct{}

func Finalizer(
	ctx context.Context,
	inchan <-chan *Batch,
	conf FinalizerConfig,
) error {
	var (
		logger = slogctx.FromCtx(ctx)
		t      = time.Now()
		items  uint64
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	for {
		select {
		case <-ctx.Done():
			return nil
		case b, open := <-inchan:
			if !open {
				return nil
			}

			items = items + (b.End - b.Start)

			logger.Info(
				"batch finalized",
				"number", b.Number,
				"start", b.Start,
				"end", b.End,
				"throughput", float64(items)/time.Since(t).Seconds(),
			)

			b.Conn.Release()
		}
	}
}
