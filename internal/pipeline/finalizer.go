package pipeline

import (
	"context"
	"time"

	"github.com/agnosticeng/tallyctx"
	"github.com/uber-go/tally/v4"
	slogctx "github.com/veqryn/slog-context"
)

type FinalizerMetrics struct {
	Batches tally.Counter
	Items   tally.Counter
	MaxEnd  tally.Gauge
}

func NewFinalizerMetrics(scope tally.Scope) *FinalizerMetrics {
	return &FinalizerMetrics{
		Batches: scope.Counter("batches"),
		Items:   scope.Counter("items"),
		MaxEnd:  scope.Gauge("max_end"),
	}
}

type FinalizerConfig struct{}

func Finalizer(
	ctx context.Context,
	inchan <-chan *Batch,
	conf FinalizerConfig,
) error {
	var (
		logger  = slogctx.FromCtx(ctx)
		metrics = NewFinalizerMetrics(tallyctx.FromContextOrNoop(ctx))
		t       = time.Now()
		items   uint64
		maxEnd  uint64
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

			items = items + (b.End - b.Start) + 1
			maxEnd = max(maxEnd, b.End)

			logger.Info(
				"batch finalized",
				"number", b.Number,
				"start", b.Start,
				"end", b.End,
				"throughput", float64(items)/time.Since(t).Seconds(),
			)

			metrics.Batches.Inc(1)
			metrics.Items.Inc(int64(b.End) - int64(b.Start))
			metrics.MaxEnd.Update(float64(maxEnd))
			b.Conn.Release()
		}
	}
}
