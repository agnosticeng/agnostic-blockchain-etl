package pipeline

import (
	"context"
	"log/slog"

	"github.com/agnosticeng/tallyctx"
	"github.com/uber-go/tally/v4"
	slogctx "github.com/veqryn/slog-context"
)

type SequencerMetrics struct {
	BufferSize tally.Gauge
}

func NewSequencerMetrics(scope tally.Scope) *SequencerMetrics {
	return &SequencerMetrics{
		BufferSize: scope.Gauge("buffer_size"),
	}
}

type SequencerConfig struct{}

func Sequencer(
	ctx context.Context,
	inchan <-chan *Batch,
	outchan chan<- *Batch,
	conf SequencerConfig,
) error {
	var (
		metrics            = NewSequencerMetrics(tallyctx.FromContextOrNoop(ctx))
		buf                BatchBuffer
		nextSequenceNumber int
		logger             = slogctx.FromCtx(ctx)
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

			buf.Insert(b)
			metrics.BufferSize.Update(float64(len(buf)))

			for {
				if len(buf) == 0 {
					break
				}

				b = buf[0]

				if b.Number != nextSequenceNumber {
					break
				}

				select {
				case <-ctx.Done():
					return nil
				case outchan <- b:
					logger.Log(
						ctx,
						slog.LevelDebug,
						"batch sequenced",
						"number", b.Number,
						"start", b.Start,
						"end", b.End,
					)
				}

				nextSequenceNumber++
				buf = buf[1:]
				metrics.BufferSize.Update(float64(len(buf)))
			}

		}
	}
}
