package pipeline

import (
	"context"
	"maps"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	slogctx "github.com/veqryn/slog-context"
)

type BatcherConfig struct {
	MaxBatchSize int
	StopAfter    int
}

func (conf BatcherConfig) WithDefaults() BatcherConfig {
	if conf.MaxBatchSize <= 0 {
		conf.MaxBatchSize = 100
	}

	return conf
}

func Batcher(
	ctx context.Context,
	pool *ch.ConnPool,
	vars map[string]interface{},
	start uint64,
	inchan <-chan uint64,
	outchan chan<- *Batch,
	tipTrackerCancel context.CancelFunc,
	conf BatcherConfig,
) error {
	defer close(outchan)
	defer tipTrackerCancel()

	var (
		tip        uint64
		iterations int
		logger     = slogctx.FromCtx(ctx)
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	for {
		select {
		case <-ctx.Done():
			return nil
		case newTip, open := <-inchan:
			if !open {
				return nil
			}

			if newTip > tip {
				tip = newTip
			}

			for {
				if start > tip {
					break
				}

				chconn, err := pool.Acquire()

				if err != nil {
					return err
				}

				var b Batch
				b.Number = iterations
				b.Start = start
				b.End = min(start+uint64(conf.MaxBatchSize)-1, tip)
				b.Conn = chconn
				b.Vars = maps.Clone(vars)
				b.Vars["NUMBER"] = b.Number
				b.Vars["START"] = b.Start
				b.Vars["END"] = b.End

				select {
				case <-ctx.Done():
					return nil
				case outchan <- &b:
					iterations++
					start = b.End + 1

					if iterations == conf.StopAfter {
						return nil
					}
				}
			}
		}
	}
}
