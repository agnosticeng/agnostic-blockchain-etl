package pipeline

import (
	"context"
	"log/slog"

	slogctx "github.com/veqryn/slog-context"
)

type SequencerConfig struct{}

func Sequencer(
	ctx context.Context,
	inchan <-chan *Batch,
	outchan chan<- *Batch,
	conf SequencerConfig,
) error {
	var (
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
			}

		}
	}
}
