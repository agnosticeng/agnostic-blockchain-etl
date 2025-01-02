package pipeline

import (
	"context"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/tallyctx"
	"github.com/samber/lo"
	"github.com/uber-go/tally/v4"
	slogctx "github.com/veqryn/slog-context"
)

type StageConfig struct {
	Files              []string
	ClickhouseSettings map[string]any
}

func (conf StageConfig) WithDefaults() StageConfig {
	return conf
}

type StageFileMetrics struct {
	QueryExecutionTime tally.Histogram
}

func NewStageFileMetrics(scope tally.Scope) *StageFileMetrics {
	return &StageFileMetrics{
		QueryExecutionTime: scope.Histogram(
			"query_execution_time",
			tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 10),
		),
	}
}

type StageMetrics struct {
	OutChanQueueTime tally.Histogram
	Files            []*StageFileMetrics
}

func NewStageMetrics(scope tally.Scope, files []string) *StageMetrics {
	return &StageMetrics{
		OutChanQueueTime: scope.Histogram(
			"out_chan_queue_time",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond, 2, 10),
		),
		Files: lo.Map(files, func(file string, _ int) *StageFileMetrics {
			return NewStageFileMetrics(scope.Tagged(map[string]string{"file": file}))
		}),
	}
}

func Stage(
	ctx context.Context,
	tmpl *template.Template,
	inchan <-chan *Batch,
	outchan chan<- *Batch,
	conf StageConfig,
) error {
	var (
		logger  = slogctx.FromCtx(ctx)
		metrics = NewStageMetrics(tallyctx.FromContextOrNoop(ctx), conf.Files)
	)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(ch.NormalizeSettings(conf.ClickhouseSettings)))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case b, open := <-inchan:
			if !open {
				return nil
			}

			for i, file := range conf.Files {
				var t0 = time.Now()

				_, err := ch.ExecFromTemplate(
					ctx,
					b.Conn,
					tmpl,
					file,
					b.Vars,
				)

				if err != nil {
					return err
				}

				metrics.Files[i].QueryExecutionTime.RecordDuration(time.Since(t0))

				logger.Debug(
					file,
					"number", b.Number,
					"start", b.Start,
					"end", b.End,
					"duration", time.Since(t0),
				)
			}

			var t0 = time.Now()

			select {
			case <-ctx.Done():
				return nil
			case outchan <- b:
				metrics.OutChanQueueTime.RecordDuration(time.Since(t0))
			}
		}
	}
}
