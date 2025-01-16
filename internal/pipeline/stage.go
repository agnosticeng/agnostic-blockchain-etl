package pipeline

import (
	"context"
	"log/slog"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/tallyctx"
	"github.com/samber/lo"
	"github.com/uber-go/tally/v4"
	slogctx "github.com/veqryn/slog-context"
)

type StageFileMetrics struct {
	QueryExecutionTime tally.Histogram
	Elapsed            tally.Histogram
	Rows               tally.Counter
	Bytes              tally.Counter
	TotalRows          tally.Counter
	WroteRows          tally.Counter
	WroteBytes         tally.Counter
}

func NewStageFileMetrics(scope tally.Scope) *StageFileMetrics {
	return &StageFileMetrics{
		QueryExecutionTime: scope.Histogram(
			"query_execution_time",
			tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 10),
		),
		Elapsed: scope.Histogram(
			"elapsed",
			tally.MustMakeExponentialDurationBuckets(100*time.Millisecond, 2, 10),
		),
		Rows:       scope.Counter("rows"),
		Bytes:      scope.Counter("bytes"),
		TotalRows:  scope.Counter("total_rows"),
		WroteRows:  scope.Counter("wrote_rows"),
		WroteBytes: scope.Counter("wrote_bytes"),
	}
}

type StageMetrics struct {
	Active           tally.Gauge
	OutChanQueueTime tally.Histogram
	Files            []*StageFileMetrics
}

func NewStageMetrics(scope tally.Scope, files []string) *StageMetrics {
	return &StageMetrics{
		Active: scope.Gauge("active"),
		OutChanQueueTime: scope.Histogram(
			"out_chan_queue_time",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*5, 2, 20),
		),
		Files: lo.Map(files, func(file string, _ int) *StageFileMetrics {
			return NewStageFileMetrics(scope.Tagged(map[string]string{"file": file}))
		}),
	}
}

type StageConfig struct {
	Files              []string
	ClickhouseSettings map[string]any
}

func (conf StageConfig) WithDefaults() StageConfig {
	return conf
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
				if err := executeQueryFile(ctx, tmpl, file, b, metrics, logger, i); err != nil {
					logger.Error(err.Error())
					return err
				}
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

func executeQueryFile(
	ctx context.Context,
	tmpl *template.Template,
	file string,
	b *Batch,
	metrics *StageMetrics,
	logger *slog.Logger,
	fileIndex int,
) error {
	var t0 = time.Now()

	metrics.Active.Update(1)
	defer metrics.Active.Update(0)

	md, err := ch.ExecFromTemplate(
		ctx,
		b.Conn,
		tmpl,
		file,
		b.Vars,
	)

	if err != nil {
		return err
	}

	var fileMetrics = metrics.Files[fileIndex]

	fileMetrics.QueryExecutionTime.RecordDuration(time.Since(t0))
	fileMetrics.Elapsed.RecordDuration(md.Elapsed)
	fileMetrics.Rows.Inc(int64(md.Rows))
	fileMetrics.Bytes.Inc(int64(md.Bytes))
	fileMetrics.TotalRows.Inc(int64(md.TotalRows))
	fileMetrics.WroteRows.Inc(int64(md.WroteRows))
	fileMetrics.WroteBytes.Inc(int64(md.WroteBytes))

	logger.Debug(
		file,
		"number", b.Number,
		"start", b.Start,
		"end", b.End,
		"duration", time.Since(t0),
	)

	return nil
}
