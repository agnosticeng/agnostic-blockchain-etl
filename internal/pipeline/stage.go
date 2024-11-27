package pipeline

import (
	"context"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	slogctx "github.com/veqryn/slog-context"
)

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
	var logger = slogctx.FromCtx(ctx)

	logger.Debug("started")
	defer logger.Debug("stopped")

	if len(conf.ClickhouseSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(conf.ClickhouseSettings))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case b, open := <-inchan:
			if !open {
				return nil
			}

			for _, file := range conf.Files {
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

				logger.Debug(
					file,
					"number", b.Number,
					"start", b.Start,
					"end", b.End,
					"duration", time.Since(t0),
				)
			}

			select {
			case <-ctx.Done():
				return nil
			case outchan <- b:
			}
		}
	}
}
