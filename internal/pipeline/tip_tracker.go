package pipeline

import (
	"context"
	"log/slog"
	"text/template"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	slogctx "github.com/veqryn/slog-context"
)

type TipTrackerConfig struct {
	Tip          string
	PollInterval time.Duration
}

func (conf TipTrackerConfig) WithDefaults() TipTrackerConfig {
	if len(conf.Tip) == 0 {
		conf.Tip = "tip.sql"
	}

	if conf.PollInterval == 0 {
		conf.PollInterval = time.Second * 10
	}

	return conf
}

func TipTracker(
	ctx context.Context,
	pool *ch.ConnPool,
	tmpl *template.Template,
	vars map[string]interface{},
	outchan chan<- uint64,
	conf TipTrackerConfig,
) error {
	defer close(outchan)

	var logger = slogctx.FromCtx(ctx)

	logger.Debug("started")
	defer logger.Debug("stopped")

	for {
		var err = func() error {
			chconn, err := pool.Acquire()

			if err != nil {
				return err
			}

			row, _, err := ch.SelectSingleRowFromTemplate[TipRow](
				ctx,
				chconn,
				tmpl,
				conf.Tip,
				vars,
			)

			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return nil
			case outchan <- row.Tip:
				logger.Log(ctx, slog.Level(-10), "new tip", "value", row.Tip)
			}

			return nil
		}()

		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(conf.PollInterval):
		}
	}
}
