package pipeline_retrier

import (
	"context"
	"fmt"
	"text/template"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
)

type RetryStrategy struct {
	MaxBatchSizeMultiplier float64
}

func Run(
	ctx context.Context,
	pool *ch.ConnPool,
	tmpl *template.Template,
	vars map[string]interface{},
	conf pipeline.PipelineConfig,
	strat RetryStrategy,
) error {
	if strat.MaxBatchSizeMultiplier == 0 {
		strat.MaxBatchSizeMultiplier = 0.8
	}

	if !(strat.MaxBatchSizeMultiplier > 0 && strat.MaxBatchSizeMultiplier < 1) {
		return fmt.Errorf("invalid MaxBatchSizeMultiplier value: %f", strat.MaxBatchSizeMultiplier)
	}

	var logger = slogctx.FromCtx(ctx)

	for {
		var err = pipeline.Run(ctx, pool, tmpl, vars, conf)

		ex, ok := lo.ErrorsAs[*proto.Exception](err)

		if !ok {
			return err
		}

		if chproto.Error(ex.Code) == chproto.ErrMemoryLimitExceeded {
			var newMaxBatchSize = int(float64(conf.Batcher.MaxBatchSize) * strat.MaxBatchSizeMultiplier)

			if newMaxBatchSize < conf.Batcher.MaxBatchSize {
				logger.Warn(
					"memory limit exceeded, will retry pipeline with lower batch size",
					"current", conf.Batcher.MaxBatchSize,
					"new", newMaxBatchSize,
					"message", ex.Message,
				)
				conf.Batcher.MaxBatchSize = newMaxBatchSize
				continue
			}
		}

		return err
	}
}
