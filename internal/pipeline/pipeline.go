package pipeline

import (
	"context"
	"fmt"
	"text/template"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/worker"
	"github.com/google/uuid"
	"github.com/samber/lo"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

type PipelineConfig struct {
	Init       InitConfig
	TipTracker TipTrackerConfig
	Batcher    BatcherConfig
	Steps      []StepConfig
	Finalizer  FinalizerConfig
}

func (conf PipelineConfig) WithDefaults() PipelineConfig {
	conf.Init = conf.Init.WithDefaults()
	conf.TipTracker = conf.TipTracker.WithDefaults()
	conf.Batcher = conf.Batcher.WithDefaults()
	conf.Steps = lo.Map(conf.Steps, func(conf StepConfig, _ int) StepConfig { return conf.WithDefaults() })
	return conf
}

func Run(
	ctx context.Context,
	pool *ch.ConnPool,
	tmpl *template.Template,
	vars map[string]interface{},
	conf PipelineConfig,
) error {
	if len(conf.Steps) == 0 {
		return fmt.Errorf("pipeline must have at leats 1 step")
	}

	runUUID, err := uuid.NewV7()

	if err != nil {
		return err
	}

	vars["UUID"] = runUUID.String()

	start, err := Init(ctx, pool, tmpl, vars, conf.Init)

	if err != nil {
		return err
	}

	var (
		group, groupctx                 = errgroup.WithContext(ctx)
		tipChan                         = make(chan uint64, 1)
		batcherChan                     = make(chan *Batch, 3)
		lastOutChan                     = batcherChan
		tipTrackerCtx, tipTrackerCancel = context.WithCancel(groupctx)
	)

	group.Go(func() error {
		return Batcher(
			groupctx,
			pool,
			vars,
			start,
			tipChan,
			batcherChan,
			tipTrackerCancel,
			conf.Batcher,
		)
	})

	group.Go(func() error {
		return TipTracker(
			tipTrackerCtx,
			pool,
			tmpl,
			vars,
			tipChan,
			conf.TipTracker,
		)
	})

	for i, step := range conf.Steps {
		var (
			inchan  = lastOutChan
			outchan = make(chan *Batch, step.ChanSize)
		)

		group.Go(func() error {
			return worker.Controller(
				groupctx,
				step.Workers,
				func(ctx context.Context, j int) func() error {
					return func() error {
						return Step(
							slogctx.With(ctx, "step", i, "worker", j),
							tmpl,
							inchan,
							outchan,
							step,
						)
					}
				},
				func() {
					close(outchan)
				},
			)
		})

		lastOutChan = outchan
	}

	group.Go(func() error {
		return Finalizer(groupctx, lastOutChan, conf.Finalizer)
	})

	return group.Wait()
}
