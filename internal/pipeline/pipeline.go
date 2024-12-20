package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"text/template"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/concu/worker"
	"github.com/agnosticeng/tallyctx"
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
	var logger = slogctx.FromCtx(ctx)

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

	logger.Info("initializing pipeline", "start", start)

	var (
		group, groupctx                 = errgroup.WithContext(ctx)
		tipChan                         = make(chan uint64, 1)
		batcherChan                     = make(chan *Batch, 3)
		lastOutChan                     = batcherChan
		tipTrackerCtx, tipTrackerCancel = context.WithCancel(groupctx)
	)

	group.Go(func() error {
		var batcherCtx = slogctx.With(groupctx, "module", "batcher")
		batcherCtx = tallyctx.NewContext(batcherCtx, tallyctx.FromContextOrNoop(batcherCtx).SubScope("batcher"))

		return Batcher(
			batcherCtx,
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
		tipTrackerCtx = slogctx.With(tipTrackerCtx, "module", "batcher")
		tipTrackerCtx = tallyctx.NewContext(tipTrackerCtx, tallyctx.FromContextOrNoop(tipTrackerCtx).SubScope("batcher"))

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
			defer close(outchan)

			return worker.RunN(
				groupctx,
				step.Workers,
				func(ctx context.Context, j int) func() error {
					return func() error {
						var stepCtx = slogctx.With(
							ctx,
							"module", "step",
							"step", i,
							"worker", j,
						)

						stepCtx = tallyctx.NewContext(
							stepCtx,
							tallyctx.FromContextOrNoop(stepCtx).
								SubScope("step").
								Tagged(map[string]string{
									"step":   strconv.FormatInt(int64(i), 10),
									"worker": strconv.FormatInt(int64(j), 10),
								}),
						)

						return Step(
							stepCtx,
							tmpl,
							inchan,
							outchan,
							step,
						)
					}
				},
			)
		})

		lastOutChan = outchan
	}

	group.Go(func() error {
		var finalizerCtx = slogctx.With(groupctx, "module", "finalizer")
		finalizerCtx = tallyctx.NewContext(finalizerCtx, tallyctx.FromContextOrNoop(finalizerCtx).SubScope("finalizer"))

		return Finalizer(finalizerCtx, lastOutChan, conf.Finalizer)
	})

	return group.Wait()
}
