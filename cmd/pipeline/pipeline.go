package pipeline

import (
	"fmt"
	"net/http"
	"net/url"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline_retrier"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	"github.com/agnosticeng/objstr"
	objstrutils "github.com/agnosticeng/objstr/utils"
	"github.com/agnosticeng/tallyctx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "template-path"},
	&cli.StringSliceFlag{Name: "var"},
}

type config struct {
	pipeline.PipelineConfig
	Clickhouse    ch.ConnPoolConfig
	LocalExecutor *ch.LocalExecutorConfig
	StartupProbe  ch.StartupProbeConfig
	PromAddr      string
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "pipeline",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				logger       = slogctx.FromCtx(ctx.Context)
				path         = ctx.Args().Get(0)
				templatePath = ctx.String("template-path")
				vars         = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
				cfg          config
			)

			if len(path) == 0 {
				return fmt.Errorf("pipeline path must be specified")
			}

			if err := cnf.Load(
				&cfg,
				cnf.WithProvider(objstrutils.NewCnfProvider(objstr.FromContextOrDefault(ctx.Context), path)),
				cnf.WithProvider(env.NewEnvProvider("AGN")),
			); err != nil {
				return err
			}

			u, err := url.Parse(path)

			if err != nil {
				return err
			}

			if len(templatePath) == 0 {
				u.Path = filepath.Dir(u.Path)
			}

			tmpl, err := utils.LoadTemplates(ctx.Context, u)

			if err != nil {
				return err
			}

			var pipelineCtx, pipelineCancel = signal.NotifyContext(ctx.Context, syscall.SIGTERM)
			defer pipelineCancel()

			var promReporter = promreporter.NewReporter(promreporter.Options{
				OnRegisterError: func(err error) {
					logger.Log(ctx.Context, -30, "failed to register metric", "error", err.Error())
				},
			})

			scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
				Prefix:         "agnostic_blockchain_etl",
				CachedReporter: promReporter,
				Separator:      promreporter.DefaultSeparator,
			}, 1*time.Second)

			defer scopeCloser.Close()

			if len(cfg.PromAddr) == 0 {
				cfg.PromAddr = ":9000"
			}

			go http.ListenAndServe("", promhttp.Handler())

			pipelineCtx = tallyctx.NewContext(pipelineCtx, scope)

			var group, groupCtx = errgroup.WithContext(pipelineCtx)

			if cfg.LocalExecutor != nil {
				group.Go(func() error {
					return ch.RunLocalExecutor(groupCtx, *cfg.LocalExecutor)
				})
			}

			group.Go(func() error {
				var pool = ch.NewConnPool(cfg.Clickhouse)
				defer pool.Close()

				if err := ch.RunStartupProbe(groupCtx, pool, cfg.StartupProbe); err != nil {
					return err
				}

				return pipeline_retrier.Run(
					groupCtx,
					pool,
					tmpl,
					vars,
					cfg.PipelineConfig.WithDefaults(),
					pipeline_retrier.RetryStrategy{},
				)
			})

			return group.Wait()
		},
	}
}
