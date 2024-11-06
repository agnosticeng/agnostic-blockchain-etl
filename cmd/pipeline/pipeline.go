package pipeline

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/template"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline_retrier"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/agnosticeng/conf"
	"github.com/urfave/cli/v2"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "dsn", Value: "clickhouse://localhost:9000/default"},
	&cli.StringFlag{Name: "template-path"},
	&cli.StringSliceFlag{Name: "var"},
	&cli.DurationFlag{Name: "max-connection-lifetime", Value: time.Hour},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "pipeline",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				path            = ctx.Args().Get(0)
				dsn             = ctx.String("dsn")
				templatePath    = ctx.String("template-path")
				vars            = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
				maxConnLifetime = ctx.Duration("max-connection-lifetime")
				pipelineConf    pipeline.PipelineConfig
			)

			if err := conf.Load(
				&pipelineConf,
				conf.WithConfigFilePath(path),
				conf.WithEnvPrefix("AGN"),
			); err != nil {
				return err
			}

			if len(templatePath) == 0 {
				templatePath = filepath.Dir(path)
			}

			stat, err := os.Stat(templatePath)

			if err != nil {
				return err
			}

			if !stat.IsDir() {
				return fmt.Errorf("path must point to a directory of SQL template files")
			}

			tmpl, err := template.ParseFS(os.DirFS(templatePath), "*.sql")

			if err != nil {
				return err
			}

			var pool = ch.NewConnPool(ch.ConnPoolConfig{
				DSN:             dsn,
				MaxConnLifetime: maxConnLifetime,
			})

			defer pool.Close()

			var pipelineCtx, pipelineCancel = signal.NotifyContext(ctx.Context, syscall.SIGTERM)
			defer pipelineCancel()

			pipelineConf = pipelineConf.WithDefaults()

			return pipeline_retrier.Run(
				pipelineCtx,
				pool,
				tmpl,
				vars,
				pipelineConf,
				pipeline_retrier.RetryStrategy{},
			)
		},
	}
}
