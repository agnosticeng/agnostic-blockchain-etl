package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/template"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/pipeline_retrier"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	"github.com/agnosticeng/cnf/providers/file"
	"github.com/urfave/cli/v2"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "template-path"},
	&cli.StringSliceFlag{Name: "var"},
}

type config struct {
	pipeline.PipelineConfig
	Clickhouse ch.ConnPoolConfig
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "pipeline",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				path         = ctx.Args().Get(0)
				templatePath = ctx.String("template-path")
				vars         = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
				cfg          config
			)

			if err := cnf.Load(
				&cfg,
				cnf.WithProvider(file.NewFileProvider(path)),
				cnf.WithProvider(env.NewEnvProvider("AGN")),
			); err != nil {
				return err
			}

			js, _ := json.MarshalIndent(cfg, "", "    ")
			fmt.Println(string(js))

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

			var pool = ch.NewConnPool(cfg.Clickhouse)

			defer pool.Close()

			var pipelineCtx, pipelineCancel = signal.NotifyContext(ctx.Context, syscall.SIGTERM)
			defer pipelineCancel()

			return pipeline_retrier.Run(
				pipelineCtx,
				pool,
				tmpl,
				vars,
				cfg.PipelineConfig.WithDefaults(),
				pipeline_retrier.RetryStrategy{},
			)
		},
	}
}
