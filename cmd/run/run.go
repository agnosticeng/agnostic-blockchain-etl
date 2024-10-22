package run

import (
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "dsn", Value: "clickhouse://localhost:9000/default"},
	&cli.IntFlag{Name: "batch-size", Value: 10000},
	&cli.IntFlag{Name: "transform-chan-size", Value: 1},
	&cli.IntFlag{Name: "load-chan-size", Value: 1},
	&cli.Uint64Flag{Name: "start-block", Value: 0},
	&cli.DurationFlag{Name: "wait-on-tip", Value: time.Second * 5},
	&cli.DurationFlag{Name: "max-connection-lifetime", Value: time.Hour},
	&cli.StringSliceFlag{Name: "var"},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				logger            = slogctx.FromCtx(ctx.Context)
				path              = ctx.Args().Get(0)
				dsn               = ctx.String("dsn")
				batchSize         = ctx.Int("batch-size")
				transformChanSize = ctx.Int("transform-chan-size")
				loadChanSize      = ctx.Int("load-chan-size")
				startBlock        = ctx.Uint64("start-block")
				waitOnTip         = ctx.Duration("wait-on-tip")
				maxConnLifetime   = ctx.Duration("max-connection-lifetime")
				vars              = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
			)

			if len(path) == 0 {
				return fmt.Errorf("a path must be specified")
			}

			if batchSize <= 0 {
				batchSize = 100
			}

			if transformChanSize <= 0 {
				transformChanSize = 1
			}

			if loadChanSize <= 0 {
				loadChanSize = 1
			}

			if waitOnTip == 0 {
				waitOnTip = 5 * time.Second
			}

			tmpl, err := utils.BuildTemplate(path)

			if err != nil {
				return err
			}

			var pool = NewConnPool(ConnPoolConfig{
				DSN:             dsn,
				MaxConnLifetime: maxConnLifetime,
			})

			defer pool.Close()

			chconn, err := pool.Acquire()

			if err != nil {
				return err
			}

			md, err := ch.ExecFromTemplate(
				ctx.Context,
				chconn,
				tmpl,
				"init_setup.sql",
				vars,
			)

			if err != nil {
				return fmt.Errorf("failed to execute init_setup.sql template: %w", err)
			}

			ch.LogQueryMetadata(ctx.Context, logger, slog.LevelDebug, "init_setup.sql", md)

			sb, md, err := ch.SelectSingleRowFromTemplate[startBlockRow](
				ctx.Context,
				chconn,
				tmpl,
				"init_start_block.sql",
				vars,
			)

			if err != nil {
				return fmt.Errorf("failed to execute init_start_block.sql template: %w", err)
			}

			if md.Rows > 0 {
				startBlock = sb.StartBlock
			}

			ch.LogQueryMetadata(ctx.Context, logger, slog.LevelDebug, "init_start_block.sql", md)

			var (
				group, groupctx = errgroup.WithContext(ctx.Context)
				transformChan   = make(chan *batch, transformChanSize)
				loadChan        = make(chan *batch, loadChanSize)
			)

			group.Go(func() error {
				return extractLoop(
					groupctx,
					pool,
					tmpl,
					maps.Clone(vars),
					startBlock,
					batchSize,
					waitOnTip,
					transformChan,
				)
			})

			group.Go(func() error {
				return transformLoop(
					groupctx,
					tmpl,
					transformChan,
					loadChan,
				)
			})

			group.Go(func() error {
				return loadLoop(
					groupctx,
					tmpl,
					loadChan,
				)

			})

			return group.Wait()
		},
	}
}

type batch struct {
	Conn       *Conn
	StartBlock uint64
	EndBlock   uint64
	Vars       map[string]interface{}
}
