package run

import (
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "dsn", Value: "clickhouse://localhost:9000/default"},
	&cli.IntFlag{Name: "batch-size", Value: 10000},
	&cli.IntFlag{Name: "transform-chan-size", Value: 1},
	&cli.IntFlag{Name: "load-chan-size", Value: 1},
	&cli.Uint64Flag{Name: "start", Value: 0},
	&cli.DurationFlag{Name: "wait-on-tip", Value: time.Second * 5},
	&cli.DurationFlag{Name: "max-connection-lifetime", Value: time.Hour},
	&cli.IntFlag{Name: "stop-after", Value: math.MaxInt},
	&cli.StringSliceFlag{Name: "var"},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				path              = ctx.Args().Get(0)
				dsn               = ctx.String("dsn")
				batchSize         = ctx.Int("batch-size")
				transformChanSize = ctx.Int("transform-chan-size")
				loadChanSize      = ctx.Int("load-chan-size")
				start             = ctx.Uint64("start")
				waitOnTip         = ctx.Duration("wait-on-tip")
				maxConnLifetime   = ctx.Duration("max-connection-lifetime")
				stopAfter         = ctx.Int("stop-after")
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

			var pool = ch.NewConnPool(ch.ConnPoolConfig{
				DSN:             dsn,
				MaxConnLifetime: maxConnLifetime,
			})

			defer pool.Close()

			chconn, err := pool.Acquire()

			if err != nil {
				return err
			}

			_, err = ch.ExecFromTemplate(
				ctx.Context,
				chconn,
				tmpl,
				"init_setup.sql",
				vars,
			)

			if err != nil {
				return err
			}

			sb, md, err := ch.SelectSingleRowFromTemplate[startRow](
				ctx.Context,
				chconn,
				tmpl,
				"init_start.sql",
				vars,
			)

			if err != nil {
				return err
			}

			if md.Rows > 0 {
				start = sb.Start
			}

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
					start,
					batchSize,
					waitOnTip,
					stopAfter,
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
