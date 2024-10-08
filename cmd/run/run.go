package run

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/puddle/v2"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
	"golang.org/x/sync/errgroup"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "dsn", Value: "clickhouse://localhost:9000/default"},
	&cli.IntFlag{Name: "batch-size", Value: 10000},
	&cli.IntFlag{Name: "load-chan-size", Value: 1},
	&cli.Uint64Flag{Name: "start-block", Value: 0},
	&cli.DurationFlag{Name: "max-connection-lifetime", Value: time.Hour},
	&cli.StringSliceFlag{Name: "var"},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				logger          = slogctx.FromCtx(ctx.Context)
				path            = ctx.Args().Get(0)
				dsn             = ctx.String("dsn")
				batchSize       = ctx.Int("batch-size")
				loadChanSize    = ctx.Int("load-chan-size")
				startBlock      = ctx.Uint64("start-block")
				maxConnLifetime = ctx.Duration("max-connection-lifetime")
				vars            = parseFlagVars(ctx.StringSlice("var"))
			)

			if len(path) == 0 {
				return fmt.Errorf("a path must be specified")
			}

			if batchSize <= 0 {
				batchSize = 100
			}

			if loadChanSize <= 0 {
				loadChanSize = 1
			}

			stat, err := os.Stat(path)

			if err != nil {
				return err
			}

			if !stat.IsDir() {
				return fmt.Errorf("path must point to a directory of SQL template files")
			}

			var _fs = os.DirFS(path)

			tmpl, err := template.ParseFS(_fs, "*.sql")

			if err != nil {
				return err
			}

			var poolConf = puddle.Config[driver.Conn]{
				MaxSize: int32(loadChanSize) + 3,
			}

			poolConf.Constructor = func(context.Context) (driver.Conn, error) {
				chopts, err := clickhouse.ParseDSN(dsn)

				if err != nil {
					return nil, err
				}

				chopts.MaxOpenConns = 1
				chopts.ConnMaxLifetime = maxConnLifetime * 2
				chconn, err := clickhouse.Open(chopts)

				if err != nil {
					return nil, err
				}

				return chconn, nil
			}

			poolConf.Destructor = func(conn driver.Conn) {
				conn.Close()
			}

			pool, err := puddle.NewPool(&poolConf)

			if err != nil {
				return err
			}

			defer pool.Close()

			chconn, err := pool.Acquire(ctx.Context)

			if err != nil {
				return err
			}

			defer chconn.Release()

			md, err := execFromTemplate(
				ctx.Context,
				chconn.Value(),
				tmpl,
				"setup.sql",
				vars,
			)

			if err != nil {
				return fmt.Errorf("failed to execute setup.sql template: %w", err)
			}

			logQueryMetadata(ctx.Context, logger, slog.LevelDebug, "setup.sql", md)

			sb, md, err := selectSingleRowFromTemplate[startBlockRow](
				ctx.Context,
				chconn.Value(),
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

			logQueryMetadata(ctx.Context, logger, slog.LevelDebug, "init_start_block.sql", md)

			var (
				group, groupctx = errgroup.WithContext(ctx.Context)
				loadChan        = make(chan *batch, loadChanSize)
			)

			group.Go(func() error {
				return transformLoop(
					groupctx,
					pool,
					tmpl,
					maps.Clone(vars),
					startBlock,
					batchSize,
					maxConnLifetime,
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
	Conn       *puddle.Resource[driver.Conn]
	StartBlock uint64
	EndBlock   uint64
	Vars       map[string]interface{}
}

func transformLoop(
	ctx context.Context,
	pool *puddle.Pool[driver.Conn],
	tmpl *template.Template,
	vars map[string]interface{},
	startBlock uint64,
	batchSize int,
	maxConnLifetime time.Duration,
	outchan chan<- *batch,
) error {
	var logger = slogctx.FromCtx(ctx)

	for {
		var (
			t0       = time.Now()
			endBlock uint64
			chconn   *puddle.Resource[driver.Conn]
			err      error
		)

		for {
			chconn, err = pool.Acquire(ctx)

			if err != nil {
				return err
			}

			if time.Since(chconn.CreationTime()) < maxConnLifetime {
				break
			}

			chconn.Destroy()
		}

		meb, md, err := selectSingleRowFromTemplate[maxEndBlockRow](
			ctx,
			chconn.Value(),
			tmpl,
			"batch_max_end_block.sql",
			vars,
		)

		if err != nil {
			chconn.Release()
			return fmt.Errorf("failed to execute batch_max_end_block.sql template: %w", err)
		}

		logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_max_end_block.sql", md)

		endBlock = min(startBlock+uint64(batchSize)-1, meb.MaxEndBlock)

		var runVars = maps.Clone(vars)
		runVars["START_BLOCK"] = startBlock
		runVars["END_BLOCK"] = endBlock

		md, err = execFromTemplate(
			ctx,
			chconn.Value(),
			tmpl,
			"batch_transform.sql",
			runVars,
		)

		if err != nil {
			chconn.Release()
			return fmt.Errorf("failed to execute batch_transform.sql template: %w", err)
		}

		logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_transform.sql", md)

		logger.Info(
			"batch_transform.sql",
			"start_block", startBlock,
			"end_block", endBlock,
			"duration", time.Since(t0),
		)

		var batch = batch{
			Conn:       chconn,
			StartBlock: startBlock,
			EndBlock:   endBlock,
			Vars:       runVars,
		}

		sb, md, err := selectSingleRowFromTemplate[startBlockRow](
			ctx,
			chconn.Value(),
			tmpl,
			"batch_next_start_block.sql",
			runVars,
		)

		if err != nil {
			chconn.Release()
			return fmt.Errorf("failed to execute batch_next_start_block.sql template: %w", err)
		}

		logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_next_start_block.sql", md)

		if md.WroteRows > 0 {
			startBlock = sb.StartBlock
		} else {
			startBlock = endBlock + 1
		}

		endBlock = 0

		select {
		case <-ctx.Done():
			chconn.Release()
			return nil
		case outchan <- &batch:
		}
	}
}

func loadLoop(
	ctx context.Context,
	tmpl *template.Template,
	inchan <-chan *batch,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, open := <-inchan:
			if !open {
				return nil
			}

			if err := load(ctx, tmpl, b); err != nil {
				return err
			}
		}
	}
}

func load(
	ctx context.Context,
	tmpl *template.Template,
	b *batch,
) error {
	var logger = slogctx.FromCtx(ctx)
	defer b.Conn.Release()

	var t0 = time.Now()

	md, err := execFromTemplate(
		ctx,
		b.Conn.Value(),
		tmpl,
		"batch_load.sql",
		b.Vars,
	)

	if err != nil {
		return fmt.Errorf("failed to execute batch_load.sql template: %w", err)
	}

	logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_load.sql", md)

	logger.Info(
		"batch_load.sql",
		"start_block", b.StartBlock,
		"end_block", b.EndBlock,
		"duration", time.Since(t0),
	)

	_, err = execFromTemplate(
		ctx,
		b.Conn.Value(),
		tmpl,
		"batch_cleanup.sql",
		b.Vars,
	)

	if err != nil {
		return fmt.Errorf("failed to execute batch_cleanup.sql template: %w", err)
	}

	return nil
}
