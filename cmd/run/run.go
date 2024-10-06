package run

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
)

var Flags = []cli.Flag{
	&cli.StringFlag{Name: "dsn", Value: "clickhouse://localhost:9000/default"},
	&cli.IntFlag{Name: "batch-size", Value: 10000},
	&cli.StringSliceFlag{Name: "var"},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				path      = ctx.Args().Get(0)
				dsn       = ctx.String("dsn")
				batchSize = ctx.Int("batch-size")
				flagVars  = ctx.StringSlice("var")
				vars      = make(map[string]interface{})
				logger    = slogctx.FromCtx(ctx.Context)
			)

			if len(path) == 0 {
				return fmt.Errorf("a path must be specified")
			}

			if batchSize <= 0 {
				batchSize = 100
			}

			for _, flagVar := range flagVars {
				var k, v, _ = strings.Cut(flagVar, "=")
				vars[k] = v
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

			chopts, err := clickhouse.ParseDSN(dsn)

			if err != nil {
				return err
			}

			chconn, err := clickhouse.Open(chopts)

			if err != nil {
				return err
			}

			defer chconn.Close()

			if _, err := execFromTemplate(
				ctx.Context,
				chconn,
				tmpl,
				"setup.sql",
				vars,
			); err != nil {
				return fmt.Errorf("failed to render setup.sql template: %w", err)
			}

			var (
				runs      int
				wroteRows uint64
			)

			for {
				var (
					t0         = time.Now()
					startBlock uint64
					endBlock   uint64
				)

				sb, md, err := selectSingleRowFromTemplate[startBlockRow](
					ctx.Context,
					chconn,
					tmpl,
					"start_block.sql",
					vars,
				)

				if err != nil {
					return fmt.Errorf("failed to execute start block query: %w", err)
				}

				logQueryMetadata(ctx.Context, logger, slog.LevelDebug, "start_block.sql", md)

				meb, md, err := selectSingleRowFromTemplate[maxEndBlockRow](
					ctx.Context,
					chconn,
					tmpl,
					"max_end_block.sql",
					vars,
				)

				if err != nil {
					return fmt.Errorf("failed to execute max end block query: %w", err)
				}

				logQueryMetadata(ctx.Context, logger, slog.LevelDebug, "max_end_block.sql", md)

				if md.Rows > 0 {
					startBlock = sb.StartBlock
				}

				if startBlock == 0 && runs > 0 {
					startBlock = uint64(runs) * uint64(batchSize)
				}

				endBlock = min(startBlock+uint64(batchSize)-1, meb.MaxEndBlock)

				var runVars = maps.Clone(vars)
				runVars["START_BLOCK"] = startBlock
				runVars["END_BLOCK"] = endBlock

				md, err = execFromTemplate(
					ctx.Context,
					chconn,
					tmpl,
					"etl.sql",
					runVars,
				)

				if err != nil {
					return fmt.Errorf("failed to render etl.sql template: %w", err)
				}

				runs += 1
				wroteRows += md.WroteRows

				logger.Info(
					"etl.sql",
					"start_block", startBlock,
					"end_block", endBlock,
					"duration", time.Since(t0),
				)

				logQueryMetadata(ctx.Context, logger, slog.LevelDebug, "etl.sql", md)
			}
		},
	}
}

func logQueryMetadata(ctx context.Context, logger *slog.Logger, level slog.Level, msg string, md *QueryMetadata) {
	if logger.Enabled(ctx, level) {
		logger.Log(
			ctx,
			level,
			msg,
			"rows", md.Rows,
			"bytes", md.Bytes,
			"total_rows", md.TotalRows,
			"wrote_rows", md.WroteRows,
			"wrote_bytes", md.WroteBytes,
			"elapsed", md.Elapsed,
		)
	}
}

type startBlockRow struct {
	StartBlock uint64 `ch:"start_block"`
}

type maxEndBlockRow struct {
	MaxEndBlock uint64 `ch:"max_end_block"`
}

func renderTemplate(tmpl *template.Template, name string, vars map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := tmpl.ExecuteTemplate(&buf, name, vars); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func selectSingleRowFromTemplate[T any](
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (T, *QueryMetadata, error) {
	var (
		zero T
		md   QueryMetadata
		res  []T
	)

	q, err := renderTemplate(tmpl, name, vars)

	if err != nil {
		return zero, nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	if err := conn.Select(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(md.progressHandler),
			clickhouse.WithLogs(md.logHandler),
		),
		&res,
		q,
	); err != nil {
		return zero, nil, err
	}

	if len(res) != 1 {
		return zero, nil, fmt.Errorf("query returned %d rows instead of 1", len(res))
	}

	return res[0], &md, nil
}

func execFromTemplate(
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (*QueryMetadata, error) {
	var md QueryMetadata

	q, err := renderTemplate(tmpl, name, vars)

	if err != nil {
		return nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	return &md, conn.Exec(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(md.progressHandler),
			clickhouse.WithLogs(md.logHandler),
		),
		q,
	)
}

type QueryMetadata struct {
	Rows       uint64
	Bytes      uint64
	TotalRows  uint64
	WroteRows  uint64
	WroteBytes uint64
	Elapsed    time.Duration
	Logs       []*clickhouse.Log
}

func (md *QueryMetadata) progressHandler(p *clickhouse.Progress) {
	if p == nil {
		return
	}

	md.Rows += p.Rows
	md.Bytes += p.Bytes
	md.TotalRows += p.TotalRows
	md.WroteRows += p.WroteRows
	md.WroteBytes += p.WroteBytes
	md.Elapsed += p.Elapsed
}

func (md *QueryMetadata) logHandler(log *clickhouse.Log) {
	md.Logs = append(md.Logs, log)
}
