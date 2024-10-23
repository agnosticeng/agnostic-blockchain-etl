package ch

import (
	"context"
	"fmt"
	"log/slog"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	slogctx "github.com/veqryn/slog-context"
)

func SelectSingleRowFromTemplate[T any](
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (T, *QueryMetadata, error) {
	var (
		logger = slogctx.FromCtx(ctx)
		zero   T
		md     QueryMetadata
		res    []T
	)

	q, err := utils.RenderTemplate(tmpl, name, vars)

	if err != nil {
		return zero, nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	if logger.Enabled(ctx, slog.Level(-10)) {
		logger.Log(ctx, -10, q, "template", name)
	}

	err = conn.Select(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(md.progressHandler),
			clickhouse.WithLogs(md.logHandler),
		),
		&res,
		q,
	)

	if err != nil {
		return zero, nil, fmt.Errorf("failed to execute template %s: %w", name, err)
	}

	if len(res) != 1 {
		return zero, nil, fmt.Errorf("query returned %d rows instead of 1", len(res))
	}

	LogQueryMetadata(ctx, logger, slog.LevelDebug, name, &md)
	return res[0], &md, nil
}
