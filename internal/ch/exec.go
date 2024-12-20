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

func ExecFromTemplate(
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (*QueryMetadata, error) {
	var (
		logger = slogctx.FromCtx(ctx)
		md     QueryMetadata
	)

	q, err := utils.RenderTemplate(tmpl, name, vars)

	if err != nil {
		return nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	if logger.Enabled(ctx, slog.Level(-10)) {
		logger.Log(ctx, -10, q, "template", name)
	}

	err = conn.Exec(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(md.progressHandler),
			clickhouse.WithLogs(md.logHandler),
		),
		q,
	)

	LogQueryMetadata(ctx, logger, slog.LevelDebug, name, &md)

	if err != nil {
		return nil, fmt.Errorf("failed to execute template %s: %w", name, err)
	}

	return &md, err
}
