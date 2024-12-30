package ch

import (
	"context"
	"fmt"
	"log/slog"
	"text/template"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/engine"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	slogctx "github.com/veqryn/slog-context"
)

func ExecFromTemplate(
	ctx context.Context,
	conn engine.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (*engine.QueryMetadata, error) {
	var logger = slogctx.FromCtx(ctx)

	q, err := utils.RenderTemplate(tmpl, name, vars)

	if err != nil {
		return nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	if logger.Enabled(ctx, slog.Level(-10)) {
		logger.Log(ctx, -10, q, "template", name)
	}

	md, err := conn.Exec(ctx, q)
	LogQueryMetadata(ctx, logger, slog.LevelDebug, name, md)

	if err != nil {
		return nil, fmt.Errorf("failed to execute template %s: %w", name, err)
	}

	return md, err
}
