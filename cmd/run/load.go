package run

import (
	"context"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	slogctx "github.com/veqryn/slog-context"
)

func loadLoop(
	ctx context.Context,
	tmpl *template.Template,
	inchan <-chan *batch,
) error {
	var logger = slogctx.FromCtx(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case b, open := <-inchan:
			if !open {
				return nil
			}

			var t0 = time.Now()

			md, err := ch.ExecFromTemplate(
				ctx,
				b.Conn,
				tmpl,
				"batch_load.sql",
				b.Vars,
			)

			if err != nil {
				return fmt.Errorf("failed to execute batch_load.sql template: %w", err)
			}

			ch.LogQueryMetadata(ctx, logger, slog.LevelDebug, "batch_load.sql", md)

			logger.Info(
				"batch_load.sql",
				"start_block", b.StartBlock,
				"end_block", b.EndBlock,
				"duration", time.Since(t0),
			)

			_, err = ch.ExecFromTemplate(
				ctx,
				b.Conn,
				tmpl,
				"batch_cleanup.sql",
				b.Vars,
			)

			if err != nil {
				return fmt.Errorf("failed to execute batch_cleanup.sql template: %w", err)
			}

			b.Conn.Release()
		}
	}
}
