package run

import (
	"context"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	slogctx "github.com/veqryn/slog-context"
)

func transformLoop(
	ctx context.Context,
	tmpl *template.Template,
	inchan <-chan *batch,
	outchan chan<- *batch,
) error {
	defer close(outchan)
	var logger = slogctx.FromCtx(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case b, open := <-inchan:
			var t0 = time.Now()

			if !open {
				return nil
			}

			md, err := execFromTemplate(
				ctx,
				b.Conn,
				tmpl,
				"batch_transform.sql",
				b.Vars,
			)

			if err != nil {
				return fmt.Errorf("failed to execute batch_transform.sql template: %w", err)
			}

			logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_transform.sql", md)

			logger.Info(
				"batch_transform.sql",
				"start_block", b.StartBlock,
				"end_block", b.EndBlock,
				"duration", time.Since(t0),
			)

			select {
			case <-ctx.Done():
				return nil
			case outchan <- b:
			}
		}
	}
}
