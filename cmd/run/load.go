package run

import (
	"context"
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

			_, err := ch.ExecFromTemplate(
				ctx,
				b.Conn,
				tmpl,
				"batch_load.sql",
				b.Vars,
			)

			if err != nil {
				return err
			}

			logger.Info(
				"batch_load.sql",
				"start", b.Start,
				"end", b.End,
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
				return err
			}

			b.Conn.Release()
		}
	}
}
