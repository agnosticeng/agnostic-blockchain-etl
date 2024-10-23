package run

import (
	"context"
	"text/template"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
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

			_, err := ch.ExecFromTemplate(
				ctx,
				b.Conn,
				tmpl,
				"batch_transform.sql",
				b.Vars,
			)

			if err != nil {
				return err
			}

			logger.Info(
				"batch_transform.sql",
				"start", b.Start,
				"end", b.End,
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
