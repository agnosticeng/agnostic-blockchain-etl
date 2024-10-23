package run

import (
	"context"
	"maps"
	"text/template"
	"time"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	slogctx "github.com/veqryn/slog-context"
)

func extractLoop(
	ctx context.Context,
	pool *ConnPool,
	tmpl *template.Template,
	vars map[string]interface{},
	start uint64,
	batchSize int,
	waitOnTip time.Duration,
	stopAfter int,
	outchan chan<- *batch,
) error {
	defer close(outchan)
	var (
		logger     = slogctx.FromCtx(ctx)
		iterations int
	)

	for {
		if iterations == stopAfter {
			return nil
		}

		var (
			t0  = time.Now()
			end uint64
		)

		chconn, err := pool.Acquire()

		if err != nil {
			return err
		}

		meb, _, err := ch.SelectSingleRowFromTemplate[maxEndRow](
			ctx,
			chconn,
			tmpl,
			"batch_max_end.sql",
			vars,
		)

		if err != nil {
			return err
		}

		end = min(start+uint64(batchSize)-1, meb.MaxEnd)

		if start > end {
			select {
			case <-time.After(waitOnTip):
				continue
			case <-ctx.Done():
				return nil
			}
		}

		var runVars = maps.Clone(vars)
		runVars["START"] = start
		runVars["END"] = end

		_, err = ch.ExecFromTemplate(
			ctx,
			chconn,
			tmpl,
			"batch_extract.sql",
			runVars,
		)

		if err != nil {
			return err
		}

		logger.Info(
			"batch_extract.sql",
			"start", start,
			"end", end,
			"duration", time.Since(t0),
		)

		var b = batch{
			Conn:  chconn,
			Start: start,
			End:   end,
			Vars:  runVars,
		}

		sb, md, err := ch.SelectSingleRowFromTemplate[startRow](
			ctx,
			chconn,
			tmpl,
			"batch_next_start.sql",
			runVars,
		)

		if err != nil {
			return err
		}

		if md.WroteRows > 0 {
			start = sb.Start
		} else {
			start = end + 1
		}

		end = 0

		select {
		case <-ctx.Done():
			return nil
		case outchan <- &b:
			iterations++
		}
	}
}
