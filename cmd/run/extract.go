package run

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"text/template"
	"time"

	slogctx "github.com/veqryn/slog-context"
)

func extractLoop(
	ctx context.Context,
	pool *ConnPool,
	tmpl *template.Template,
	vars map[string]interface{},
	startBlock uint64,
	batchSize int,
	waitOnTip time.Duration,
	outchan chan<- *batch,
) error {
	defer close(outchan)
	var logger = slogctx.FromCtx(ctx)

	for {
		var (
			t0       = time.Now()
			endBlock uint64
		)

		chconn, err := pool.Acquire()

		if err != nil {
			return err
		}

		meb, md, err := selectSingleRowFromTemplate[maxEndBlockRow](
			ctx,
			chconn,
			tmpl,
			"batch_max_end_block.sql",
			vars,
		)

		if err != nil {
			return fmt.Errorf("failed to execute batch_max_end_block.sql template: %w", err)
		}

		logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_max_end_block.sql", md)

		endBlock = min(startBlock+uint64(batchSize)-1, meb.MaxEndBlock)

		if startBlock > endBlock {
			select {
			case <-time.After(waitOnTip):
				continue
			case <-ctx.Done():
				return nil
			}
		}

		var runVars = maps.Clone(vars)
		runVars["START_BLOCK"] = startBlock
		runVars["END_BLOCK"] = endBlock

		md, err = execFromTemplate(
			ctx,
			chconn,
			tmpl,
			"batch_extract.sql",
			runVars,
		)

		if err != nil {
			return fmt.Errorf("failed to execute batch_extract.sql template: %w", err)
		}

		logQueryMetadata(ctx, logger, slog.LevelDebug, "batch_extract.sql", md)

		logger.Info(
			"batch_extract.sql",
			"start_block", startBlock,
			"end_block", endBlock,
			"duration", time.Since(t0),
		)

		var b = batch{
			Conn:       chconn,
			StartBlock: startBlock,
			EndBlock:   endBlock,
			Vars:       runVars,
		}

		sb, md, err := selectSingleRowFromTemplate[startBlockRow](
			ctx,
			chconn,
			tmpl,
			"batch_next_start_block.sql",
			runVars,
		)

		if err != nil {
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
			return nil
		case outchan <- &b:
		}
	}
}
