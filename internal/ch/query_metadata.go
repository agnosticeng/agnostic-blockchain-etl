package ch

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

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

func LogQueryMetadata(ctx context.Context, logger *slog.Logger, level slog.Level, msg string, md *QueryMetadata) {
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
