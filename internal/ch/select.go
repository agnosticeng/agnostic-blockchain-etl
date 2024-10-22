package ch

import (
	"context"
	"fmt"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
)

func SelectSingleRowFromTemplate[T any](
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (T, *QueryMetadata, error) {
	var (
		zero T
		md   QueryMetadata
		res  []T
	)

	q, err := utils.RenderTemplate(tmpl, name, vars)

	if err != nil {
		return zero, nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	if err := conn.Select(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(md.progressHandler),
			clickhouse.WithLogs(md.logHandler),
		),
		&res,
		q,
	); err != nil {
		return zero, nil, err
	}

	if len(res) != 1 {
		return zero, nil, fmt.Errorf("query returned %d rows instead of 1", len(res))
	}

	return res[0], &md, nil
}
