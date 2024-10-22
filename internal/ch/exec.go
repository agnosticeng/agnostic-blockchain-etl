package ch

import (
	"context"
	"fmt"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
)

func ExecFromTemplate(
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (*QueryMetadata, error) {
	var md QueryMetadata

	q, err := utils.RenderTemplate(tmpl, name, vars)

	if err != nil {
		return nil, fmt.Errorf("failed to render %s template: %w", name, err)
	}

	return &md, conn.Exec(
		clickhouse.Context(
			ctx,
			clickhouse.WithProgress(md.progressHandler),
			clickhouse.WithLogs(md.logHandler),
		),
		q,
	)
}
