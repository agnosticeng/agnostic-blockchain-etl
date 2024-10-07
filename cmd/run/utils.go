package run

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func parseFlagVars(flagVars []string) map[string]interface{} {
	var m = make(map[string]interface{})

	for _, flagVar := range flagVars {
		var k, v, _ = strings.Cut(flagVar, "=")
		m[k] = v
	}

	return m
}

func renderTemplate(tmpl *template.Template, name string, vars map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := tmpl.ExecuteTemplate(&buf, name, vars); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func selectSingleRowFromTemplate[T any](
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

	q, err := renderTemplate(tmpl, name, vars)

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

func execFromTemplate(
	ctx context.Context,
	conn driver.Conn,
	tmpl *template.Template,
	name string,
	vars map[string]interface{},
) (*QueryMetadata, error) {
	var md QueryMetadata

	q, err := renderTemplate(tmpl, name, vars)

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
