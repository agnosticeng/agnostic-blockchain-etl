package ch

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/iancoleman/strcase"
)

func NormalizeSettings(settings clickhouse.Settings) clickhouse.Settings {
	var m = make(clickhouse.Settings)

	for k, v := range settings {
		m[strcase.ToSnake(k)] = v
	}

	return m
}
