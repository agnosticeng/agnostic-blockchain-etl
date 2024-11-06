package pipeline

import (
	"context"
	"text/template"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
)

type InitConfig struct {
	Setup        string
	Start        string
	DefaultStart uint64
}

func (conf InitConfig) WithDefaults() InitConfig {
	if len(conf.Setup) == 0 {
		conf.Setup = "setup.sql"
	}

	if len(conf.Start) == 0 {
		conf.Start = "start.sql"
	}

	return conf
}

func Init(
	ctx context.Context,
	pool *ch.ConnPool,
	tmpl *template.Template,
	vars map[string]interface{},
	conf InitConfig,
) (uint64, error) {
	var start = conf.DefaultStart

	chconn, err := pool.Acquire()

	if err != nil {
		return start, err
	}

	defer chconn.Release()

	_, err = ch.ExecFromTemplate(ctx, chconn, tmpl, conf.Setup, vars)

	if err != nil {
		return start, err
	}

	sb, md, err := ch.SelectSingleRowFromTemplate[StartRow](ctx, chconn, tmpl, conf.Start, vars)

	if err != nil {
		return start, err
	}

	if md.Rows > 0 {
		start = sb.Start
	}

	return start, nil
}
